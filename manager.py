# manager.py
import uuid
import os
import json
import logging
import asyncio
from decimal import Decimal
from collections import deque, OrderedDict
from typing import Dict, Any, Optional, Deque, List

from utils import safe_api_call, _normalize_to_dict, validate_and_quantize, would_self_match, format_by_increment
import db_manager

logger = logging.getLogger("CoinbaseOrderManager")
DRY_RUN: bool = os.getenv("DRY_RUN", "0") == "1"

class CoinbaseOrderManager:
    def __init__(self, portfolio_clients: Dict[str, Any]) -> None:
        self.processed_fills: set[str] = set()
        self.clients: Dict[str, Any] = portfolio_clients
        self.active_orders: Dict[str, Dict[str, Any]] = {}
        self.priority_queue: Deque[Dict[str, Any]] = deque()
        self.execution_queue: Deque[Dict[str, Any]] = deque()
        self.cost_basis_ledger: Dict[str, Deque[Dict[str, Decimal]]] = {
            pid: deque() for pid in portfolio_clients
        }
        self.ledger_locks: Dict[str, asyncio.Lock] = {}
        self._queue_lock: asyncio.Lock = asyncio.Lock()
        self.live_ticker_price: Decimal = Decimal("0")
        self.total_dust_swept: int = 0
        self.realized_gains_history: List[Decimal] = []
        self.profits_portfolio_id: str = os.getenv("PROFITS_PORTFOLIO_ID", "ca3e8f25-8dd3-4a58-89b4-884d2e10519b")
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.terminal_order_cache: OrderedDict[str, bool] = OrderedDict()
        self.CACHE_MAX_SIZE: int = 5000

    def _ensure_lock(self, portfolio_id: str) -> None:
        if portfolio_id not in self.ledger_locks:
            self.ledger_locks[portfolio_id] = asyncio.Lock()

    def _get_side(self, order: Dict[str, Any]) -> str:
        return (order.get("order_side") or order.get("side") or order.get("side_type") or "").upper()

    def _normalize_pid(self, payload: Dict[str, Any]) -> str:
        return payload.get("source_pid") or payload.get("portfolio_id") or payload.get("pid")

    def enqueue(self, payload: Dict[str, Any]):
        """
        Central enqueue for tasks. PLACE_ORDER payloads are validated and checked
        for conservative self-match avoidance and id uniqueness here.
        """
        action = payload.get("action")
        if action == "PLACE_ORDER":
            price = payload.get("price")
            size = payload.get("size")
            tick_size = payload.get("tick_size") or payload.get("market_tick_size") or Decimal("0.00001")

            price_q, size_d = validate_and_quantize(price, size, tick_size, format_by_increment)
            if price_q is None:
                return

            payload["price"] = str(price_q)
            payload["size"] = str(size_d)

            if not payload.get("post_only", False):
                payload["stp_id"] = payload.get("stp_id") or str(uuid.uuid4())
                payload["client_order_id"] = payload.get("client_order_id") or str(uuid.uuid4())
            else:
                if not payload.get("client_order_id"):
                    pid = self._normalize_pid(payload) or "unknown"
                    payload["client_order_id"] = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{pid}_{payload.get('side')}_{price_q}"))

            portfolio_id = self._normalize_pid(payload)
            product_id = payload.get("product_id")
            active = [o for o in self.active_orders.values()
                      if o.get("portfolio_id") == portfolio_id and o.get("product_id") == product_id]

            if would_self_match(payload.get("side", ""), price_q, active):
                logger.info("Skipping PLACE_ORDER that would self-match: pid=%s product=%s side=%s price=%s",
                            portfolio_id[:8] if portfolio_id else 'None', product_id, payload.get("side"), price_q)
                return

        # Queue routing
        if action in ["CANCEL_ORDER", "TRANSFER_PROFIT"]:
            self.priority_queue.append(payload)
        else:
            self.execution_queue.append(payload)

    def register_active_order(self, client_order_id: str, payload: Dict[str, Any]) -> None:
        if client_order_id in self.terminal_order_cache: return
        self.active_orders[f"local_{client_order_id}"] = payload

    async def prime_ledger(self) -> None:
        logger.info("Priming FIFO ledger with historical fill data...")
        for pid, client in self.clients.items():
            self._ensure_lock(pid)
            async with self.ledger_locks[pid]:
                try:
                    api_res: Dict[str, Any] = await safe_api_call(client, "GET", "/api/v3/brokerage/orders/historical/fills", {"product_id": "AERO-USDC"})
                    data: Dict[str, Any] = api_res.get("response", {})
                    if not data or not isinstance(data, dict): continue

                    fills: List[Dict[str, Any]] = data.get("fills") or data.get("items") or data.get("data", {}).get("fills") or []
                    sorted_fills = sorted(fills, key=lambda x: x.get("trade_time", "") if isinstance(x, dict) else "")

                    adopted: int = 0
                    for f in sorted_fills:
                        try:
                            if self._get_side(f) != "BUY": continue
                            p = Decimal(str(f.get("price")))
                            s = Decimal(str(f.get("size")))
                        except Exception: continue

                        if pid not in self.cost_basis_ledger: self.cost_basis_ledger[pid] = deque()
                        self.cost_basis_ledger[pid].append({"price": p, "size": s})
                        adopted += 1

                    logger.info("[%s] Ledger primed. Batches adopted: %d", pid[:8], adopted)
                except Exception as e:
                    logger.error("[%s] Ledger priming failed: %s", pid[:8], e, exc_info=True)

    async def add_buy_to_ledger(self, portfolio_id: str, price: str, size: str) -> None:
        self._ensure_lock(portfolio_id)
        async with self.ledger_locks[portfolio_id]:
            try:
                p: Decimal = Decimal(str(price))
                s: Decimal = Decimal(str(size))
            except Exception: return

            if portfolio_id not in self.cost_basis_ledger: self.cost_basis_ledger[portfolio_id] = deque()
            self.cost_basis_ledger[portfolio_id].append({"price": p, "size": s})
            logger.info("[%s] Added buy batch: %s @ %s", portfolio_id[:8], s, p)

    async def log_pnl_to_db(self, portfolio_id: str, realized_pnl: Decimal, trade_id: str, client_order_id: Optional[str] = None, buy_price: str = "0", sell_price: str = "0", size: str = "0") -> None:
        try:
            cid_to_log: str = client_order_id if client_order_id else trade_id
            db_manager.log_trade_pnl(trade_id, cid_to_log, portfolio_id, str(realized_pnl), buy_price, sell_price, size)
            logger.info("[%s] Queued PnL to DB: %s USDC (ID: %s)", portfolio_id[:8], realized_pnl, trade_id[:8])
        except Exception as e:
            logger.error("[%s] Failed to queue PnL to DB: %s", portfolio_id[:8], e)

    async def calculate_and_sweep(self, portfolio_id: str, sell_price: str, sell_size: str, client_order_id: Optional[str] = None, exchange_id: Optional[str] = None) -> None:
        if portfolio_id not in self.cost_basis_ledger: self.cost_basis_ledger[portfolio_id] = deque()
        self._ensure_lock(portfolio_id)
        async with self.ledger_locks[portfolio_id]:
            try:
                if not self.cost_basis_ledger[portfolio_id]: return
                try:
                    remaining: Decimal = Decimal(str(sell_size))
                    s_price: Decimal = Decimal(str(sell_price))
                except Exception: return

                realized_usdc: Decimal = Decimal("0")
                while remaining > 0 and self.cost_basis_ledger[portfolio_id]:
                    batch = self.cost_basis_ledger[portfolio_id][0]
                    batch_size: Decimal = batch.get("size", Decimal("0"))
                    batch_price: Decimal = batch.get("price", Decimal("0"))

                    if batch_size <= 0:
                        self.cost_basis_ledger[portfolio_id].popleft()
                        continue

                    if batch_size <= remaining:
                        realized_usdc += (s_price - batch_price) * batch_size
                        remaining -= batch_size
                        self.cost_basis_ledger[portfolio_id].popleft()
                    else:
                        realized_usdc += (s_price - batch_price) * remaining
                        self.cost_basis_ledger[portfolio_id][0]["size"] = batch_size - remaining
                        remaining = Decimal("0")

                executed_size: Decimal = Decimal(str(sell_size)) - remaining
                if realized_usdc > Decimal("0") and executed_size > Decimal("0"):
                    avg_buy_price: Decimal = s_price - (realized_usdc / executed_size)
                    trade_id: str = exchange_id if exchange_id else str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{portfolio_id}-{sell_price}-{sell_size}-{uuid.uuid4()}"))
                    await self.log_pnl_to_db(portfolio_id=portfolio_id, realized_pnl=realized_usdc, trade_id=trade_id, client_order_id=client_order_id, buy_price=str(avg_buy_price), sell_price=str(sell_price), size=str(executed_size))
            except Exception as e:
                logger.error("? FATAL ERROR IN calculate_and_sweep for %s: %s", portfolio_id, e, exc_info=True)

    async def sync_active_orders(self) -> None:
        """
        Inverted Reconciliation Watchdog with PnL Harvesting.
        If the WebSocket drops a FILLED message, this sweep identifies the ghost order,
        verifies its terminal state, and executes the proper ledger accounting.
        """
        for pid, client in self.clients.items():
            if pid == self.profits_portfolio_id: continue

            try:
                params = {"limit": 100, "retail_portfolio_id": pid}
                resp = await safe_api_call(client, "GET", "/api/v3/brokerage/orders/historical/batch", payload=params)
                if not resp.get("success"): continue

                orders_list = resp.get("response", {}).get("orders", [])
                
                # Create a map of terminal orders for fast lookup
                terminal_orders = {}
                for order in orders_list:
                    cid = order.get("client_order_id")
                    if cid: terminal_orders[cid] = order

                ghosts_purged = 0
                keys_to_remove = []

                for local_key, local_order in list(self.active_orders.items()):
                    if local_order.get("portfolio_id") == pid:
                        loc_cid = local_order.get("client_order_id")
                        
                        # We found a ghost order
                        if loc_cid and loc_cid in terminal_orders:
                            terminal_data = terminal_orders[loc_cid]
                            status = terminal_data.get("status", "").upper()
                            
                            # If the ghost order was actually a missed fill, process it immediately!
                            if status == "FILLED" and loc_cid not in self.processed_fills:
                                side = self._get_side(terminal_data)
                                price = str(terminal_data.get("average_filled_price", terminal_data.get("avg_price", "0")))
                                size = str(terminal_data.get("filled_size", terminal_data.get("cumulative_quantity", "0")))
                                exchange_id = terminal_data.get("order_id")

                                self.processed_fills.add(loc_cid)
                                
                                logger.info("?? [Watchdog] Harvested missing FILL payload for %s. Routing to ledger.", loc_cid[:8])
                                if side == "BUY":
                                    await self.add_buy_to_ledger(pid, price, size)
                                elif side == "SELL":
                                    await self.calculate_and_sweep(pid, price, size, loc_cid, exchange_id)

                            keys_to_remove.append(local_key)

                for key in keys_to_remove:
                    loc_cid = self.active_orders[key].get("client_order_id")
                    if loc_cid:
                        db_manager.purge_ghost_order(loc_cid)
                        async with self._queue_lock:
                            self.priority_queue = deque(t for t in self.priority_queue if t.get("client_order_id") != loc_cid)
                            self.execution_queue = deque(t for t in self.execution_queue if t.get("client_order_id") != loc_cid)
                    del self.active_orders[key]
                    ghosts_purged += 1

                if ghosts_purged > 0:
                    logger.warning("[%s] Reconciliation Watchdog discovered and purged %d ghost orders.", pid[:8], ghosts_purged)

            except Exception as e:
                logger.error("Error in sync_active_orders for %s: %s", pid[:8], e)

    async def skim_dust(self) -> None:
        logger.info("Scanning portfolios for dust to skim...")
        DUST_THRESHOLD: Decimal = Decimal("1.00")
        STABLECOIN_EXCLUSIONS: set[str] = {"USDC", "USDT"}

        for pid, client in self.clients.items():
            if pid == self.profits_portfolio_id: continue
            try:
                resp: Dict[str, Any] = await safe_api_call(client, "GET", f"/api/v3/brokerage/portfolios/{pid}")
                if not resp.get("success"): continue
                data: Dict[str, Any] = resp.get("response", {})
                assets: List[Dict[str, Any]] = data.get("portfolio", {}).get("breakdown", {}).get("spot_positions", []) or data.get("positions", []) or []

                for asset in assets:
                    try:
                        balance_raw = asset.get("account_balance", {}).get("value", "0") if isinstance(asset, dict) else "0"
                        balance: Decimal = Decimal(str(balance_raw))
                        currency: Optional[str] = asset.get("asset") or asset.get("currency") or asset.get("symbol")
                    except Exception: continue

                    if balance > 0 and balance < DUST_THRESHOLD and currency not in STABLECOIN_EXCLUSIONS:
                        transfer_payload: Dict[str, Any] = {
                            "source_portfolio_uuid": pid,
                            "target_portfolio_uuid": self.profits_portfolio_id,
                            "funds": {"value": str(balance), "currency": currency},
                        }
                        if DRY_RUN:
                            logger.info("[DRY_RUN] Would move dust %s %s from %s to %s", balance, currency, pid[:8], self.profits_portfolio_id[:8])
                        else:
                            success: bool = (await safe_api_call(client, "POST", "/api/v3/brokerage/portfolios/move_funds", payload=transfer_payload)).get("success", False)
                            if success:
                                self.enqueue({"action": "TRANSFER_FUNDS", "payload": transfer_payload})
                                self.total_dust_swept += 1
                                logger.info("[%s] Queued dust transfer: %s %s", pid[:8], balance, currency)
            except Exception as e:
                logger.error("[%s] Dust skimming fault: %s", pid[:8], e)

    async def _cleanup_terminal_order(self, client_order_id: str) -> None:
        if not client_order_id: return
        if client_order_id not in self.terminal_order_cache:
            self.terminal_order_cache[client_order_id] = True
            if len(self.terminal_order_cache) > self.CACHE_MAX_SIZE:
                self.terminal_order_cache.popitem(last=False)
        local_key = f"local_{client_order_id}"
        self.active_orders.pop(local_key, None)
        async with self._queue_lock:
            self.priority_queue = deque(t for t in self.priority_queue if t.get("client_order_id") != client_order_id)
            self.execution_queue = deque(t for t in self.execution_queue if t.get("client_order_id") != client_order_id)

    def on_user_message(self, message: Any) -> None:
        try:
            if isinstance(message, str): message = json.loads(message)
        except Exception: return

        events = message.get("events", []) if isinstance(message, dict) else []
        for event in events:
            orders = event.get("orders", []) if isinstance(event, dict) else []
            for order in orders:
                try:
                    status = order.get("status", "").upper()
                    client_order_id = order.get("client_order_id")
                    cancel_reason = order.get("cancel_reason", "")
                    side = self._get_side(order)
                    price = str(order.get("limit_price", order.get("avg_price", "0")))
                    size = str(order.get("cumulative_quantity", "0"))
                    pid = order.get("retail_portfolio_id")
                    exchange_id = order.get("order_id")

                    if not client_order_id or not side or not pid: continue

                    if status in ["OPEN", "PENDING"]:
                        self.register_active_order(client_order_id, {
                            "client_order_id": client_order_id,
                            "portfolio_id": pid,
                            "product_id": order.get("product_id", "AERO-USDC"),
                            "side": side,
                            "price": price,
                            "status": status,
                            "exchange_id": exchange_id
                        })
                        continue

                    if status in ["CANCELLED", "EXPIRED", "FILLED"]:
                        if status != "FILLED":
                            logger.info("[OrderManager] Terminal state reached for %s (%s: %s). Purging from memory.", client_order_id, status, cancel_reason)
                        if self.loop and self.loop.is_running():
                            asyncio.run_coroutine_threadsafe(self._cleanup_terminal_order(client_order_id), self.loop)
                        if status != "FILLED": continue

                    if client_order_id in self.processed_fills: continue
                    self.processed_fills.add(client_order_id)

                    if self.loop and self.loop.is_running():
                        if side == "BUY":
                            asyncio.run_coroutine_threadsafe(self.add_buy_to_ledger(pid, price, size), self.loop)
                        elif side == "SELL":
                            asyncio.run_coroutine_threadsafe(self.calculate_and_sweep(pid, price, size, client_order_id, exchange_id), self.loop)
                    else:
                        logger.error("? Threading Error: OrderManager loop not attached. Cannot process Fill.")
                except Exception as e:
                    logger.error("Error handling user order event: %s\n%s", order, e)

    def on_ticker_message(self, message: Any) -> None:
        try:
            if isinstance(message, str): message = json.loads(message)
        except Exception: return
        events = message.get("events", []) if isinstance(message, dict) else []
        for event in events:
            tickers = event.get("tickers", []) if isinstance(event, dict) else []
            for ticker in tickers:
                try:
                    price = ticker.get("price")
                    if price is None: continue
                    self.live_ticker_price = Decimal(str(price))
                except Exception: pass

    async def drain_execution_queue(self, limit: int = 20) -> List[Dict[str, Any]]:
        batch: List[Dict[str, Any]] = []
        async with self._queue_lock:
            while self.priority_queue and len(batch) < limit: batch.append(self.priority_queue.popleft())
            while self.execution_queue and len(batch) < limit: batch.append(self.execution_queue.popleft())
        return batch

    def attach_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        logger.info("Event loop attached to CoinbaseOrderManager.")

    async def start_background_tasks(self) -> None:
        try:
            await self.prime_ledger()
        except Exception as e:
            logger.error("Error priming ledger on startup: %s", e, exc_info=True)

        async def _periodic():
            while True:
                try: await self.skim_dust()
                except Exception as e: logger.error("Periodic dust skim failed: %s", e, exc_info=True)
                try: await self.sync_active_orders()
                except Exception as e: logger.debug("Periodic sync_active_orders skipped or failed: %s", e)
                await asyncio.sleep(60)

        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(_periodic(), self.loop)
        else:
            asyncio.create_task(_periodic())

    async def shutdown(self) -> None:
        logger.info("Shutting down CoinbaseOrderManager...")
        try:
            if hasattr(db_manager, "flush"): db_manager.flush()
        except Exception: pass
        logger.info("CoinbaseOrderManager shutdown complete.")

    def dump_active_orders(self) -> List[Dict[str, Any]]:
        return [v.copy() for v in self.active_orders.values()]