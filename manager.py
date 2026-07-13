# manager.py
import uuid
import os
import json
import logging
import asyncio
import collections
from decimal import Decimal
from collections import deque
from typing import Dict, Any, Optional

from utils import safe_api_call
import db_manager

logger = logging.getLogger("CoinbaseOrderManager")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

class CoinbaseOrderManager:
    def __init__(self, portfolio_clients: Dict[str, Any]):
        self.processed_fills = set() 
        self.clients = portfolio_clients
        self.active_orders: Dict[str, Dict[str, Any]] = {}
        self.execution_queue: deque = deque()
        self.cost_basis_ledger: Dict[str, deque] = {pid: deque() for pid in portfolio_clients}
        self.ledger_lock = asyncio.Lock()
        self._queue_lock = asyncio.Lock()
        self.live_ticker_price = Decimal("0")
        self.total_dust_swept = 0
        self.realized_gains_history = []
        self.profits_portfolio_id = os.getenv("PROFITS_PORTFOLIO_ID", "ca3e8f25-8dd3-4a58-89b4-884d2e10519b")
        self.loop = None
        # Cache to prevent REST threads from resurrecting orders killed by WS race conditions
        self.terminal_order_cache = deque(maxlen=5000) 

    def enqueue(self, payload: Dict[str, Any]):
        """Synchronous thread-safe enqueue."""
        self.execution_queue.append(payload)

    def register_active_order(self, client_order_id: str, payload: dict):
        """Thread-safe injection of new orders, protected by terminal cache."""
        if client_order_id in self.terminal_order_cache:
            return
        local_key = f"local_{client_order_id}"
        self.active_orders[local_key] = payload

    async def prime_ledger(self):
        async with self.ledger_lock:
            logger.info("Priming FIFO ledger with historical fill data...")
            for pid, client in self.clients.items():
                try:
                    data = None
                    if hasattr(client, "list_fills"):
                        try:
                            resp = await asyncio.to_thread(client.list_fills, product_id="AERO-USDC")
                            data = resp if isinstance(resp, dict) else (resp.json() if hasattr(resp, 'json') else None)
                        except Exception:
                            data = None
                    else:
                        api_res = await safe_api_call(client, "GET", "/api/v3/brokerage/orders/historical/fills", {"product_id": "AERO-USDC"})
                        data = api_res.get("response")

                    if not data or not isinstance(data, dict):
                        continue

                    fills = data.get("fills", []) or data.get("data", {}).get("fills", []) or []
                    sorted_fills = sorted(fills, key=lambda x: x.get("trade_time", "") if isinstance(x, dict) else "")
                    adopted = 0
                    for f in sorted_fills:
                        try:
                            if f.get("side") != "BUY": continue
                            p = Decimal(str(f.get("price")))
                            s = Decimal(str(f.get("size")))
                        except Exception:
                            continue

                        if pid not in self.cost_basis_ledger:
                            self.cost_basis_ledger[pid] = deque()
                        self.cost_basis_ledger[pid].append({"price": p, "size": s})
                        adopted += 1

                    logger.info("[%s] Ledger primed. Batches adopted: %d", pid[:8], adopted)
                except Exception as e:
                    logger.error("[%s] Ledger priming failed: %s", pid[:8], e, exc_info=True)

    async def sync_active_orders(self):
        for pid, client in self.clients.items():
            if pid == self.profits_portfolio_id:
                continue
            try:
                endpoint = "/api/v3/brokerage/orders/historical/batch?order_status=OPEN&limit=1000"
                resp = await safe_api_call(client, "GET", endpoint)
                if not resp.get("success"):
                    continue
                    
                orders_list = resp.get("response", {}).get("orders", [])
                exchange_open_client_ids = set()
                
                for order in orders_list:
                    cid = order.get("client_order_id")
                    if cid and order.get("status") == "OPEN":
                        exchange_open_client_ids.add(cid)
                        temp_id = f"local_{cid}"
                        if temp_id not in self.active_orders:
                            order_config = order.get("order_configuration", {})
                            limit_config = order_config.get("limit_limit_gtc", {}) or order_config.get("limit_limit_gtd", {})
                            self.active_orders[temp_id] = {
                                "client_order_id": cid, "portfolio_id": pid, "product_id": order.get("product_id"),
                                "side": order.get("side", "UNKNOWN").upper(), "price": limit_config.get("limit_price", "0"),
                                "status": "OPEN", "exchange_id": order.get("order_id")
                            }

                ghosts_purged = 0
                keys_to_remove = []
                for local_key, local_order in self.active_orders.items():
                    if local_order.get("portfolio_id") == pid:
                        loc_cid = local_order.get("client_order_id")
                        if loc_cid and loc_cid not in exchange_open_client_ids:
                            keys_to_remove.append(local_key)
                            
                for key in keys_to_remove:
                    loc_cid = self.active_orders[key].get("client_order_id")
                    if loc_cid:
                        db_manager.purge_ghost_order(loc_cid)
                    del self.active_orders[key]
                    ghosts_purged += 1
                    
                if ghosts_purged > 0:
                    logger.warning("[%s] Reconciliation Watchdog purged %d ghost orders.", pid[:8], ghosts_purged)
            except Exception as e:
                logger.error("Error reconciling portfolio %s: %s", pid[:8], e)
                
    async def skim_dust(self):
        logger.info("Scanning portfolios for dust to skim...")
        DUST_THRESHOLD = Decimal("1.00")
        STABLECOIN_EXCLUSIONS = {"USDC", "USDT"}

        for pid, client in self.clients.items():
            if pid == self.profits_portfolio_id:
                continue
            try:
                resp = await safe_api_call(client, "GET", f"/api/v3/brokerage/portfolios/{pid}")
                if not resp.get("success"):
                    continue
                data = resp.get("response", {})

                assets = data.get("portfolio", {}).get("breakdown", {}).get("spot_positions", []) or data.get("positions", []) or []
                for asset in assets:
                    try:
                        balance_raw = asset.get("account_balance", {}).get("value", "0") if isinstance(asset, dict) else "0"
                        balance = Decimal(str(balance_raw))
                        currency = asset.get("asset") or asset.get("currency") or asset.get("symbol")
                    except Exception:
                        continue

                    if balance > 0 and balance < DUST_THRESHOLD and (currency not in STABLECOIN_EXCLUSIONS):
                        transfer_payload = {
                            "source_portfolio_uuid": pid,
                            "target_portfolio_uuid": self.profits_portfolio_id,
                            "funds": {"value": str(balance), "currency": currency}
                        }
                        if DRY_RUN:
                            logger.info("[DRY_RUN] Would move dust %s %s from %s to %s", balance, currency, pid[:8], self.profits_portfolio_id[:8])
                        else:
                            success = (await safe_api_call(client, "POST", "/api/v3/brokerage/portfolios/move_funds", payload=transfer_payload)).get("success", False)
                            if success:
                                self.enqueue({"action": "TRANSFER_FUNDS", "payload": transfer_payload})
                                self.total_dust_swept += 1
                                logger.info("[%s] Queued dust transfer: %s %s", pid[:8], balance, currency)
            except Exception as e:
                logger.error("[%s] Dust skimming fault: %s", pid[:8], e)

    async def add_buy_to_ledger(self, portfolio_id: str, price: str, size: str):
        async with self.ledger_lock:
            try:
                p = Decimal(str(price))
                s = Decimal(str(size))
            except Exception:
                return

            if portfolio_id not in self.cost_basis_ledger:
                self.cost_basis_ledger[portfolio_id] = deque()
            self.cost_basis_ledger[portfolio_id].append({"price": p, "size": s})
            logger.info("[%s] Added buy batch: %s @ %s", portfolio_id[:8], s, p)

    async def log_pnl_to_db(self, portfolio_id: str, realized_pnl: Decimal, trade_id: str, client_order_id: str = None, buy_price: str = "0", sell_price: str = "0", size: str = "0"):
        try:
            cid_to_log = client_order_id if client_order_id else trade_id
            db_manager.log_trade_pnl(trade_id, cid_to_log, portfolio_id, str(realized_pnl), buy_price, sell_price, size)
            logger.info("[%s] Queued PnL to DB: %s USDC (ID: %s)", portfolio_id[:8], realized_pnl, trade_id[:8])
        except Exception as e:
            logger.error("[%s] Failed to queue PnL to DB: %s", portfolio_id[:8], e)

    async def calculate_and_sweep(self, portfolio_id: str, sell_price: str, sell_size: str, client_order_id: str = None, exchange_id: str = None):
        if portfolio_id not in self.cost_basis_ledger:
            self.cost_basis_ledger[portfolio_id] = deque()

        async with self.ledger_lock:
            try:
                if not self.cost_basis_ledger[portfolio_id]:
                    return

                try:
                    remaining = Decimal(str(sell_size))
                    s_price = Decimal(str(sell_price))
                except Exception:
                    return

                realized_usdc = Decimal("0")
                consumed_batches = []

                while remaining > 0 and self.cost_basis_ledger[portfolio_id]:
                    batch = self.cost_basis_ledger[portfolio_id][0]
                    batch_size = batch.get("size", Decimal("0"))
                    batch_price = batch.get("price", Decimal("0"))
                    
                    if batch_size <= 0:
                        self.cost_basis_ledger[portfolio_id].popleft()
                        continue

                    if batch_size <= remaining:
                        realized_usdc += (s_price - batch_price) * batch_size
                        remaining -= batch_size
                        consumed_batches.append(self.cost_basis_ledger[portfolio_id].popleft())
                    else:
                        realized_usdc += (s_price - batch_price) * remaining
                        self.cost_basis_ledger[portfolio_id][0]["size"] = batch_size - remaining
                        remaining = Decimal("0")

                executed_size = Decimal(str(sell_size)) - remaining

                if realized_usdc > Decimal("0") and executed_size > Decimal("0"):
                    avg_buy_price = s_price - (realized_usdc / executed_size)
                    trade_id = exchange_id if exchange_id else str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{portfolio_id}-{sell_price}-{sell_size}-{uuid.uuid4()}"))
                    
                    await self.log_pnl_to_db(
                        portfolio_id=portfolio_id, realized_pnl=realized_usdc,
                        trade_id=trade_id, client_order_id=client_order_id,
                        buy_price=str(avg_buy_price), sell_price=str(sell_price), size=str(executed_size)
                    )
                    logger.info("💰 [PnL] Logged realized profit for %s: $%s", portfolio_id[:8], realized_usdc)

            except Exception as e:
                logger.error("❌ FATAL ERROR IN calculate_and_sweep for %s: %s", portfolio_id, e)

    async def _cleanup_terminal_order(self, client_order_id: str):
        if not client_order_id: return
        
        if client_order_id not in self.terminal_order_cache:
            self.terminal_order_cache.append(client_order_id)

        local_key = f"local_{client_order_id}"
        self.active_orders.pop(local_key, None)
        
        async with self._queue_lock:
            new_queue = deque()
            for task in self.execution_queue:
                if task.get("client_order_id") != client_order_id:
                    new_queue.append(task)
            self.execution_queue = new_queue

    def on_user_message(self, message):
        import json
        logger.info(f"⚡ RAW WS PAYLOAD: {str(message)[:250]}")
        
        try:
            if isinstance(message, str): message = json.loads(message)
        except Exception:
            return

        events = message.get("events", []) if isinstance(message, dict) else []
        for event in events:
            orders = event.get("orders", []) if isinstance(event, dict) else []
            for order in orders:
                logger.info("DEBUG_ORDER_EVENT: ID=%s Status=%s", order.get("client_order_id"), order.get("status"))
                try:
                    status = order.get("status", "").upper()
                    client_order_id = order.get("client_order_id")
                    cancel_reason = order.get("cancel_reason", "")
                    
                    if status in ["CANCELLED", "EXPIRED", "FILLED"]:
                        if status != "FILLED":
                            logger.info(f"[OrderManager] Terminal state reached for {client_order_id} ({status}: {cancel_reason}). Purging from memory.")
                        if self.loop and self.loop.is_running():
                            asyncio.run_coroutine_threadsafe(self._cleanup_terminal_order(client_order_id), self.loop)
                        
                        if status != "FILLED":
                            continue
                    
                    if client_order_id in self.processed_fills:
                        continue
                    self.processed_fills.add(client_order_id)
                    
                    side = order.get("order_side", "").upper() 
                    price = order.get("avg_price") or order.get("limit_price", "0")
                    size = order.get("cumulative_quantity", "0")
                    pid = order.get("retail_portfolio_id") 
                    exchange_id = order.get("order_id") 
                    
                    if not client_order_id or not side or not pid:
                        continue
                        
                    if self.loop and self.loop.is_running():
                        if side == "BUY":
                            asyncio.run_coroutine_threadsafe(self.add_buy_to_ledger(pid, price, size), self.loop)
                        elif side == "SELL":
                            asyncio.run_coroutine_threadsafe(self.calculate_and_sweep(pid, price, size, client_order_id, exchange_id), self.loop)
                    else:
                        logger.error("❌ Threading Error: OrderManager loop not attached. Cannot process Fill.")

                except Exception as e:
                    logger.error("Error handling user order event: %s\n%s", order, e)

    def on_ticker_message(self, message: Any):
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
                except Exception:
                    pass

    async def drain_execution_queue(self, limit: int = 20) -> list:
        batch = []
        async with self._queue_lock:
            while self.execution_queue and len(batch) < limit:
                batch.append(self.execution_queue.popleft())
        return batch