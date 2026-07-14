# manager.py
import uuid
import os
import json
import logging
import asyncio
from decimal import Decimal
from collections import deque, OrderedDict
from typing import Dict, Any, Optional

from utils import safe_api_call, _normalize_to_dict
import db_manager

logger = logging.getLogger("CoinbaseOrderManager")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

class CoinbaseOrderManager:
    def __init__(self, portfolio_clients: Dict[str, Any]):
        self.processed_fills = set() 
        self.clients = portfolio_clients
        self.active_orders: Dict[str, Dict[str, Any]] = {}
        
        # Action-Based Priority Queuing (Cancellations jump the line)
        self.priority_queue: deque = deque()
        self.execution_queue: deque = deque()
        
        self.cost_basis_ledger: Dict[str, deque] = {pid: deque() for pid in portfolio_clients}
        
        # Portfolio-Specific Ledger Locks (Lazy Loaded)
        self.ledger_locks: Dict[str, asyncio.Lock] = {}
        
        self._queue_lock = asyncio.Lock()
        self.live_ticker_price = Decimal("0")
        self.total_dust_swept = 0
        self.realized_gains_history = []
        self.profits_portfolio_id = os.getenv("PROFITS_PORTFOLIO_ID", "ca3e8f25-8dd3-4a58-89b4-884d2e10519b")
        self.loop = None
        
        # O(1) Lookups for WebSocket/REST synchronization
        self.terminal_order_cache = OrderedDict()
        self.CACHE_MAX_SIZE = 5000 

    def _ensure_lock(self, portfolio_id: str):
        """Lazy creation helper to attach asyncio.Lock safely within the running loop."""
        if portfolio_id not in self.ledger_locks:
            self.ledger_locks[portfolio_id] = asyncio.Lock()

    def _get_side(self, order: Dict[str, Any]) -> str:
        """Helper to extract order side robustly across variable websocket/REST payloads."""
        return (order.get("order_side") or order.get("side") or order.get("side_type") or "").upper()

    def enqueue(self, payload: Dict[str, Any]):
        """Synchronous thread-safe enqueue. Route cancellations to priority channel."""
        if payload.get("action") == "CANCEL_ORDER":
            self.priority_queue.append(payload)
        else:
            self.execution_queue.append(payload)

    def register_active_order(self, client_order_id: str, payload: dict):
        """Thread-safe injection of new tracking parameters, protected by the terminal cache."""
        if client_order_id in self.terminal_order_cache:
            return
        local_key = f"local_{client_order_id}"
        self.active_orders[local_key] = payload

    async def prime_ledger(self):
        """Priming sequence to populate FIFO state histories with historical fills on boot."""
        logger.info("Priming FIFO ledger with historical fill data...")
        for pid, client in self.clients.items():
            self._ensure_lock(pid)
            async with self.ledger_locks[pid]:
                try:
                    api_res = await safe_api_call(client, "GET", "/api/v3/brokerage/orders/historical/fills", {"product_id": "AERO-USDC"})
                    data = api_res.get("response")

                    if not data or not isinstance(data, dict):
                        continue

                    fills = data.get("fills") or data.get("items") or data.get("data", {}).get("fills") or []
                    sorted_fills = sorted(fills, key=lambda x: x.get("trade_time", "") if isinstance(x, dict) else "")
                    adopted = 0
                    for f in sorted_fills:
                        try:
                            if self._get_side(f) != "BUY": 
                                continue
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
        """Watchdog background synchronization method to reconcile local cache with exchange state."""
        for pid, client in self.clients.items():
            if pid == self.profits_portfolio_id:
                continue
            try:
                endpoint = "/api/v3/brokerage/orders/historical/batch"
                
                # FIX 1: Pass order_status as a LIST to force strict API filtering
                # FIX 2: Pass via payload dictionary so the SDK signs the URL hash correctly
                params = {
                    "order_status": ["OPEN", "PENDING"], 
                    "limit": 1000
                }
                
                resp = await safe_api_call(client, "GET", endpoint, payload=params)
                if not resp.get("success"):
                    continue
                    
                orders_list = resp.get("response", {}).get("orders", [])
                exchange_open_client_ids = set()
                
                for order in orders_list:
                    cid = order.get("client_order_id")
                    
                    # FIX 3: Strict Local Filter. 
                    # Never trust the API's filtering; always verify the status manually.
                    if cid and order.get("status", "").upper() in ["OPEN", "PENDING"]:
                        exchange_open_client_ids.add(cid)
                        temp_id = f"local_{cid}"
                        if temp_id not in self.active_orders:
                            order_config = order.get("order_configuration", {})
                            limit_config = order_config.get("limit_limit_gtc", {}) or order_config.get("limit_limit_gtd", {})
                            self.active_orders[temp_id] = {
                                "client_order_id": cid, "portfolio_id": pid, "product_id": order.get("product_id"),
                                "side": self._get_side(order), "price": limit_config.get("limit_price", "0"),
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
        """Scans multi-tenant sub-accounts for marginal dust balances and sweeps to profits portfolio."""
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
        """Appends confirmed buy allocations into the portfolio's queue tracking cost-basis."""
        self._ensure_lock(portfolio_id)
        async with self.ledger_locks[portfolio_id]:
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
        """Calculates realized profit metrics using FIFO inventory accounting structures."""
        if portfolio_id not in self.cost_basis_ledger:
            self.cost_basis_ledger[portfolio_id] = deque()
            
        self._ensure_lock(portfolio_id)
        async with self.ledger_locks[portfolio_id]:
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
                logger.error("❌ FATAL ERROR IN calculate_and_sweep for %s: %s", portfolio_id, e, exc_info=True)

    async def _cleanup_terminal_order(self, client_order_id: str):
        if not client_order_id: return
        
        if client_order_id not in self.terminal_order_cache:
            self.terminal_order_cache[client_order_id] = True
            if len(self.terminal_order_cache) > self.CACHE_MAX_SIZE:
                self.terminal_order_cache.popitem(last=False)

        local_key = f"local_{client_order_id}"
        self.active_orders.pop(local_key, None)
        
        async with self._queue_lock:
            # Purge structural mutations from priority queue
            new_priority = deque()
            for task in self.priority_queue:
                if task.get("client_order_id") != client_order_id:
                    new_priority.append(task)
            self.priority_queue = new_priority
            
            # Purge structural mutations from standard queue
            new_standard = deque()
            for task in self.execution_queue:
                if task.get("client_order_id") != client_order_id:
                    new_standard.append(task)
            self.execution_queue = new_standard

    def on_user_message(self, message):
        logger.info(f"⚡ RAW WS PAYLOAD: {str(message)[:250]}")
        
        try:
            if isinstance(message, str): message = json.loads(message)
        except Exception:
            return

        events = message.get("events", []) if isinstance(message, dict) else []
        for event in events:
            orders = event.get("orders", []) if isinstance(event, dict) else []
            for order in orders:
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
                    
                    side = self._get_side(order)
                    price = order.get("avg_price") or order.get("limit_price", "0")
                    size = order.get("cumulative_quantity", "0")
                    pid = order.get("retail_portfolio_id") 
                    exchange_id = order.get("order_id") 
                    
                    if not client_order_id or not side or not pid:
                        continue
                        
                    self.processed_fills.add(client_order_id)
                        
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
            # Drain prioritized structural parameters first
            while self.priority_queue and len(batch) < limit:
                batch.append(self.priority_queue.popleft())
                
            # Fill out execution bandwidth with standard placements
            while self.execution_queue and len(batch) < limit:
                batch.append(self.execution_queue.popleft())
        return batch