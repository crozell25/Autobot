# run.py
#!/usr/bin/env python3
import aiosqlite
import os
import sys
import json
import ast
import logging
import asyncio
from uuid import uuid4
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler
from quart import Quart, jsonify
from dotenv import load_dotenv
from collections import defaultdict
from order_rebalancer import evaluate_rebalances

try:
    from coinbase.rest import RESTClient
    from coinbase.websocket import WSClient
except Exception:
    RESTClient = None
    WSClient = None

from manager import CoinbaseOrderManager
from utils import safe_api_call, calculate_clock_drift
import db_manager

try:
    import strategies.aero_asymmetric_grid_v2 as aero_grid
    import strategies.aero_reserves_ladder as aero_ladder
    import strategies.aero_simple_ladder as aero_simple 
    import strategies.aero_aggressive_scalper as aero_scalper
except ImportError as e:
    aero_grid = None
    aero_ladder = None
    aero_simple = None 
    aero_scalper = None
load_dotenv()

if sys.platform == "win32":
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass

log_handlers = [
    RotatingFileHandler("engine.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"),
    logging.StreamHandler(sys.stdout),
]
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=log_handlers)
logger = logging.getLogger("Orchestrator")

app = Quart(__name__)

BOT_STATUS = {
    "engine_active": False,
    "websocket_public_connected": False,
    "errors": []
}

def read_json_sync(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def load_portfolio_clients():
    clients = {}
    portfolios = read_json_sync("portfolios.json")
    if not portfolios:
        logger.error("❌ portfolios.json not found or invalid!")
        return clients

    for name, pid in portfolios.items():
        clean_name = name.upper().replace(" ", "_").replace(":", "")
        api_key = os.getenv(f"PORTFOLIO_{clean_name}_KEY")
        api_secret = os.getenv(f"PORTFOLIO_{clean_name}_SECRET")
        
        if not api_key or not api_secret:
            api_key = os.getenv("COINBASE_PRIVATE_KEY")
            api_secret = os.getenv("COINBASE_PRIVATE_SECRET")

        if not api_key or not api_secret:
            logger.warning(f"⚠️ Skipping {name}: No credentials found in .env.")
            continue
    
        if api_key and api_secret and RESTClient:
            if "\\n" in api_secret: api_secret = api_secret.replace("\\n", "\n")
            try:
                clients[pid] = RESTClient(api_key=api_key, api_secret=api_secret)
                logger.info("✅ Bound write permissions for portfolio: %s [%s]", name, pid[:8])
            except Exception as e:
                logger.error("❌ Failed to create REST client for %s: %s", name, e)
    return clients

portfolio_clients = load_portfolio_clients()
order_manager = CoinbaseOrderManager(portfolio_clients)
active_user_websockets = []

def route_strategy(portfolio_name, pid, order_manager, market_data):
    name_upper = portfolio_name.upper()
    if name_upper.startswith("GRID") and aero_grid:
        try: aero_grid.run_strategy(order_manager, market_data)
        except Exception as e: logger.exception("Strategy error (grid): %s", e)
    elif (name_upper.startswith("AERO RESERVE") or name_upper.startswith("RESERVE")) and aero_ladder:
        try: aero_ladder.run_strategy(order_manager, market_data)
        except Exception as e: logger.exception("Strategy error (reserve): %s", e)
    elif name_upper.startswith("SCALP") and aero_scalper: # New assignment
        logger.info(f"➡️ Routing '{portfolio_name}' to Aggressive Scalper Strategy...")
        try: aero_scalper.run_strategy(order_manager, market_data)
        except Exception as e: logger.exception("Strategy error (scalper): %s", e)

    elif name_upper.startswith("STRAT"):
        if aero_simple is None: return
        logger.info(f"➡️ Routing '{portfolio_name}' to Simple Ladder Strategy...")
        try: aero_simple.run_strategy(order_manager, market_data)
        except Exception as e: logger.exception("Strategy error (simple): %s", e)

async def execute_single_task(task):
    action_type = task.get("action")
    pid = task.get("source_pid") or task.get("portfolio_id")
    client = portfolio_clients.get(pid)

    if not client and not (os.getenv("DRY_RUN", "0") == "1"):
        return

    if action_type == "PLACE_ORDER":
        order_payload = {
            "client_order_id": task.get("client_order_id", str(uuid4())),
            "product_id": task["product_id"],
            "side": task["side"].upper(),
            "order_configuration": {
                "limit_limit_gtc": {
                    "base_size": str(Decimal(str(task["size"]))),
                    "limit_price": str(Decimal(str(task["price"]))),
                    "post_only": task.get("post_only", True),
                }
            },
            "self_trade_prevention_id": task.get("stp_id", str(uuid4())),
        }

        if os.getenv("DRY_RUN", "0") == "1":
            logger.info("[DRY_RUN] Would place order: %s", order_payload)
        else:
            api_result = await safe_api_call(client, "POST", "/api/v3/brokerage/orders", payload=order_payload)
            if api_result["success"]:
                logger.info("✅ Successfully placed %s order for portfolio %s", task["side"], pid[:8])
                resp_data = api_result.get("response", {})
                
                # Safely extract the returned order details
                success_resp = resp_data.get("success_response", {}) or resp_data.get("order", {})
                if not success_resp: 
                    success_resp = resp_data
                    
                exchange_id_val = success_resp.get("order_id") or success_resp.get("id")
                returned_status = success_resp.get("status", "OPEN").upper()

                # If the exchange instantly killed the order (e.g. STP), don't save it to local memory
                if returned_status in ["CANCELLED", "EXPIRED", "REJECTED", "FILLED"]:
                    logger.info("⚡ Order %s instantly terminated by exchange (%s). Skipping memory lock.", order_payload["client_order_id"][:8], returned_status)
                else:
                    # Route through our new manager function to respect WS race conditions
                    order_manager.register_active_order(order_payload["client_order_id"], {
                        "client_order_id": order_payload["client_order_id"],
                        "portfolio_id": pid, "product_id": task["product_id"],
                        "side": task["side"].upper(), "price": task["price"],
                        "status": "OPEN", "exchange_id": exchange_id_val 
                    })
                    
                try:
                    db_manager.log_new_order(order_payload['client_order_id'], pid, task['product_id'], task['side'].upper(), str(task['price']), str(task['size']), order_payload['self_trade_prevention_id'])
                except Exception:
                    pass
            else:
                logger.error("❌ Order rejected for %s: %s", pid[:8], api_result.get("error"))

    elif action_type == "CANCEL_ORDER":
        client_order_id = task.get("client_order_id")
        exchange_id = task.get("exchange_id")
        
        if not exchange_id:
            order_manager.active_orders.pop(f"local_{client_order_id}", None)
            return

        if not hasattr(order_manager, 'dead_orders'):
            order_manager.dead_orders = set()

        if exchange_id in order_manager.dead_orders:
            order_manager.active_orders.pop(f"local_{client_order_id}", None)
            return

        if os.getenv("DRY_RUN", "0") == "1":
            success = True
        else:
            success = False
            try:
                api_response = await asyncio.to_thread(client.cancel_orders, order_ids=[exchange_id])
                if isinstance(api_response, dict) and api_response.get("results", [{}])[0].get("success"):
                    success = True
                elif getattr(getattr(api_response, "results", [None])[0], "success", False):
                    success = True
            except Exception: pass

        order_manager.active_orders.pop(f"local_{client_order_id}", None)

        if success:
            logger.info("✅ Cancel confirmed and memory synced for: %s", str(client_order_id)[:8])
            db_manager.update_order_status(client_order_id, 'CANCELLED')
        else:
            order_manager.dead_orders.add(exchange_id)
            db_manager.update_order_status(client_order_id, 'GHOST_PURGED')

    elif action_type == "TRANSFER_PROFIT":
        safe_amount = str(round(Decimal(str(task["amount"])), 6))
        transfer_payload = {
            "source_portfolio_uuid": pid,
            "target_portfolio_uuid": order_manager.profits_portfolio_id,
            "value": safe_amount,
            "currency": "USDC"
        }
        
        if os.getenv("DRY_RUN", "0") == "1":
            success = True
        else:
            api_result = await safe_api_call(client, "POST", "/api/v3/brokerage/portfolios/move_funds", payload=transfer_payload)
            success = api_result.get("success", False)

        if success:
            logger.info("✅ Profit transfer successful for portfolio %s: %s USDC", pid[:8], safe_amount)
            if os.getenv("DRY_RUN", "0") != "1":
                try:
                    async with aiosqlite.connect("trading_bot.db") as db:
                        db.row_factory = aiosqlite.Row
                        cursor = await db.execute("SELECT amount_swept FROM pnl_swept_registry WHERE portfolio_id = ?", (pid,))
                        row = await cursor.fetchone()
                        current_swept = Decimal(str(row["amount_swept"] if row else "0"))
                        new_swept = current_swept + Decimal(safe_amount)
                        
                        await db.execute("""
                            INSERT INTO pnl_swept_registry (portfolio_id, amount_swept)
                            VALUES (?, ?)
                            ON CONFLICT(portfolio_id) DO UPDATE SET amount_swept = excluded.amount_swept
                        """, (pid, str(new_swept)))
                        await db.commit()
                except Exception: pass
        else:
            logger.error("❌ Profit transfer rejected by exchange. Rolling back ledger batches.")
            async with order_manager.ledger_lock:
                for rolled_batch in reversed(task.get("rollback_batches", [])):
                    order_manager.cost_basis_ledger[pid].appendleft(rolled_batch)

async def process_execution_queue():
    while True:
        batch = await order_manager.drain_execution_queue(limit=5)
        if batch:
            # FIX: Gather with exceptions isolated
            results = await asyncio.gather(*(execute_single_task(task) for task in batch), return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    logger.error("Execution task failed: %s", r, exc_info=True)
            await asyncio.sleep(0.5) 
        else:
            await asyncio.sleep(1)

async def fee_reconciliation_loop():
    await asyncio.sleep(75)
    logger.info("💸 Fee reconciliation loop initialized with Orphan Rescue.")
    orphan_tracker = defaultdict(int)
    
    while True:
        try:
            if not BOT_STATUS["engine_active"]:
                await asyncio.sleep(10)
                continue

            pending_by_pid = await db_manager.get_pending_fees_by_portfolio()
            if not pending_by_pid:
                orphan_tracker.clear() 
                await asyncio.sleep(120)
                continue

            for pid, pending_orders in pending_by_pid.items():
                client = portfolio_clients.get(pid)
                if not client: continue

                # CRITICAL FIX: 'payload', NOT 'params'
                response = await safe_api_call(client, "GET", "/api/v3/brokerage/orders/historical/fills", payload={"limit": 100})
                
                commission_map = defaultdict(float)
                if response.get("success"):
                    fills = response.get("response", {}).get("fills", [])
                    for fill in fills:
                        o_id = fill.get("order_id")
                        comm_str = fill.get("commission", "0.0")
                        commission_map[o_id] += float(comm_str) if comm_str else 0.0

                pending_set = set(pending_orders)
                matched_in_batch = set()
                
                for o_id, total_commission in commission_map.items():
                    if o_id in pending_set:
                        db_manager.queue_fee_reconciliation(o_id, total_commission)
                        matched_in_batch.add(o_id)
                        orphan_tracker.pop(o_id, None)

                unmatched = pending_set - matched_in_batch
                for o_id in unmatched:
                    orphan_tracker[o_id] += 1
                    
                    if orphan_tracker[o_id] >= 3:
                        logger.info(f"[Fee Recon] Order {o_id[:8]} fell out of batch. Initiating rescue.")
                        # CRITICAL FIX: 'payload', NOT 'params'
                        rescue_res = await safe_api_call(client, "GET", "/api/v3/brokerage/orders/historical/fills", payload={"order_id": o_id})
                        
                        if rescue_res.get("success") and rescue_res.get("response", {}).get("fills"):
                            rescue_comm = sum(float(f.get("commission", "0.0") or 0.0) for f in rescue_res["response"]["fills"])
                            db_manager.queue_fee_reconciliation(o_id, rescue_comm)
                            orphan_tracker.pop(o_id, None)
                        else:
                            if orphan_tracker[o_id] >= 5: 
                                db_manager.queue_fee_error(o_id)
                                orphan_tracker.pop(o_id, None)
                                
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"[Fee Recon Error] Critical exception in loop: {e}")

        await asyncio.sleep(120)

async def reconciliation_watchdog():
    await asyncio.sleep(45) 
    while True:
        try:
            if BOT_STATUS["engine_active"]:
                logger.info("🔄 Running 5-minute state reconciliation sweep...")
                await order_manager.sync_active_orders()
        except Exception as e:
            pass
        await asyncio.sleep(300)

async def profit_sweeper_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if BOT_STATUS["engine_active"]:
                portfolios = await asyncio.to_thread(read_json_sync, "portfolios.json")

                for name, pid in portfolios.items():
                    if pid == order_manager.profits_portfolio_id:
                        continue

                    async with aiosqlite.connect("trading_bot.db") as db:
                        db.row_factory = aiosqlite.Row
                        cursor = await db.execute("SELECT SUM(net_pnl) as total_pnl FROM grid_trades WHERE portfolio_id = ? AND fee NOT IN ('PENDING', 'FEE_NOT_FOUND')", (pid,))
                        row = await cursor.fetchone()
                        total_realized = Decimal(str(row["total_pnl"] or "0"))

                        cursor = await db.execute("SELECT amount_swept FROM pnl_swept_registry WHERE portfolio_id = ?", (pid,))
                        db_row = await cursor.fetchone()
                        total_swept = Decimal(str(db_row["amount_swept"] if db_row else "0"))

                    sweepable_pnl = total_realized - total_swept

                    if sweepable_pnl >= Decimal("1.00"):
                        client = portfolio_clients.get(pid)
                        if not client: continue
                        
                        accounts_resp = await safe_api_call(client, "GET", "/api/v3/brokerage/accounts")
                        available_usdc = Decimal("0")
                        
                        if accounts_resp.get("success"):
                            data = accounts_resp.get("response", {})
                            accounts_list = data.get("accounts", []) or data.get("data", [])
                            for acc in accounts_list:
                                currency = acc.get("currency") if isinstance(acc, dict) else getattr(acc, "currency", None)
                                if currency == "USDC":
                                    bal_val = acc.get("available_balance", {}).get("value", "0") if isinstance(acc, dict) else getattr(acc.available_balance, "value", "0")
                                    available_usdc = Decimal(str(bal_val))
                                    break

                        safe_wallet_allowance = available_usdc - Decimal("10.00")
                        final_sweep_amount = min(sweepable_pnl, safe_wallet_allowance)

                        if final_sweep_amount >= Decimal("1.00"):
                            logger.info("💰 [SWEEPER] Realized PnL detected for %s. Total Realized: $%s | Already Swept: $%s. Scheduling sweep for: %s USDC", 
                                        name[:8], total_realized, total_swept, final_sweep_amount)
                            
                            order_manager.enqueue({
                                "action": "TRANSFER_PROFIT",
                                "source_pid": pid,
                                "amount": str(final_sweep_amount)
                            })
        except Exception:
            pass
        await asyncio.sleep(300)

async def autonomous_trading_loop():
    portfolio_balances = {}
    while True:
        try:
            if BOT_STATUS["engine_active"]:
                portfolios = await asyncio.to_thread(read_json_sync, "portfolios.json")

                for name, pid in portfolios.items():
                    client = portfolio_clients.get(pid)
                    if not client: continue

                    try:
                        accounts_resp = await safe_api_call(client, "GET", "/api/v3/brokerage/accounts")
                        if accounts_resp.get("success"):
                            data = accounts_resp.get("response", {})
                            accounts_list = data.get("accounts", []) or data.get("data", [])

                            for acc in accounts_list:
                                if isinstance(acc, dict):
                                    acc_pid = acc.get("retail_portfolio_id", "UNKNOWN")
                                    currency = acc.get("currency")
                                    avail = Decimal(str(acc.get("available_balance", {}).get("value", "0")))
                                else:
                                    acc_pid = getattr(acc, 'retail_portfolio_id', "UNKNOWN")
                                    currency = getattr(acc, 'currency', None)
                                    avail = Decimal(str(getattr(acc.available_balance, 'value', '0')))
                                
                                if acc_pid not in portfolio_balances:
                                    portfolio_balances[acc_pid] = {"AERO": "0", "USDC": "0"}
                                
                                if currency == "AERO":
                                    # TELEMETRY FIX: Strict available balance, dropping held capital
                                    portfolio_balances[acc_pid]["AERO"] = str(avail)
                                elif currency == "USDC":
                                    portfolio_balances[acc_pid]["USDC"] = str(avail)

                    except Exception as e:
                        logger.warning("Failed to fetch balances for %s: %s", name, e)

                    balances = portfolio_balances.get(pid, {"AERO": "0", "USDC": "0"})
                    aero_avail = Decimal(balances.get("AERO", "0"))
                    usdc_avail = Decimal(balances.get("USDC", "0"))

                    # --- LOCAL QUEUE RESERVATION SYSTEM ---
                    # Deduct funds already committed in the execution queues to prevent double-spending
                    reserved_aero = Decimal("0")
                    reserved_usdc = Decimal("0")
                    
                    try:
                        queued_tasks = list(order_manager.priority_queue) + list(order_manager.execution_queue)
                        for task in queued_tasks:
                            if task.get("action") == "PLACE_ORDER" and (task.get("source_pid") == pid or task.get("portfolio_id") == pid):
                                side = task.get("side", "").upper()
                                try:
                                    size_dec = Decimal(str(task.get("size", "0")))
                                    price_dec = Decimal(str(task.get("price", "0")))
                                    
                                    if side == "SELL":
                                        reserved_aero += size_dec
                                    elif side == "BUY":
                                        # Size is base size, price is quote price. Total quote required = size * price
                                        reserved_usdc += (size_dec * price_dec)
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.warning("Queue parsing error: %s", e)

                    aero_effective = max(Decimal("0"), aero_avail - reserved_aero)
                    usdc_effective = max(Decimal("0"), usdc_avail - reserved_usdc)

                    logger.info("Telemetry [%s] -> Price: %s | AERO: %s (Eff: %s) | USDC: %s (Eff: %s) | Queue: %d",
                                name[:8], order_manager.live_ticker_price, 
                                aero_avail, aero_effective, usdc_avail, usdc_effective, 
                                len(order_manager.execution_queue))

                    market_context = {
                        "price": order_manager.live_ticker_price,
                        "current_price": order_manager.live_ticker_price,
                        "live_price": order_manager.live_ticker_price,
                        "timestamp": asyncio.get_event_loop().time(),
                        "portfolio_id": pid, 
                        "aero_bal": str(aero_effective), # Passing safe bounds to the strategy bots
                        "usdc_bal": str(usdc_effective),
                    }

                    try:
                        if order_manager.live_ticker_price > 0:
                            evaluate_rebalances(order_manager, pid, order_manager.live_ticker_price, "AERO-USDC")
                    except Exception: pass

                    try: route_strategy(name, pid, order_manager, market_context)
                    except Exception: pass

                    await asyncio.sleep(0.3)

        except Exception as e:
            logger.error("Error in autonomous_trading_loop iteration: %s", e)

        await asyncio.sleep(10)

async def load_active_orders_from_db():
    loaded_count = 0
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM orders_registry WHERE status = 'OPEN'")
            rows = await cursor.fetchall()
            
            for row in rows:
                temp_id = f"local_{row['client_order_id']}"
                order_manager.active_orders[temp_id] = {
                    "client_order_id": row['client_order_id'], "portfolio_id": row['portfolio_id'],
                    "product_id": row['product_id'], "side": row['side'],
                    "price": str(row['price']), "status": "OPEN", "exchange_id": row['exchange_id']
                }
                loaded_count += 1
        logger.info("✅ Zero-Amnesia Boot: Loaded %d active orders from local registry.", loaded_count)
    except Exception as e:
        logger.error("❌ Failed to load active orders from database: %s", e)

async def boot_sequence():
    await db_manager.init_db()
    await load_active_orders_from_db()
    
    logger.info("Initializing ledger priming sequence...")
    await order_manager.prime_ledger()

    PUB_KEY = os.getenv("COINBASE_PUBLIC_KEY")
    PUB_SECRET = os.getenv("COINBASE_PUBLIC_SECRET")
    if PUB_KEY and PUB_SECRET and WSClient:
        try:
            ws_public = WSClient(api_key=PUB_KEY, api_secret=PUB_SECRET, on_message=order_manager.on_ticker_message)
            ws_public.open()
            ws_public.subscribe(product_ids=["AERO-USDC"], channels=["ticker"])
            logger.info("✅ Public ticker stream connected (Read-Only).")
            BOT_STATUS["websocket_public_connected"] = True
        except Exception as e:
            logger.error("❌ Failed to connect public WebSocket: %s", e)

    from coinbase.websocket import WSUserClient
    try:
        portfolios_config = read_json_sync("portfolios.json")
            
        for name, pid in portfolios_config.items():
            clean_name = name.upper().replace(" ", "_").replace(":", "")
            api_key = os.getenv(f"PORTFOLIO_{clean_name}_KEY")
            api_secret = os.getenv(f"PORTFOLIO_{clean_name}_SECRET")

            if not api_key or not api_secret:
                api_key = os.getenv("COINBASE_PRIVATE_KEY")
                api_secret = os.getenv("COINBASE_PRIVATE_SECRET")

            if api_key and api_secret:
                try:
                    if "\\n" in api_secret: api_secret = api_secret.replace("\\n", "\n")
                    ws_user = WSUserClient(api_key=api_key, api_secret=api_secret, on_message=order_manager.on_user_message)
                    ws_user.open()
                    ws_user.user(product_ids=["AERO-USDC", "AERO-USD", "USDC-USD"]) 
                    active_user_websockets.append(ws_user) 
                    logger.info("✅ Private user stream connected for %s [%s]", name, pid[:8])
                except Exception as e:
                    logger.error("❌ Failed to connect private WebSocket for %s: %s", name, e)

    except Exception as e:
         logger.error("❌ Failed to initialize multi-tenant WebSockets: %s", e)

    BOT_STATUS["engine_active"] = True
    logger.info("🚀 All infrastructure loops online with segregated key scopes.")

@app.before_serving
async def initialize_pillars():
    order_manager.profits_portfolio_id = "ca3e8f25-8dd3-4a58-89b4-884d2e10519b"
    order_manager.loop = asyncio.get_running_loop() 
    await calculate_clock_drift()
    app.add_background_task(boot_sequence)
    app.add_background_task(db_manager.run_db_manager)
    app.add_background_task(process_execution_queue)
    app.add_background_task(fee_reconciliation_loop)
    app.add_background_task(order_manager.skim_dust)
    app.add_background_task(autonomous_trading_loop)
    app.add_background_task(reconciliation_watchdog)
    app.add_background_task(profit_sweeper_loop)

@app.route("/status", methods=["GET"])
async def status_dashboard():
    return jsonify({
        "status": BOT_STATUS,
        "live_price": str(order_manager.live_ticker_price),
        "dust_swept_count": order_manager.total_dust_swept,
        "active_portfolios": list(portfolio_clients.keys())
    })

if __name__ == "__main__":
    import hypercorn.asyncio
    from hypercorn.config import Config

    config = Config()
    config.bind = ["0.0.0.0:5000"]
    asyncio.run(hypercorn.asyncio.serve(app, config))