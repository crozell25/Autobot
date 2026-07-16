# run.py
#!/usr/bin/env python3
import os
import sys
import json
import ast
import logging
import asyncio
import threading
from uuid import uuid4
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler
from quart import Quart, jsonify
from dotenv import load_dotenv

# Coinbase Advanced SDK imports
try:
    from coinbase.rest import RESTClient
    from coinbase.websocket import WSClient, WSUserClient
except Exception:
    RESTClient = None
    WSClient = None
    WSUserClient = None

# Four Pillars Architecture Imports
from manager import CoinbaseOrderManager
from utils import safe_api_call, calculate_clock_drift
import db_manager
from order_rebalancer import evaluate_rebalances

# Dynamic Strategy Module Imports
try:
    import strategies.aero_asymmetric_grid_v2 as aero_grid
    
    import strategies.aero_simple_ladder as aero_simple
    import strategies.aero_aggressive_scalper as aero_scalper
except ImportError as e:
    logging.getLogger("Orchestrator").warning("Dynamic strategy import failed: %s", e)
    aero_grid = None
    
    aero_simple = None
    aero_scalper = None

from hypercorn.asyncio import serve
from hypercorn.config import Config
from hypercorn.middleware import DispatcherMiddleware

# =====================================================================
# ENVIRONMENT & LOGGING INITIALIZATION
# =====================================================================
load_dotenv()

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

log_handlers = [
    RotatingFileHandler("engine.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"),
    logging.StreamHandler(sys.stdout),
]
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=log_handlers)
logger = logging.getLogger("Orchestrator")

# =====================================================================
# APP & STATE REGISTRY
# =====================================================================
app = Quart(__name__)

BOT_STATUS = {
    "engine_active": False,
    "websocket_public_connected": False,
    "errors": []
}

# Globally keep track of the private WebSockets so they don't get garbage collected
active_user_websockets = []

# =====================================================================
# MULTI-TENANT CREDENTIAL ROUTER
# =====================================================================
def load_portfolio_clients():
    clients = {}
    try:
        with open("portfolios.json", "r", encoding="utf-8") as f:
            portfolios = json.load(f)
    except Exception as e:
        logger.error("❌ Failed to read portfolios.json: %s", e)
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
            if "\\n" in api_secret:
                api_secret = api_secret.replace("\\n", "\n")
            try:
                clients[pid] = RESTClient(api_key=api_key, api_secret=api_secret)
                logger.info("✅ Bound write permissions for portfolio: %s [%s]", name, pid[:8])
            except Exception as e:
                logger.error("❌ Failed to create REST client for %s: %s", name, e)
        else:
            logger.error("❌ Missing credentials or RESTClient for strategy: %s", name)
    return clients

portfolio_clients = load_portfolio_clients()
order_manager = CoinbaseOrderManager(portfolio_clients)

# =====================================================================
# STRATEGY ROUTER LOGIC
# =====================================================================
def route_strategy(portfolio_name, pid, order_manager, market_data):
    name_upper = portfolio_name.upper()

    if name_upper.startswith("GRID"):
        if aero_grid:
            try:
                aero_grid.run_strategy(order_manager, market_data)
            except Exception as e:
                logger.exception("Strategy execution error (grid) for %s: %s", portfolio_name, e)
                
    
                
    elif name_upper.startswith("SCALP"):
        if aero_scalper:
            try:
                aero_scalper.run_strategy(order_manager, market_data)
            except Exception as e:
                logger.exception("Strategy execution error (scalp) for %s: %s", portfolio_name, e)

    elif name_upper.startswith("STRAT"):
        if aero_simple is None:
            logger.error(f"❌ '{portfolio_name}' triggered, but aero_simple_ladder.py failed to load!")
            return
            
        try:
            aero_simple.run_strategy(order_manager, market_data)
        except Exception as e:
            logger.exception("Strategy execution error (simple) for %s: %s", portfolio_name, e)

# =====================================================================
# UTILITIES: tolerant parsing helpers
# =====================================================================
def _extract_decimal(field):
    if field is None:
        return Decimal("0")
    if isinstance(field, (int, float, Decimal)):
        return Decimal(str(field))
    if isinstance(field, dict):
        v = field.get("value") or field.get("amount") or field.get("available") or "0"
    else:
        v = getattr(field, "value", None) or getattr(field, "amount", None) or str(field)
    s = str(v).strip().replace(",", "").replace("$", "").replace("USDC", "").replace("AERO", "").strip()
    if s == "" or s.lower() in ("none", "null", "nan"):
        return Decimal("0")
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")

def _normalize_accounts_response(resp):
    if isinstance(resp, (dict, list)):
        parsed = resp
    else:
        parsed = None
        if hasattr(resp, "json"):
            try: parsed = resp.json()
            except Exception: parsed = None
        if parsed is None:
            body = getattr(resp, "text", None) or str(resp)
            try: parsed = json.loads(body)
            except Exception:
                try: parsed = ast.literal_eval(body)
                except Exception: parsed = None

    if parsed is None: return []

    if isinstance(parsed, dict):
        for key in ("accounts", "data", "results", "items"):
            if key in parsed and isinstance(parsed[key], list):
                return parsed[key]
        for v in parsed.values():
            if isinstance(v, list):
                return v
        return []
    elif isinstance(parsed, list):
        return parsed
    else:
        return []

# =====================================================================
# CONCURRENT EXECUTION ENGINE
# =====================================================================
async def execute_single_task(task):
    """Processes a single execution payload. Designed to run in parallel."""
    action_type = task.get("action")
    pid = task.get("source_pid")
    client = portfolio_clients.get(pid)

    if not client and not (os.getenv("DRY_RUN", "0") == "1"):
        logger.error("❌ Cannot execute action %s: No API client found for portfolio UUID %s", action_type, pid)
        return

    # --- PROCESS PLACE_ORDER ACTION ---
    if action_type == "PLACE_ORDER":
        safe_size_str = "{:.2f}".format(float(task["size"]))
        safe_price_str = "{:.5f}".format(float(task["price"]))
        
        cid = task.get("client_order_id", str(uuid4()))
        side = task.get("side", "").upper()
        prod = task.get("product_id")
        post = task.get("post_only", True)
        stp = task.get("stp_id", str(uuid4()))

        if os.getenv("DRY_RUN", "0") == "1":
            logger.info("[DRY_RUN] Would place %s order for %s @ %s", side, safe_size_str, safe_price_str)
        else:
            is_successful = False
            error_msg = "Unknown Error"
            exchange_id_val = None

            try:
                # Use native SDK typed calls to completely avoid nested dictionary parameter bugs
                if side == "BUY":
                    api_response = await asyncio.to_thread(
                        client.limit_order_gtc_buy,
                        client_order_id=cid,
                        product_id=prod,
                        base_size=safe_size_str,
                        limit_price=safe_price_str,
                        post_only=post,
                        self_trade_prevention_id=stp
                    )
                else:
                    api_response = await asyncio.to_thread(
                        client.limit_order_gtc_sell,
                        client_order_id=cid,
                        product_id=prod,
                        base_size=safe_size_str,
                        limit_price=safe_price_str,
                        post_only=post,
                        self_trade_prevention_id=stp
                    )

                if isinstance(api_response, dict):
                    is_successful = api_response.get("success", False)
                    if not is_successful:
                        err_data = api_response.get("error_response", {})
                        error_msg = err_data.get("message", str(api_response))
                    else:
                        exchange_id_val = api_response.get("success_response", {}).get("order_id")
                elif api_response is not None:
                    is_successful = getattr(api_response, "success", False)
                    if not is_successful:
                        err_resp = getattr(api_response, "error_response", None)
                        if err_resp:
                            error_msg = getattr(err_resp, "message", str(err_resp))
                        else:
                            error_msg = str(getattr(api_response, "failure_reason", api_response))
                    else:
                        success_resp = getattr(api_response, "success_response", None)
                        if success_resp:
                            exchange_id_val = getattr(success_resp, "order_id", None)
                            
            except Exception as e:
                error_msg = str(e)

            if is_successful:
                logger.info("✅ Successfully placed %s order for portfolio %s", side, pid[:8])
                
                temp_id = f"local_{cid}"
                order_manager.active_orders[temp_id] = {
                    "client_order_id": cid,
                    "portfolio_id": pid,
                    "product_id": prod,
                    "side": side,
                    "price": safe_price_str,
                    "status": "OPEN",
                    "exchange_id": exchange_id_val 
                }
                
                try:
                    db_manager.log_new_order(
                        client_order_id=cid,
                        pid=pid,
                        product_id=prod,
                        side=side,
                        price=safe_price_str,
                        size=safe_size_str,
                        stp_id=stp
                    )
                except Exception as e:
                    logger.error("❌ Failed to queue new order to database: %s", e)
            else:
                logger.error("❌ Order placement rejected by Coinbase for %s: %s | Payload: %s", 
                             pid[:8], error_msg, safe_price_str)

    # --- PROCESS CANCEL_ORDER ACTION ---
    elif action_type == "CANCEL_ORDER":
        client_order_id = task.get("client_order_id")
        exchange_id = task.get("exchange_id")
        
        if not exchange_id:
            logger.warning("❌ Cannot cancel: Missing exchange_id for local order %s", client_order_id)
            order_manager.active_orders.pop(f"local_{client_order_id}", None)
            return

        if not hasattr(order_manager, 'dead_orders'):
            order_manager.dead_orders = set()

        if exchange_id in order_manager.dead_orders:
            order_manager.active_orders.pop(f"local_{client_order_id}", None)
            return

        if os.getenv("DRY_RUN", "0") == "1":
            logger.info("[DRY_RUN] Would cancel order exchange_id: %s", exchange_id)
            success = True
        else:
            success = False
            try:
                api_response = await asyncio.to_thread(client.cancel_orders, order_ids=[exchange_id])
                
                if isinstance(api_response, dict):
                    results = api_response.get("results", [])
                    if results and results[0].get("success"):
                        success = True
                elif api_response is not None:
                    results = getattr(api_response, "results", [])
                    if results and getattr(results[0], "success", False):
                        success = True
            except Exception as e:
                logger.error("❌ Native cancel API call failed for %s: %s", exchange_id, e)

        order_manager.active_orders.pop(f"local_{client_order_id}", None)

        if success:
            logger.info("✅ Cancel confirmed and memory synced for: %s", str(client_order_id)[:8])
            try: db_manager.update_order_status(client_order_id, 'CANCELLED')
            except Exception: pass
        else:
            logger.warning("⚠️ Zombie Order detected! Cancel failed. Blacklisting ghost: %s", str(client_order_id)[:8])
            order_manager.dead_orders.add(exchange_id)
            try: db_manager.update_order_status(client_order_id, 'GHOST_PURGED')
            except Exception: pass

    # --- PROCESS TRANSFER_PROFIT ACTION ---
    elif action_type == "TRANSFER_PROFIT":
        safe_amount = str(round(Decimal(str(task["amount"])), 6))
        transfer_payload = {
            "source_portfolio_uuid": pid,
            "target_portfolio_uuid": order_manager.profits_portfolio_id,
            "funds": {
                "value": safe_amount,
                "currency": "USDC"
            }
        }
        
        if os.getenv("DRY_RUN", "0") == "1":
            logger.info("[DRY_RUN] Would transfer profit: %s", transfer_payload)
            success = True
        else:
            success_dict = await safe_api_call(client, "POST", "/api/v3/brokerage/portfolios/move_funds", payload=transfer_payload)
            success = success_dict.get("success", False)

        if success:
            logger.info("✅ Profit transfer successful for portfolio %s: %s USDC", pid[:8], safe_amount)
            if os.getenv("DRY_RUN", "0") != "1":
                try:
                    import aiosqlite
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
                except Exception as e:
                    logger.error("❌ Failed to log successful sweep to tracking registry: %s", e)
        else:
            logger.error("❌ Profit transfer rejected by exchange.")

async def process_execution_queue():
    while True:
        batch = await order_manager.drain_execution_queue(limit=5)
        if batch:
            await asyncio.gather(*(execute_single_task(task) for task in batch))
            await asyncio.sleep(0.5) 
        else:
            await asyncio.sleep(1)

async def profit_sweeper_loop():
    await asyncio.sleep(60) 
    
    while True:
        try:
            if BOT_STATUS["engine_active"]:
                import aiosqlite
                async with aiosqlite.connect("trading_bot.db") as db:
                    await db.execute("""
                        CREATE TABLE IF NOT EXISTS pnl_swept_registry (
                            portfolio_id TEXT PRIMARY KEY,
                            amount_swept TEXT
                        )
                    """)
                    await db.commit()

                try:
                    with open("portfolios.json", "r", encoding="utf-8") as f:
                        portfolios = json.load(f)
                except Exception: portfolios = {}

                for name, pid in portfolios.items():
                    if pid == order_manager.profits_portfolio_id: continue

                    async with aiosqlite.connect("trading_bot.db") as db:
                        db.row_factory = aiosqlite.Row
                        cursor = await db.execute("SELECT SUM(pnl) as total_pnl FROM grid_trades WHERE portfolio_id = ?", (pid,))
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
                        accounts_list = _normalize_accounts_response(accounts_resp)
                        
                        available_usdc = Decimal("0")
                        for acc in accounts_list:
                            currency = getattr(acc, 'currency', acc.get("currency"))
                            if currency == "USDC":
                                val = getattr(acc.available_balance, 'value', '0') if hasattr(acc, 'available_balance') else acc.get("available_balance", {}).get("value", "0")
                                available_usdc = Decimal(str(val))
                                break

                        safe_wallet_allowance = available_usdc - Decimal("10.00")
                        final_sweep_amount = min(sweepable_pnl, safe_wallet_allowance)

                        if final_sweep_amount >= Decimal("1.00"):
                            logger.info("💰 [SWEEPER] Realized PnL detected for %s. Total Realized: $%s | Already Swept: $%s. Scheduling sweep for: %s USDC", 
                                        name[:8], total_realized, total_swept, final_sweep_amount)
                            order_manager.execution_queue.append({
                                "action": "TRANSFER_PROFIT", "source_pid": pid, "amount": str(final_sweep_amount)
                            })

        except Exception as e:
            logger.error("❌ Error in profit_sweeper_loop iteration: %s", e)
        await asyncio.sleep(300) 

async def autonomous_trading_loop():
    """
    Monitors portfolios.json configurations, pulls data feeds, and routes traffic.
    """
    import time
    portfolio_balances = {}
    
    while True:
        try:
            if BOT_STATUS["engine_active"]:
                try:
                    with open("portfolios.json", "r", encoding="utf-8") as f:
                        portfolios = json.load(f)
                except Exception:
                    portfolios = {}

                for name, pid in portfolios.items():
                    client = portfolio_clients.get(pid)
                    if not client: continue

                    try:
                        params = {
                            "limit": 250, 
                            "retail_portfolio_id": pid
                        }
                        accounts_resp = await safe_api_call(client, "GET", "/api/v3/brokerage/accounts", payload=params)
                        
                        accounts_list = []
                        if isinstance(accounts_resp.get("response"), dict):
                            accounts_list = accounts_resp["response"].get("accounts", [])
                        
                        aero_avail = Decimal("0")
                        aero_hold = Decimal("0")
                        usdc_avail = Decimal("0")

                        for acc in accounts_list:
                            if hasattr(acc, 'currency'): 
                                currency = acc.currency
                                avail = Decimal(str(getattr(acc.available_balance, 'value', '0')))
                                hold = Decimal(str(getattr(acc.hold, 'value', '0')))
                            else: 
                                currency = acc.get("currency")
                                avail = Decimal(str(acc.get("available_balance", {}).get("value", "0")))
                                hold = Decimal(str(acc.get("hold", {}).get("value", "0")))
                            
                            if not currency:
                                continue

                            if currency == "AERO":
                                aero_avail += avail
                                aero_hold += hold
                            elif currency == "USDC":
                                usdc_avail += avail

                        portfolio_balances[pid] = {
                            "AERO": str(aero_avail + aero_hold),
                            "USDC": str(usdc_avail)
                        }

                    except Exception as e:
                        logger.warning("Failed to fetch balances for %s: %s", name, e)

                    balances = portfolio_balances.get(pid, {"AERO": "0", "USDC": "0"})
                    aero_bal = balances.get("AERO", "0")
                    usdc_bal = balances.get("USDC", "0")

                    logger.info("Telemetry [%s] -> Price: %s | AERO: %s | USDC: %s | Queue: %d",
                                name[:8], order_manager.live_ticker_price, aero_bal, usdc_bal, len(order_manager.execution_queue))

                    # ---------------------------------------------------------
                    # CRITICAL FIX: Synchronize the timing architecture.
                    # ---------------------------------------------------------
                    current_unix_time = time.time()
                    
                    market_context = {
                        "price": order_manager.live_ticker_price,
                        "current_price": order_manager.live_ticker_price,
                        "live_price": order_manager.live_ticker_price,
                        "timestamp": current_unix_time,
                        "portfolio_id": pid,
                        "aero_bal": aero_bal,
                        "usdc_bal": usdc_bal,
                    }

                    try:
                        if order_manager.live_ticker_price > 0:
                            evaluate_rebalances(order_manager, pid, order_manager.live_ticker_price, "AERO-USDC")
                    except Exception as e:
                        pass

                    try:
                        route_strategy(name, pid, order_manager, market_context)
                    except Exception as e:
                        logger.exception("Error routing strategy for %s: %s", name, e)

                    await asyncio.sleep(0.3)
        except Exception as e:
            logger.error("Error in autonomous_trading_loop iteration: %s", e)

        await asyncio.sleep(10)


# =====================================================================
# STARTUP HANDSHAKE ENGINE
# =====================================================================

async def load_active_orders_from_db():
    import aiosqlite
    loaded_count = 0
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM orders_registry WHERE status = 'OPEN'")
            rows = await cursor.fetchall()
            
            for row in rows:
                temp_id = f"local_{row['client_order_id']}"
                order_manager.active_orders[temp_id] = {
                    "client_order_id": row['client_order_id'],
                    "portfolio_id": row['portfolio_id'],
                    "product_id": row['product_id'],
                    "side": row['side'],
                    "price": str(row['price']),
                    "status": "OPEN",
                    "exchange_id": row['exchange_id']
                }
                loaded_count += 1
        logger.info("✅ Zero-Amnesia Boot: Loaded %d active orders from local registry.", loaded_count)
    except Exception as e:
        logger.error("❌ Failed to load active orders from database: %s", e)

@app.before_serving
async def boot_sequence():
    order_manager.loop = asyncio.get_running_loop()
    await db_manager.init_db()
    await load_active_orders_from_db()
    
    await order_manager.start_background_tasks()
    app.add_background_task(db_manager.run_db_manager)
    app.add_background_task(process_execution_queue)
    app.add_background_task(autonomous_trading_loop)
    app.add_background_task(profit_sweeper_loop)

    PUB_KEY = os.getenv("COINBASE_PUBLIC_KEY")
    PUB_SECRET = os.getenv("COINBASE_PUBLIC_SECRET")
    
    if PUB_KEY and PUB_SECRET and WSClient:
        def start_market_ws():
            try:
                clean_secret = PUB_SECRET.replace("\\n", "\n")
                market_ws = WSClient(api_key=PUB_KEY, api_secret=clean_secret, on_message=order_manager.on_ticker_message)
                market_ws.open()
                market_ws.subscribe(product_ids=["AERO-USDC"], channels=["ticker", "heartbeats"])
                market_ws.run_forever_with_exception_check()
            except Exception as e:
                logger.error(f"Market WS crashed: {e}")
        
        threading.Thread(target=start_market_ws, daemon=True, name="MarketWSThread").start()
        logger.info("✅ Public ticker stream connecting (Read-Only).")
        BOT_STATUS["websocket_public_connected"] = True
    else:
        logger.warning("⚠️ No global public credentials found to open market WebSocket.")

    try:
        with open("portfolios.json", "r", encoding="utf-8") as f:
            portfolios_config = json.load(f)
            
        for name, pid in portfolios_config.items():
            clean_name = name.upper().replace(" ", "_").replace(":", "")
            api_key = os.getenv(f"PORTFOLIO_{clean_name}_KEY")
            api_secret = os.getenv(f"PORTFOLIO_{clean_name}_SECRET")

            if not api_key or not api_secret:
                api_key = os.getenv("COINBASE_PRIVATE_KEY")
                api_secret = os.getenv("COINBASE_PRIVATE_SECRET")

            if api_key and api_secret and WSUserClient:
                def start_user_ws(ak, asec, p_name):
                    try:
                        clean_sec = asec.replace("\\n", "\n")
                        ws_user = WSUserClient(api_key=ak, api_secret=clean_sec, on_message=order_manager.on_user_message)
                        ws_user.open()
                        ws_user.user(product_ids=[]) 
                        active_user_websockets.append(ws_user)
                        ws_user.run_forever_with_exception_check()
                    except Exception as e:
                        logger.error(f"User WS crashed for {p_name}: {e}")

                threading.Thread(target=start_user_ws, args=(api_key, api_secret, name), daemon=True, name=f"UserWS_{clean_name}").start()
                logger.info("✅ Private user stream connecting for %s [%s]", name, pid[:8])
            else:
                logger.warning("⚠️ No credentials found to open user WebSocket for %s", name)

    except Exception as e:
         logger.error("❌ Failed to initialize multi-tenant WebSockets: %s", e)

    BOT_STATUS["engine_active"] = True
    logger.info("🚀 Trading engine initialized. WebSockets and Strategies are LIVE.")

# =====================================================================
# REST ENDPOINTS
# =====================================================================
@app.route("/status", methods=["GET"])
async def status_dashboard():
    return jsonify({
        "status": BOT_STATUS,
        "live_price": str(order_manager.live_ticker_price),
        "dust_swept_count": order_manager.total_dust_swept,
        "active_portfolios": list(portfolio_clients.keys())
    })

# Combine Quart + FastAPI dashboard
from db_manager import app as dashboard_app
combined_app = DispatcherMiddleware({"/": app, "/dashboard": dashboard_app})

config = Config()
config.bind = ["0.0.0.0:8000"]

# =====================================================================
# ENTRYPOINT
# =====================================================================
if __name__ == "__main__":
    try:
        # Run the server
        asyncio.run(serve(combined_app, config))
    except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
        # Catch CTRL+C and swallow the ugly CancelledError traceback
        print("\n✅ Trading Engine successfully powered down.")
    except Exception as e:
        logger.error("Engine terminated with error: %s", e)