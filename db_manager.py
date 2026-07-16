# db_manager.py
import aiosqlite
import asyncio
import logging

logger = logging.getLogger("DBManager")

_db_queue = None

def get_db_queue():
    global _db_queue
    if _db_queue is None:
        _db_queue = asyncio.Queue()
    return _db_queue

async def init_db():
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS orders_registry (
                    client_order_id TEXT PRIMARY KEY,
                    exchange_id TEXT,
                    portfolio_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price DECIMAL NOT NULL,
                    size DECIMAL NOT NULL,
                    status TEXT DEFAULT 'OPEN',
                    stp_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS grid_trades (
                    trade_id TEXT PRIMARY KEY,
                    client_order_id TEXT NOT NULL,
                    portfolio_id TEXT NOT NULL,
                    pnl DECIMAL NOT NULL,
                    buy_price DECIMAL,
                    sell_price DECIMAL,
                    size DECIMAL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (client_order_id) REFERENCES orders_registry(client_order_id)
                )
            """)
            
            try:
                await db.execute("ALTER TABLE grid_trades ADD COLUMN buy_price DECIMAL")
                await db.execute("ALTER TABLE grid_trades ADD COLUMN sell_price DECIMAL")
                await db.execute("ALTER TABLE grid_trades ADD COLUMN size DECIMAL")
            except Exception:
                pass 
                
            try:
                await db.execute("ALTER TABLE grid_trades ADD COLUMN fee TEXT DEFAULT 'PENDING'")
                await db.execute("ALTER TABLE grid_trades ADD COLUMN net_pnl DECIMAL")
            except Exception:
                pass 

            await db.execute("""
                CREATE TABLE IF NOT EXISTS pnl_swept_registry (
                    portfolio_id TEXT PRIMARY KEY,
                    amount_swept TEXT
                )
            """)

            await db.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id TEXT NOT NULL,
                    aero_balance DECIMAL,
                    usdc_balance DECIMAL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders_registry(status)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_trades_portfolio ON grid_trades(portfolio_id)")
            await db.commit()
            logger.info("✅ Database schema initialized successfully.")
    except Exception as e:
        logger.error("❌ Failed to initialize database: %s", e)

async def run_db_manager():
    logger.info("🗄️ Async Database Manager online. Listening for ledger events...")
    queue = get_db_queue()
    while True:
        try:
            task = await queue.get()
            action = task.get("action")
            
            async with aiosqlite.connect("trading_bot.db") as db:
                if action == "UPSERT_ORDER":
                    await db.execute(
                        "INSERT OR REPLACE INTO orders_registry (client_order_id, portfolio_id, product_id, side, price, size, status, stp_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (task.get('client_order_id'), task.get('portfolio_id'), task.get('product_id'), task.get('side'), str(task.get('price')), str(task.get('size')), task.get('status'), task.get('stp_id'))
                    )
                elif action == "UPDATE_STATUS":
                    await db.execute(
                        "UPDATE orders_registry SET status = ? WHERE client_order_id = ?", 
                        (task.get('status'), task.get('client_order_id'))
                    )
                elif action == "LOG_PNL":
                    await db.execute(
                        "INSERT INTO grid_trades (trade_id, client_order_id, portfolio_id, pnl, buy_price, sell_price, size, fee) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')",
                        (
                            task.get('trade_id'), 
                            task.get('client_order_id'), 
                            task.get('portfolio_id'), 
                            str(task.get('pnl')),
                            str(task.get('buy_price', '0')),
                            str(task.get('sell_price', '0')),
                            str(task.get('size', '0'))
                        )
                    )
                elif action == "RECONCILE_FEE":
                    await db.execute(
                        "UPDATE grid_trades SET fee = ?, net_pnl = pnl - ? WHERE trade_id = ? AND fee = 'PENDING'",
                        (str(task.get('commission')), str(task.get('commission')), task.get('trade_id'))
                    )
                elif action == "MARK_FEE_ERROR":
                    await db.execute(
                        "UPDATE grid_trades SET fee = 'FEE_NOT_FOUND' WHERE trade_id = ?",
                        (task.get('trade_id'),)
                    )
                elif action == "PURGE_GHOSTS":
                    await db.execute(
                        "UPDATE orders_registry SET status = 'GHOST_PURGED' WHERE client_order_id = ?",
                        (task.get('client_order_id'),)
                    )
                elif action == "MASS_PURGE_GHOSTS":
                    await db.execute(
                        "UPDATE orders_registry SET status = 'GHOST_PURGED' WHERE status = 'OPEN' AND portfolio_id = ?",
                        (task.get('portfolio_id'),)
                    )
                await db.commit()
            
            queue.task_done()
            
        except Exception as e:
            logger.error(f"Database write fault: {e}")

def log_new_order(client_order_id, pid, product_id, side, price, size, stp_id):
    get_db_queue().put_nowait({
        "action": "UPSERT_ORDER", "client_order_id": client_order_id, 
        "portfolio_id": pid, "product_id": product_id, "side": side, 
        "price": price, "size": size, "status": "OPEN", "stp_id": stp_id
    })

def update_order_status(client_order_id, status):
    get_db_queue().put_nowait({"action": "UPDATE_STATUS", "client_order_id": client_order_id, "status": status})

def log_failed_order(client_order_id):
    """Explicitly marks an order as FAILED to prevent ghost tracking."""
    update_order_status(client_order_id, "FAILED")

def log_trade_pnl(trade_id, client_order_id, pid, pnl, buy_price=0, sell_price=0, size=0):
    get_db_queue().put_nowait({
        "action": "LOG_PNL", 
        "trade_id": trade_id, 
        "client_order_id": client_order_id,
        "portfolio_id": pid, 
        "pnl": pnl,
        "buy_price": buy_price,
        "sell_price": sell_price,
        "size": size
    })    

def purge_ghost_order(client_order_id):
    get_db_queue().put_nowait({"action": "PURGE_GHOSTS", "client_order_id": client_order_id})

def mass_purge_portfolio_ghosts(portfolio_id):
    """Emergency helper to wipe stuck OPEN orders for a specific portfolio."""
    get_db_queue().put_nowait({"action": "MASS_PURGE_GHOSTS", "portfolio_id": portfolio_id})

def queue_fee_reconciliation(trade_id, commission):
    get_db_queue().put_nowait({"action": "RECONCILE_FEE", "trade_id": trade_id, "commission": commission})

def queue_fee_error(trade_id):
    get_db_queue().put_nowait({"action": "MARK_FEE_ERROR", "trade_id": trade_id})

async def get_pending_fees_by_portfolio():
    pending = {}
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT trade_id, portfolio_id FROM grid_trades WHERE fee = 'PENDING'")
            rows = await cursor.fetchall()
            for row in rows:
                pid = row["portfolio_id"]
                tid = row["trade_id"]
                if pid not in pending:
                    pending[pid] = []
                pending[pid].append(tid)
    except Exception as e:
        logger.error("Failed to fetch pending fees: %s", e)
    return pending

def emergency_clean_db():
    """Synchronous cleanup block designed to run from terminal."""
    import sqlite3
    print("Initiating emergency sweep of ALL stale OPEN orders...")
    conn = sqlite3.connect("trading_bot.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE orders_registry SET status = 'GHOST_PURGED' WHERE status = 'OPEN'")
    purged = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"✅ Successfully purged {purged} ghost orders. Your reports will now be accurate.")

if __name__ == "__main__":
    emergency_clean_db()
# --- Async Query Helpers ---

async def get_open_orders(portfolio_id: str):
    """Fetch all OPEN orders for a given portfolio."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM orders_registry WHERE portfolio_id = ? AND status = 'OPEN'",
                (portfolio_id,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch open orders for {portfolio_id}: {e}")
        return []

async def get_recent_trades(limit: int = 50):
    """Retrieve the most recent trades across all portfolios."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM grid_trades ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch recent trades: {e}")
        return []

async def get_portfolio_state(portfolio_id: str):
    """Get the latest balance snapshot for a portfolio."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT aero_balance, usdc_balance, timestamp
                FROM portfolio_state
                WHERE portfolio_id = ?
                ORDER BY timestamp DESC LIMIT 1
                """,
                (portfolio_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"Failed to fetch portfolio state for {portfolio_id}: {e}")
        return None


# --- Async Query Helpers ---

async def get_open_orders(portfolio_id: str):
    """Fetch all OPEN orders for a given portfolio."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM orders_registry WHERE portfolio_id = ? AND status = 'OPEN'",
                (portfolio_id,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch open orders for {portfolio_id}: {e}")
        return []

async def get_recent_trades(limit: int = 50):
    """Retrieve the most recent trades across all portfolios."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM grid_trades ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch recent trades: {e}")
        return []

async def get_portfolio_state(portfolio_id: str):
    """Get the latest balance snapshot for a portfolio."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT aero_balance, usdc_balance, timestamp
                FROM portfolio_state
                WHERE portfolio_id = ?
                ORDER BY timestamp DESC LIMIT 1
                """,
                (portfolio_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"Failed to fetch portfolio state for {portfolio_id}: {e}")
        return None

# --- Async Insert/Update Helpers ---

async def record_portfolio_state(portfolio_id: str, aero_balance: float, usdc_balance: float):
    """Log a new portfolio balance snapshot."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            await db.execute(
                "INSERT INTO portfolio_state (portfolio_id, aero_balance, usdc_balance) VALUES (?, ?, ?)",
                (portfolio_id, str(aero_balance), str(usdc_balance))
            )
            await db.commit()
            logger.info(f"📊 Portfolio state recorded for {portfolio_id}: AERO={aero_balance}, USDC={usdc_balance}")
    except Exception as e:
        logger.error(f"Failed to record portfolio state for {portfolio_id}: {e}")

async def update_order_price(client_order_id: str, new_price: float):
    """Update the price of an existing order."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            await db.execute(
                "UPDATE orders_registry SET price = ? WHERE client_order_id = ?",
                (str(new_price), client_order_id)
            )
            await db.commit()
            logger.info(f"💰 Updated price for order {client_order_id}: {new_price}")
    except Exception as e:
        logger.error(f"Failed to update price for order {client_order_id}: {e}")

async def mark_trade_completed(trade_id: str):
    """Mark a trade as completed and reconcile its fee if pending."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            await db.execute(
                "UPDATE grid_trades SET fee = 'SETTLED' WHERE trade_id = ? AND fee = 'PENDING'",
                (trade_id,)
            )
            await db.commit()
            logger.info(f"✅ Trade {trade_id} marked as completed.")
    except Exception as e:
        logger.error(f"Failed to mark trade {trade_id} as completed: {e}")

# --- Async Analytics Helpers ---

async def get_total_realized_pnl(portfolio_id: str):
    """Calculate total realized PnL for a portfolio."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            cursor = await db.execute(
                "SELECT SUM(net_pnl) FROM grid_trades WHERE portfolio_id = ? AND fee = 'SETTLED'",
                (portfolio_id,)
            )
            result = await cursor.fetchone()
            return float(result[0]) if result and result[0] is not None else 0.0
    except Exception as e:
        logger.error(f"Failed to calculate total realized PnL for {portfolio_id}: {e}")
        return 0.0

async def get_average_trade_size(portfolio_id: str):
    """Compute average trade size for a portfolio."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            cursor = await db.execute(
                "SELECT AVG(size) FROM grid_trades WHERE portfolio_id = ?",
                (portfolio_id,)
            )
            result = await cursor.fetchone()
            return float(result[0]) if result and result[0] is not None else 0.0
    except Exception as e:
        logger.error(f"Failed to compute average trade size for {portfolio_id}: {e}")
        return 0.0

async def get_portfolio_performance_summary(portfolio_id: str):
    """Return a summary of portfolio performance metrics."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT 
                    COUNT(*) AS total_trades,
                    SUM(net_pnl) AS total_pnl,
                    AVG(size) AS avg_size,
                    MIN(timestamp) AS first_trade,
                    MAX(timestamp) AS last_trade
                FROM grid_trades
                WHERE portfolio_id = ?
                """,
                (portfolio_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else {}
    except Exception as e:
        logger.error(f"Failed to generate performance summary for {portfolio_id}: {e}")
        return {}

# --- Async Dashboard Aggregation Helpers ---

async def get_daily_pnl_trend(portfolio_id: str, days: int = 7):
    """Return daily total PnL for the last N days."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT DATE(timestamp) AS day, SUM(net_pnl) AS total_pnl
                FROM grid_trades
                WHERE portfolio_id = ? AND fee = 'SETTLED'
                GROUP BY day
                ORDER BY day DESC
                LIMIT ?
                """,
                (portfolio_id, days)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch daily PnL trend for {portfolio_id}: {e}")
        return []

async def get_trade_frequency_by_hour(portfolio_id: str):
    """Count trades grouped by hour of day."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT STRFTIME('%H', timestamp) AS hour, COUNT(*) AS trade_count
                FROM grid_trades
                WHERE portfolio_id = ?
                GROUP BY hour
                ORDER BY hour
                """,
                (portfolio_id,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to compute trade frequency by hour for {portfolio_id}: {e}")
        return []

async def get_rolling_avg_pnl(portfolio_id: str, window: int = 10):
    """Compute rolling average PnL over the last N trades."""
    try:
        async with aiosqlite.connect("trading_bot.db") as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT trade_id, net_pnl
                FROM grid_trades
                WHERE portfolio_id = ? AND fee = 'SETTLED'
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (portfolio_id, window)
            )
            rows = await cursor.fetchall()
            if not rows:
                return 0.0
            pnl_values = [float(row["net_pnl"]) for row in rows if row["net_pnl"] is not None]
            return sum(pnl_values) / len(pnl_values) if pnl_values else 0.0
    except Exception as e:
        logger.error(f"Failed to compute rolling average PnL for {portfolio_id}: {e}")
        return 0.0
# --- Chart.js‑Ready JSON Formatting Helpers ---

import json

async def format_daily_pnl_for_chart(portfolio_id: str, days: int = 7):
    """Return daily PnL trend formatted for Chart.js line chart."""
    data = await get_daily_pnl_trend(portfolio_id, days)
    labels = [row["day"] for row in reversed(data)]
    values = [float(row["total_pnl"]) for row in reversed(data)]
    chart_json = {
        "labels": labels,
        "datasets": [{
            "label": f"{portfolio_id} Daily PnL",
            "data": values,
            "borderColor": "#4CAF50",
            "backgroundColor": "rgba(76,175,80,0.2)",
            "fill": True,
            "tension": 0.3
        }]
    }
    return json.dumps(chart_json)

async def format_trade_frequency_for_chart(portfolio_id: str):
    """Return hourly trade frequency formatted for Chart.js bar chart."""
    data = await get_trade_frequency_by_hour(portfolio_id)
    labels = [row["hour"] for row in data]
    values = [row["trade_count"] for row in data]
    chart_json = {
        "labels": labels,
        "datasets": [{
            "label": f"{portfolio_id} Trades per Hour",
            "data": values,
            "backgroundColor": "#2196F3"
        }]
    }
    return json.dumps(chart_json)

async def format_rolling_avg_pnl_for_chart(portfolio_id: str, window: int = 10):
    """Return rolling average PnL formatted for Chart.js gauge or single‑value display."""
    avg_pnl = await get_rolling_avg_pnl(portfolio_id, window)
    chart_json = {
        "label": f"{portfolio_id} Rolling Avg PnL ({window} trades)",
        "value": avg_pnl,
        "color": "#FFC107" if avg_pnl >= 0 else "#F44336"
    }
    return json.dumps(chart_json)

# --- FastAPI Dashboard Endpoints ---

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Trading Bot Analytics API")

@app.get("/api/pnl/daily/{portfolio_id}")
async def daily_pnl_chart(portfolio_id: str, days: int = 7):
    """Serve daily PnL trend for Chart.js line chart."""
    chart_json = await format_daily_pnl_for_chart(portfolio_id, days)
    return JSONResponse(content=json.loads(chart_json))

@app.get("/api/trades/frequency/{portfolio_id}")
async def trade_frequency_chart(portfolio_id: str):
    """Serve hourly trade frequency for Chart.js bar chart."""
    chart_json = await format_trade_frequency_for_chart(portfolio_id)
    return JSONResponse(content=json.loads(chart_json))

@app.get("/api/pnl/rolling/{portfolio_id}")
async def rolling_avg_pnl_chart(portfolio_id: str, window: int = 10):
    """Serve rolling average PnL for Chart.js gauge or summary card."""
    chart_json = await format_rolling_avg_pnl_for_chart(portfolio_id, window)
    return JSONResponse(content=json.loads(chart_json))

@app.get("/api/summary/{portfolio_id}")
async def portfolio_summary(portfolio_id: str):
    """Serve portfolio performance summary."""
    summary = await get_portfolio_performance_summary(portfolio_id)
    return JSONResponse(content=summary)

# --- Socket.IO Real‑Time Emitters ---

import socketio

# Create an async Socket.IO server
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
app = socketio.ASGIApp(sio, app)

@sio.event
async def connect(sid, environ):
    logger.info(f"🔌 Dashboard connected: {sid}")

@sio.event
async def disconnect(sid):
    logger.info(f"❎ Dashboard disconnected: {sid}")

# Emit updates when new trades or portfolio states are logged
async def emit_trade_update(portfolio_id: str):
    """Push latest trade and PnL data to dashboard."""
    daily_pnl = await get_daily_pnl_trend(portfolio_id, days=7)
    rolling_avg = await get_rolling_avg_pnl(portfolio_id, window=10)
    summary = await get_portfolio_performance_summary(portfolio_id)
    payload = {
        "portfolio_id": portfolio_id,
        "daily_pnl": daily_pnl,
        "rolling_avg": rolling_avg,
        "summary": summary
    }
    await sio.emit("trade_update", payload)
    logger.info(f"📡 Emitted trade update for {portfolio_id}")

async def emit_portfolio_state_update(portfolio_id: str):
    """Push latest portfolio balance snapshot to dashboard."""
    state = await get_portfolio_state(portfolio_id)
    await sio.emit("portfolio_state_update", {"portfolio_id": portfolio_id, "state": state})
    logger.info(f"📡 Emitted portfolio state update for {portfolio_id}")
