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