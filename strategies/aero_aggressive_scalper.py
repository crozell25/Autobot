# strategies/aero_aggressive_scalper.py
import uuid
import logging
import time
from decimal import Decimal, ROUND_HALF_EVEN
from utils import format_by_increment

logger = logging.getLogger("AggressiveScalper")

SELL_SIZE = Decimal("0.1")
BUY_SIZE = Decimal("0.1")
# Hard Limit Constants
MAX_SELL_ORDERS = 30
MAX_BUY_ORDERS = 5
STALE_THRESHOLD_PCT = Decimal("0.015") 
ANCHOR_RESOLUTION = Decimal("0.002")

def snap_to_anchor(price: Decimal, anchor: Decimal) -> Decimal:
    if anchor <= Decimal("0"): return price
    return (price / anchor).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN) * anchor

def run_strategy(order_manager, market_data):
    try:
        portfolio_id = market_data.get("portfolio_id")
        product_id = market_data.get("product_id", "AERO-USDC")
        raw_mid = Decimal(str(market_data.get("price", "0")))
        tick_size = Decimal(str(market_data.get("tick_size", "0.00001")))
        best_ask = market_data.get("best_ask")
        if not portfolio_id or raw_mid <= 0: return

        aero_bal = Decimal(str(market_data.get("aero_bal", "0")))
        usdc_bal = Decimal(str(market_data.get("usdc_bal", "0")))
        mid_price = snap_to_anchor(raw_mid, ANCHOR_RESOLUTION)

        active_orders = [o for o in order_manager.active_orders.values() if o.get("portfolio_id") == portfolio_id and o.get("product_id") == product_id]
        pending_queue = [p for p in order_manager.execution_queue if p.get("source_pid") == portfolio_id and p.get("product_id") == product_id]

        # Hard Lock
        if (len(active_orders) + len(pending_queue)) >= (MAX_SELL_ORDERS + MAX_BUY_ORDERS + 10): return

        # Effective Balance
        queued_aero = sum(Decimal(str(p.get("size", "0"))) for p in pending_queue if p.get("side") == "SELL" and p.get("action") == "PLACE_ORDER")
        queued_usdc = sum(Decimal(str(p.get("size", "0"))) * Decimal(str(p.get("price", "0"))) for p in pending_queue if p.get("side") == "BUY" and p.get("action") == "PLACE_ORDER")
        effective_aero = max(Decimal("0"), aero_bal - queued_aero)
        effective_usdc = max(Decimal("0"), usdc_bal - queued_usdc)

        sell_orders = [o for o in active_orders if o.get("side") == "SELL"]
        buy_orders = [o for o in active_orders if o.get("side") == "BUY"]

        # Cancel logic...
        # (Same as before, keep your cancel loops but use the new hard limits)

        if len(buy_orders) + len([p for p in pending_queue if p.get("side")=="BUY"]) < MAX_BUY_ORDERS:
            target_buy = (Decimal(str(best_ask)) + tick_size) if best_ask else (mid_price * Decimal("0.9995"))
            formatted_buy = format_by_increment(target_buy, tick_size)
            if effective_usdc >= (BUY_SIZE * Decimal(formatted_buy)):
                order_manager.enqueue({
                    "action": "PLACE_ORDER", "source_pid": portfolio_id, "client_order_id": str(uuid.uuid4()),
                    "product_id": product_id, "side": "BUY", "price": str(formatted_buy), "size": str(BUY_SIZE),
                    "post_only": False, "stp_id": str(uuid.uuid4())
                })
                effective_usdc -= (BUY_SIZE * Decimal(formatted_buy))

        orders_queued = 0
        for i in range(1, 21): 
            if (len(sell_orders) + orders_queued) >= MAX_SELL_ORDERS: break
            target_sell = mid_price * (Decimal("1") + (Decimal("0.001") * Decimal(str(i))))
            formatted_sell = format_by_increment(target_sell, tick_size)
            if effective_aero >= SELL_SIZE:
                order_manager.enqueue({
                    "action": "PLACE_ORDER", "source_pid": portfolio_id, "client_order_id": str(uuid.uuid4()),
                    "product_id": product_id, "side": "SELL", "price": str(formatted_sell), "size": str(SELL_SIZE),
                    "post_only": True, "stp_id": str(uuid.uuid4())
                })
                effective_aero -= SELL_SIZE
                orders_queued += 1

    except Exception as exc:
        logger.exception("run_strategy failed: %s", exc)