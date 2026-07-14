# strategies/aero_simple_ladder.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import logging
import uuid
from decimal import Decimal, ROUND_HALF_EVEN
from utils import format_by_increment

logger = logging.getLogger("StrategyLogic")

PORTFOLIO_CONFIG = {
    "4f8c51ed-720a-47a4-b1a7-d1a7ffd54a85": Decimal("0.1"), 
    "ec4c59b8-3960-4bad-b474-46cbbf7b2dba": Decimal("1.0"),                
}

PRICE_INCREMENT = Decimal("0.00006")
LEVELS = 50
STP_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
MAX_QUEUE_PER_TICK = 10 

# --- Grid Snap Resolution ---
ANCHOR_RESOLUTION = Decimal("0.002")

def snap_to_anchor(price: Decimal, anchor: Decimal) -> Decimal:
    """Rounds the raw market price to the nearest stable grid anchor."""
    if anchor <= Decimal("0"): return price
    return (price / anchor).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN) * anchor

def run_strategy(order_manager, market_data):
    portfolio_id = market_data.get("portfolio_id")
    
    if len(order_manager.execution_queue) > 100:
        logger.error(f"🚨 CRITICAL: Execution queue full ({len(order_manager.execution_queue)}). Strategy paused.")
        return

    pending_for_pid = sum(1 for t in order_manager.execution_queue if t.get("source_pid") == portfolio_id or t.get("portfolio_id") == portfolio_id)
    if pending_for_pid > 10:
        return

    strategy_name = "SIMPLE_LADDER"
    product_id = market_data.get("product_id", "AERO-USDC")
    if not portfolio_id: return
    order_size = PORTFOLIO_CONFIG.get(portfolio_id, Decimal("0.1"))

    if not hasattr(order_manager, 'strategy_states'):
        order_manager.strategy_states = {}
    state_key = f"last_ladder_check_{portfolio_id}"
    current_time = time.time()
    if current_time - order_manager.strategy_states.get(state_key, 0) < 30:
        return
        
    try:
        raw_price = market_data.get("price") or market_data.get("current_price")
        raw_mid = Decimal(str(raw_price))
        if raw_mid <= 0: return
        
        # --- GRID SNAP (Price Anchoring) ---
        mid_price = snap_to_anchor(raw_mid, ANCHOR_RESOLUTION)
        
        aero_bal = Decimal(str(market_data.get("aero_bal", "0")))
        usdc_bal = Decimal(str(market_data.get("usdc_bal", "0")))
    except Exception: return

    active_orders = list(order_manager.active_orders.values())
    pending_queue = list(order_manager.execution_queue)

    safety_offset = PRICE_INCREMENT * 20 # Increased safety offset
    highest_buy_price = mid_price - safety_offset
    lowest_sell_price = mid_price + safety_offset
    
    target_levels = []
    target_formatted_prices = set() 
    
    for i in range(LEVELS):
        offset = Decimal(str(i)) * PRICE_INCREMENT
        buy_p = highest_buy_price - offset
        sell_p = lowest_sell_price + offset
        
        target_levels.append({"side": "BUY", "price": buy_p})
        target_levels.append({"side": "SELL", "price": sell_p})
        
        target_formatted_prices.add(str(format_by_increment(buy_p, Decimal("0.00001"))))
        target_formatted_prices.add(str(format_by_increment(sell_p, Decimal("0.00001"))))

    cancels_queued = 0
    for order in active_orders:
        if cancels_queued >= 50: break # Guard against queue flood
        if order.get("portfolio_id") != portfolio_id: continue
            
        oprice_str = str(format_by_increment(Decimal(str(order.get("price", "0"))), Decimal("0.00001")))
        
        if oprice_str not in target_formatted_prices:
            cid = order.get("client_order_id")
            eid = order.get("exchange_id")
            
            if cid and eid and not any(p.get("client_order_id") == cid for p in pending_queue):
                order_manager.enqueue({
                    "action": "CANCEL_ORDER",
                    "client_order_id": cid,
                    "exchange_id": eid,
                    "source_pid": portfolio_id
                })
                cancels_queued += 1

    if cancels_queued > 0:
        logger.info(f"[{strategy_name}] Pruning ladder: Queued {cancels_queued} stale orders for cancellation on {portfolio_id[:8]}.")

    orders_queued = 0
    for level in target_levels:
        if orders_queued >= MAX_QUEUE_PER_TICK: break
            
        formatted_price = format_by_increment(level["price"], Decimal("0.00001"))
        client_order_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{portfolio_id}_{strategy_name}_{level['side']}_{formatted_price}"))
        
        if any(o.get("client_order_id") == client_order_id for o in active_orders) or \
           any(p.get("client_order_id") == client_order_id for p in pending_queue):
            continue

        payload = {
            "action": "PLACE_ORDER",
            "source_pid": portfolio_id,
            "client_order_id": client_order_id,
            "product_id": product_id,
            "side": level["side"],
            "price": str(formatted_price),
            "size": str(order_size),
            "post_only": True,
            "strategy": strategy_name,
            "stp_id": str(uuid.uuid5(STP_NAMESPACE, f"{portfolio_id}_{strategy_name}"))
        }
        
        if level["side"] == "SELL" and aero_bal > order_size:
            order_manager.enqueue(payload)
            orders_queued += 1
            aero_bal -= order_size
        elif level["side"] == "BUY" and usdc_bal > (order_size * Decimal(formatted_price)):
            order_manager.enqueue(payload)
            orders_queued += 1
            usdc_bal -= (order_size * Decimal(formatted_price))

    order_manager.strategy_states[state_key] = current_time
    if orders_queued > 0:
        logger.info(f"[{strategy_name}] Ladder expanded: Queued {orders_queued} new orders for {portfolio_id[:8]} (Size: {order_size}).")