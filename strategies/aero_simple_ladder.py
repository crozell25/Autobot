# strategies/aero_simple_ladder.py
import sys
import os
import time
import logging
import uuid
from decimal import Decimal, ROUND_HALF_EVEN
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import format_by_increment

logger = logging.getLogger("StrategyLogic")

# Portfolio Config
PORTFOLIO_CONFIG = {
    "4f8c51ed-720a-47a4-b1a7-d1a7ffd54a85": Decimal("0.1"), 
    "ec4c59b8-3960-4bad-b474-46cbbf7b2dba": Decimal("1.0"),                
}

# Constraints
PRICE_INCREMENT = Decimal("0.00006")
LEVELS = 50
MAX_BUYS = LEVELS + 15
MAX_SELLS = LEVELS + 15
MAX_QUEUE_PER_TICK = 150
ANCHOR_RESOLUTION = Decimal("0.002")

def snap_to_anchor(price: Decimal, anchor: Decimal) -> Decimal:
    if anchor <= Decimal("0"): return price
    return (price / anchor).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN) * anchor

def run_strategy(order_manager, market_data):
    portfolio_id = market_data.get("portfolio_id")
    if not portfolio_id: return
    
    # Global Queue Pressure Relief
    if len(order_manager.execution_queue) > 200:
        return

    # Portfolio-Specific Queue Pressure Relief
    pending_for_pid = sum(1 for t in order_manager.execution_queue if t.get("source_pid") == portfolio_id)
    if pending_for_pid > 20:
        return

    strategy_name = "SIMPLE_LADDER"
    product_id = market_data.get("product_id", "AERO-USDC")
    order_size = PORTFOLIO_CONFIG.get(portfolio_id, Decimal("0.1"))

    # Rate Limiting
    if not hasattr(order_manager, 'strategy_states'): order_manager.strategy_states = {}
    state_key = f"last_ladder_check_{portfolio_id}"
    current_time = time.time()
    if current_time - order_manager.strategy_states.get(state_key, 0) < 30:
        return
        
    try:
        raw_price = market_data.get("price") or market_data.get("current_price")
        raw_mid = Decimal(str(raw_price))
        if raw_mid <= 0: return
        mid_price = snap_to_anchor(raw_mid, ANCHOR_RESOLUTION)
        aero_bal = Decimal(str(market_data.get("aero_bal", "0")))
        usdc_bal = Decimal(str(market_data.get("usdc_bal", "0")))
    except Exception: return

    active_orders = list(order_manager.active_orders.values())
    pending_queue = list(order_manager.execution_queue)

    # STRICT STATE ACCOUNTING
    active_buys = sum(1 for o in active_orders if o.get("side") == "BUY" and o.get("portfolio_id") == portfolio_id)
    active_sells = sum(1 for o in active_orders if o.get("side") == "SELL" and o.get("portfolio_id") == portfolio_id)
    
    pending_buy_orders = [p for p in pending_queue if p.get("source_pid") == portfolio_id and p.get("side") == "BUY" and p.get("action") == "PLACE_ORDER"]
    pending_sell_orders = [p for p in pending_queue if p.get("source_pid") == portfolio_id and p.get("side") == "SELL" and p.get("action") == "PLACE_ORDER"]

    current_buys = active_buys + len(pending_buy_orders)
    current_sells = active_sells + len(pending_sell_orders)

    queued_aero = sum(Decimal(str(p.get("size", "0"))) for p in pending_sell_orders)
    queued_usdc = sum(Decimal(str(p.get("size", "0"))) * Decimal(str(p.get("price", "0"))) for p in pending_buy_orders)

    effective_aero = max(Decimal("0"), aero_bal - queued_aero)
    effective_usdc = max(Decimal("0"), usdc_bal - queued_usdc)

    safety_offset = PRICE_INCREMENT * 20
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
        if cancels_queued >= 150: break
        if order.get("portfolio_id") != portfolio_id: continue
        oprice_str = str(format_by_increment(Decimal(str(order.get("price", "0"))), Decimal("0.00001")))
        if oprice_str not in target_formatted_prices:
            cid = order.get("client_order_id")
            eid = order.get("exchange_id")
            if cid and eid and not any(p.get("client_order_id") == cid for p in pending_queue):
                order_manager.enqueue({"action": "CANCEL_ORDER", "client_order_id": cid, "exchange_id": eid, "source_pid": portfolio_id})
                cancels_queued += 1

    orders_queued = 0
    for level in target_levels:
        if orders_queued >= MAX_QUEUE_PER_TICK: break
        
        # Hard Lock Check
        if level["side"] == "BUY" and current_buys >= MAX_BUYS: continue
        if level["side"] == "SELL" and current_sells >= MAX_SELLS: continue
            
        formatted_price = format_by_increment(level["price"], Decimal("0.00001"))
        
        already_active = any(
            str(format_by_increment(Decimal(str(o.get("price", "0"))), Decimal("0.00001"))) == formatted_price 
            and o.get("side") == level["side"]
            for o in active_orders if o.get("portfolio_id") == portfolio_id
        )
        already_pending = any(
            str(format_by_increment(Decimal(str(p.get("price", "0"))), Decimal("0.00001"))) == formatted_price 
            and p.get("side") == level["side"]
            for p in pending_queue if p.get("source_pid") == portfolio_id
        )
        
        if already_active or already_pending: continue

        # Use uuid4 for absolute uniqueness
        client_order_id = str(uuid.uuid4())
        stp_id = str(uuid.uuid4())

        payload = {
            "action": "PLACE_ORDER", "source_pid": portfolio_id, "client_order_id": client_order_id,
            "product_id": product_id, "side": level["side"], "price": str(formatted_price),
            "size": str(order_size), "post_only": True, "strategy": strategy_name,
            "stp_id": stp_id
        }
        
        if level["side"] == "SELL" and effective_aero > order_size:
            order_manager.enqueue(payload)
            orders_queued += 1
            effective_aero -= order_size
            current_sells += 1
        elif level["side"] == "BUY" and effective_usdc > (order_size * Decimal(formatted_price)):
            order_manager.enqueue(payload)
            orders_queued += 1
            effective_usdc -= (order_size * Decimal(formatted_price))
            current_buys += 1

    order_manager.strategy_states[state_key] = current_time
    if orders_queued > 0:
        logger.info(f"[{strategy_name}] Ladder expanded: Queued {orders_queued} new orders for {portfolio_id[:8]}.")