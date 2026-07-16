# strategies/aero_asymmetric_grid_v2.py
import logging
import uuid
import math
import time
from decimal import Decimal, InvalidOperation
from collections import deque
from utils import format_by_increment

logger = logging.getLogger("StrategyLogic")

MIN_NOTIONAL = Decimal("1.00")
PRICE_INCREMENT = Decimal("0.00001") 
BASE_INCREMENT = Decimal("0.1")
DRY_RUN_MODE = False
MAX_GRID_BUYS = 8
MAX_GRID_SELLS = 8
# Hard limit for queue+active protection
MAX_EXCHANGE_ORDERS = 100 

def get_vwap_and_bands(order_manager, portfolio_id, price, volume):
    if not hasattr(order_manager, 'strategy_states'): order_manager.strategy_states = {}
    window_key = f"vwap_window_{portfolio_id}"
    pv_key = f"sum_pv_{portfolio_id}"
    v_key = f"sum_v_{portfolio_id}"

    if window_key not in order_manager.strategy_states:
        order_manager.strategy_states[window_key] = deque(maxlen=100)
        order_manager.strategy_states[pv_key] = Decimal("0")
        order_manager.strategy_states[v_key] = Decimal("0")
    
    window = order_manager.strategy_states[window_key]
    window.append((price, volume))
    order_manager.strategy_states[pv_key] += (price * volume)
    order_manager.strategy_states[v_key] += volume
    
    total_volume = order_manager.strategy_states[v_key]
    vwap = (order_manager.strategy_states[pv_key] / total_volume) if total_volume > 0 else price
    prices = [p for p, v in window]
    n = len(prices)
    if n == 0: std_dev = Decimal("0")
    else:
        variance = sum((p - vwap) * (p - vwap) for p in prices) / Decimal(n)
        try: std_dev = Decimal(str(math.sqrt(float(variance)))).quantize(Decimal("0.00001"))
        except Exception: std_dev = Decimal("0")
    return vwap, std_dev

def run_strategy(order_manager, market_data):
    strategy_name = "AERO_ASYMMETRIC_V2"
    product_id = market_data.get("product_id", "AERO-USDC")
    portfolio_id = market_data.get("portfolio_id")
    if not portfolio_id: return

    try:
        raw_price = market_data.get("price") or market_data.get("current_price")
        price_val = Decimal(str(raw_price))
        if price_val <= 0: return
            
        aero_bal = Decimal(str(market_data.get("aero_bal", "0")))
        usdc_bal = Decimal(str(market_data.get("usdc_bal", "0")))
        
        active_orders = [o for o in order_manager.active_orders.values() if o.get("product_id") == product_id and o.get("portfolio_id") == portfolio_id]
        pending_queue = [p for p in order_manager.execution_queue if p.get("source_pid") == portfolio_id and p.get("product_id") == product_id]

        # Hard Lock
        if (len(active_orders) + len(pending_queue)) >= MAX_EXCHANGE_ORDERS: return

        # Effective Balance Calculation
        pending_buy_orders = [p for p in pending_queue if p.get("side") == "BUY" and p.get("action") == "PLACE_ORDER"]
        pending_sell_orders = [p for p in pending_queue if p.get("side") == "SELL" and p.get("action") == "PLACE_ORDER"]

        current_buys = sum(1 for o in active_orders if o.get("side") == "BUY") + len(pending_buy_orders)
        current_sells = sum(1 for o in active_orders if o.get("side") == "SELL") + len(pending_sell_orders)
        
        queued_aero = sum(Decimal(str(p.get("size", "0"))) for p in pending_sell_orders)
        queued_usdc = sum(Decimal(str(p.get("size", "0"))) * Decimal(str(p.get("price", "0"))) for p in pending_buy_orders)

        effective_aero = max(Decimal("0"), aero_bal - queued_aero)
        effective_usdc = max(Decimal("0"), usdc_bal - queued_usdc)

        vwap, std_dev = get_vwap_and_bands(order_manager, portfolio_id, price_val, Decimal("1"))

        working_aero = effective_aero * Decimal("0.8")
        working_usdc = effective_usdc * Decimal("0.8")

        d_vwap = Decimal(str(vwap))
        actual_step = max(Decimal(str(std_dev)), d_vwap * Decimal("0.002"))

        buy_depth = min(int(working_usdc // MIN_NOTIONAL), 4)
        sell_depth = min(int((working_aero * price_val) // MIN_NOTIONAL), 4)
        
        grid_levels = []
        for i in range(1, buy_depth + 1): grid_levels.append({"side": "BUY", "offset": Decimal(f"-{i}.0") * actual_step})
        for i in range(1, sell_depth + 1): grid_levels.append({"side": "SELL", "offset": Decimal(f"{i}.0") * actual_step})

        maker_spread = price_val * Decimal("0.0001")

        for idx, level in enumerate(grid_levels):
            if level["side"] == "BUY" and current_buys >= MAX_GRID_BUYS: continue
            if level["side"] == "SELL" and current_sells >= MAX_GRID_SELLS: continue

            target_price = d_vwap + level["offset"]
            if level["side"] == "BUY":
                target_price -= maker_spread
                formatted_size = format_by_increment((working_usdc / buy_depth) / target_price, BASE_INCREMENT)
            else:
                target_price += maker_spread
                formatted_size = format_by_increment((working_aero / sell_depth), BASE_INCREMENT)
            formatted_price = format_by_increment(target_price, PRICE_INCREMENT)

            already_active = any(str(format_by_increment(Decimal(str(o.get("price", "0"))), PRICE_INCREMENT)) == formatted_price and o.get("side") == level["side"] for o in active_orders)
            already_pending = any(str(format_by_increment(Decimal(str(p.get("price", "0"))), PRICE_INCREMENT)) == formatted_price and p.get("side") == level["side"] for p in pending_queue)
            if already_active or already_pending: continue

            payload = {
                "action": "PLACE_ORDER", "source_pid": portfolio_id, "client_order_id": str(uuid.uuid4()),
                "product_id": product_id, "side": level["side"], "price": str(formatted_price), "size": str(formatted_size),
                "post_only": True, "strategy": strategy_name, "stp_id": str(uuid.uuid4())
            }
            
            if level["side"] == "SELL" and effective_aero >= Decimal(str(formatted_size)):
                order_manager.enqueue(payload)
                effective_aero -= Decimal(str(formatted_size))
                current_sells += 1
            elif level["side"] == "BUY" and effective_usdc >= (Decimal(str(formatted_size)) * Decimal(formatted_price)):
                order_manager.enqueue(payload)
                effective_usdc -= (Decimal(str(formatted_size)) * Decimal(formatted_price))
                current_buys += 1
    except Exception as e:
        logger.error(f"[{portfolio_id[:8]}] Strategy execution fault: {e}")