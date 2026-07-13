# strategies/aero_asymmetric_grid_v2.py
import logging
import uuid
import math
from decimal import Decimal, InvalidOperation
from collections import deque

from utils import format_by_increment
logger = logging.getLogger("StrategyLogic")

MIN_NOTIONAL = Decimal("1.00")
PRICE_INCREMENT = Decimal("0.00001") 
BASE_INCREMENT = Decimal("0.1")
STP_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
DRY_RUN_MODE = False

def get_vwap_and_bands(order_manager, portfolio_id, price, volume):
    if not hasattr(order_manager, 'strategy_states'):
        order_manager.strategy_states = {}

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
    if n == 0:
        std_dev = Decimal("0")
    else:
        variance = sum((p - vwap) * (p - vwap) for p in prices) / Decimal(n)
        if variance < 0 and abs(variance) < Decimal("1e-18"): variance = Decimal("0")
        try:
            std_dev = Decimal(str(math.sqrt(float(variance))))
            std_dev = std_dev.quantize(Decimal("0.00001"))
        except Exception:
            std_dev = Decimal("0")
    
    return vwap, std_dev

def run_strategy(order_manager, market_data):
    strategy_name = "AERO_ASYMMETRIC_V2"
    product_id = market_data.get("product_id", "AERO-USDC")
    portfolio_id = market_data.get("portfolio_id")

    if not portfolio_id: return

    try:
        raw_price = market_data.get("price") or market_data.get("current_price")
        try: price_val = raw_price if isinstance(raw_price, Decimal) else Decimal(str(raw_price))
        except Exception: return

        if price_val <= 0: return
            
        raw_vol = market_data.get("volume", "1")
        try: volume_val = raw_vol if isinstance(raw_vol, Decimal) else Decimal(str(raw_vol))
        except Exception: volume_val = Decimal("1")
        
        aero_bal = Decimal(str(market_data.get("aero_bal", "0")))
        usdc_bal = Decimal(str(market_data.get("usdc_bal", "0")))
        
        active_orders = [o for o in order_manager.active_orders.values() if o.get("product_id") == product_id and o.get("portfolio_id") == portfolio_id]
        pending_ids = {payload.get("client_order_id") for payload in order_manager.execution_queue if payload.get("action") == "PLACE_ORDER" and payload.get("source_pid") == portfolio_id}

        if len(active_orders) >= 8: return

        vwap, std_dev = get_vwap_and_bands(order_manager, portfolio_id, price_val, volume_val)

        total_value = (aero_bal * price_val) + usdc_bal
        if total_value < Decimal("8.00"): return

        margin_factor = Decimal("0.80")
        working_aero = aero_bal * margin_factor
        working_usdc = usdc_bal * margin_factor

        d_vwap = Decimal(str(vwap))
        min_offset_step = d_vwap * Decimal("0.002") 
        actual_step = max(Decimal(str(std_dev)), min_offset_step)

        buy_depth = min(int(working_usdc // MIN_NOTIONAL), 4)
        sell_depth = min(int((working_aero * price_val) // MIN_NOTIONAL), 4)
        
        grid_levels = []
        for i in range(1, buy_depth + 1): grid_levels.append({"side": "BUY", "offset": Decimal(f"-{i}.0") * actual_step})
        for i in range(1, sell_depth + 1): grid_levels.append({"side": "SELL", "offset": Decimal(f"{i}.0") * actual_step})

        maker_spread = price_val * Decimal("0.0001")

        for idx, level in enumerate(grid_levels):
            target_price = d_vwap + level["offset"]
            
            if level["side"] == "BUY":
                target_price -= maker_spread
                notional = (working_usdc / Decimal(str(max(buy_depth, 1))))
                formatted_size = format_by_increment(notional / target_price, BASE_INCREMENT)
            else:
                target_price += maker_spread
                capital_per_level = (working_aero / Decimal(str(max(sell_depth, 1))))
                formatted_size = format_by_increment(capital_per_level, BASE_INCREMENT)
                
            formatted_price = format_by_increment(target_price, PRICE_INCREMENT)

            try:
                Decimal(formatted_price)
                Decimal(formatted_size)
            except (InvalidOperation, ValueError):
                continue

            unique_seed = f"{portfolio_id}_{strategy_name}_{level['side']}_{formatted_price}"
            client_order_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_seed))
            
            if client_order_id in order_manager.active_orders or client_order_id in pending_ids:
                continue

            stp_id = str(uuid.uuid5(STP_NAMESPACE, f"{portfolio_id}_{strategy_name}_{idx}"))

            payload = {
                "action": "PLACE_ORDER",
                "source_pid": portfolio_id,  
                "client_order_id": client_order_id,
                "product_id": product_id,
                "side": level["side"],
                "price": str(formatted_price),
                "size": str(formatted_size),
                "post_only": True,
                "strategy": strategy_name,
                "stp_id": stp_id
            }
            
            if DRY_RUN_MODE:
                logger.info("[Dry Run] Would place order: %s", payload)
            else:
                order_manager.enqueue(payload)
        
    except Exception as e:
        logger.error(f"[{portfolio_id[:8]}] Strategy execution fault: {e}")