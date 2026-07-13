import logging
import uuid
import time
from decimal import Decimal
from utils import format_by_increment

logger = logging.getLogger(__name__)

# =====================================================================
# CONFIGURATION BOUNDS
# =====================================================================
MIN_NOTIONAL = Decimal("1.00")
PRICE_INCREMENT = Decimal("0.00001") 
BASE_INCREMENT = Decimal("0.1")
STP_NAMESPACE = uuid.UUID("7ca7b810-9dad-11d1-80b4-00c04fd430c9")

# Ladder Settings
MAX_SELL_ORDERS = 50
LADDER_INTERVAL = Decimal("0.00001") 
MAINTENANCE_INTERVAL_SECONDS = 30 
PROFIT_FEE_BUFFER = Decimal("1.0025") 

# Capital Allocation
BUYBACK_RATIO = Decimal("0.80") 

# =====================================================================
# IN-MEMORY STATE 
# =====================================================================
RUNTIME_STATE = {}

def get_state(pid, initial_aero_bal):
    if pid not in RUNTIME_STATE:
        RUNTIME_STATE[pid] = {
            "last_maintenance": 0,
            "local_high": Decimal("0"),
            "local_low": Decimal("999999"),
            "cost_basis": Decimal("0.50"), # <-- UPDATE TO YOUR TRUE AVERAGE ENTRY
            "tracked_balance": initial_aero_bal 
        }
    return RUNTIME_STATE[pid]

def run_strategy(order_manager, market_data):
    """
    Dynamic 50-Node Market Maker & Trailing Bounce Buyer.
    """
    strategy_name = "AERO_RESERVE_LADDER"
    portfolio_id = market_data.get("portfolio_id")
    product_id = market_data.get("product_id", "AERO-USDC")
    
    # 1. HARDCODED PID CHECK: Only run for the 'Aero Res' portfolio
    if not portfolio_id or not portfolio_id.startswith("4b5556b8"):
        return

    # Pre-define to ensure NameError never happens if try block fails early
    active_sells = {}
    pending_ids = set()

    try:
        current_price = Decimal(str(market_data.get("current_price", "0")))
        if current_price <= 0:
            return

        aero_bal = Decimal(str(market_data.get("aero_bal", "0")))
        usdc_bal = Decimal(str(market_data.get("usdc_bal", "0")))
        
        state = get_state(portfolio_id, aero_bal)
        current_time = time.time()

        # =====================================================================
        # BULLETPROOF ORDER EXTRACTION (Prevents NameError/AttributeError)
        # =====================================================================
        raw_active = {}
        if hasattr(order_manager, 'active_orders'):
            if isinstance(order_manager.active_orders, dict):
                raw_active = order_manager.active_orders.values()
            elif isinstance(order_manager.active_orders, (list, set)):
                raw_active = order_manager.active_orders
        
        active_orders = [
            o for o in raw_active 
            if isinstance(o, dict) and o.get("product_id") == product_id and (o.get("pid") == portfolio_id or o.get("source_pid") == portfolio_id)
        ]
        
        active_sells = {Decimal(str(o.get("price"))): o.get("client_oid", o.get("client_order_id")) for o in active_orders if o.get("side") == "SELL"}
        
        if hasattr(order_manager, 'execution_queue'):
            pending_ids = {payload.get("client_order_id") for payload in order_manager.execution_queue if payload.get("action") in ["PLACE_ORDER", "CANCEL_ORDER"]}

        # =====================================================================
        # PHASE 1: FALLING KNIFE / BOUNCE DETECTOR & ON-THE-FLY COST BASIS
        # =====================================================================
        if current_price > state["local_high"]:
            state["local_high"] = current_price
            state["local_low"] = current_price 
        elif current_price < state["local_low"]:
            state["local_low"] = current_price

        drop_threshold = state["local_high"] * Decimal("0.98")
        bounce_trigger = state["local_low"] * Decimal("1.005")

        if current_price <= drop_threshold and current_price >= bounce_trigger and usdc_bal > MIN_NOTIONAL:
            deployable_usdc = usdc_bal * BUYBACK_RATIO
            buy_price = format_by_increment(current_price, PRICE_INCREMENT)
            buy_size = format_by_increment(deployable_usdc / Decimal(buy_price), BASE_INCREMENT)

            if Decimal(str(buy_size)) * Decimal(str(buy_price)) >= MIN_NOTIONAL:
                buy_client_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{portfolio_id}_BOUNCE_BUY_{current_time}"))
                
                # --- DYNAMIC COST BASIS CALCULATION (Bulletproofed) ---
                # Force everything to Decimal right at the moment of calculation
                tracked_bal = Decimal(str(state["tracked_balance"]))
                c_basis = Decimal(str(state["cost_basis"]))
                b_size = Decimal(str(buy_size))
                b_price = Decimal(str(buy_price))

                old_value = tracked_bal * c_basis
                new_value = b_size * b_price
                new_total_balance = tracked_bal + b_size
                
                if new_total_balance > Decimal("0"):
                    state["cost_basis"] = (old_value + new_value) / new_total_balance
                
                state["tracked_balance"] = new_total_balance 
                # ------------------------------------------------------
                
                payload = {
                    "action": "PLACE_ORDER",
                    "source_pid": portfolio_id,
                    "client_order_id": buy_client_id,
                    "product_id": product_id,
                    "side": "BUY",
                    "price": str(buy_price),
                    "size": str(buy_size),
                    "post_only": False, 
                    "self_trade_prevention_id": str(uuid.uuid5(STP_NAMESPACE, f"{portfolio_id}_BUY"))
                }
                order_manager.execution_queue.append(payload)
                logger.info(f"🔪 BOUNCE CAUGHT! Dropping {deployable_usdc:.2f} USDC on AERO at {buy_price}")
                
                state["local_high"] = current_price
                state["local_low"] = current_price

        # =====================================================================
        # PHASE 2: 50-NODE LADDER MAINTENANCE (30-Second Throttle)
        # =====================================================================
        if current_time - state["last_maintenance"] >= MAINTENANCE_INTERVAL_SECONDS:
            state["last_maintenance"] = current_time
            
            min_prof_price = state["cost_basis"] * PROFIT_FEE_BUFFER
            ladder_start_price = max(current_price + PRICE_INCREMENT, min_prof_price)
            
            target_prices = set()
            for i in range(MAX_SELL_ORDERS):
                p = format_by_increment(ladder_start_price + (Decimal(i) * LADDER_INTERVAL), PRICE_INCREMENT)
                target_prices.add(Decimal(str(p)))

            for active_price, oid in active_sells.items():
                if active_price not in target_prices and oid not in pending_ids:
                    order_manager.execution_queue.append({
                        "action": "CANCEL_ORDER",
                        "source_pid": portfolio_id,
                        "client_order_id": oid
                    })

            available_nodes = MAX_SELL_ORDERS - len(active_sells)
            if available_nodes > 0 and aero_bal > 0:
                raw_size = (aero_bal * Decimal("0.98")) / Decimal(MAX_SELL_ORDERS)
                size_per_node = format_by_increment(raw_size, BASE_INCREMENT)
                
                for target_price in target_prices:
                    # FIX: Explicitly cast both variables to string then Decimal to prevent sequence TypeError
                    notional_value = Decimal(str(size_per_node)) * Decimal(str(target_price))
                    
                    if target_price not in active_sells and notional_value >= MIN_NOTIONAL:
                        sell_client_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{portfolio_id}_SELL_{target_price}"))
                        
                        if sell_client_id not in pending_ids:
                            order_manager.execution_queue.append({
                                "action": "PLACE_ORDER",
                                "source_pid": portfolio_id,
                                "client_order_id": sell_client_id,
                                "product_id": product_id,
                                "side": "SELL",
                                "price": str(target_price),
                                "size": str(size_per_node),
                                "post_only": True,
                                "self_trade_prevention_id": str(uuid.uuid5(STP_NAMESPACE, f"{portfolio_id}_SELL"))
                            })

        # =====================================================================
        # AUTOMATED PROFIT TAKING
        # =====================================================================
        PROFIT_THRESHOLD = Decimal("0.50") # Move to Vault if USDC balance > $0.50
        
        if usdc_bal > PROFIT_THRESHOLD:
            sweep_amount = usdc_bal - Decimal("0.05")
            
            transfer_payload = {
                "action": "TRANSFER_PROFIT",
                "source_pid": portfolio_id,
                "amount": str(sweep_amount)
            }
            order_manager.execution_queue.append(transfer_payload)
            logger.info(f"💰 PROFIT TRIGGER: Moving {sweep_amount} USDC to Vault.")

    except Exception as e:
        logger.error(f"[{portfolio_id[:8]}] Dynamic Ladder fault: {e}", exc_info=True)