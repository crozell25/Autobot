# order_rebalancer.py
import time
import logging
import uuid
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Dict, Any, List

logger = logging.getLogger("order_rebalancer")

REBALANCE_REGISTRY: Dict[str, Dict[str, Any]] = {}

def _to_decimal(v):
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")

def register_rebalance(portfolio_id: str, max_distance_pct: float = 0.10, keep_closest_n: int = 8, replace_levels: int = 4, cooldown_seconds: int = 300, spacing_pct: float = 0.01) -> str:
    tid = str(uuid.uuid4())
    REBALANCE_REGISTRY[tid] = {
        "id": tid,
        "portfolio_id": portfolio_id,
        "max_distance_pct": Decimal(str(max_distance_pct)),
        "keep_closest_n": int(keep_closest_n),
        "replace_levels": int(replace_levels),
        "cooldown_seconds": cooldown_seconds,
        "spacing_pct": Decimal(str(spacing_pct)),
        "last_rebalance_time": 0,
        "active": True
    }
    logger.info("Registered rebalance %s for %s (max_dist=%s keep=%d replace=%d cooldown=%ds spacing=%s)", 
                tid, portfolio_id, max_distance_pct, keep_closest_n, replace_levels, cooldown_seconds, spacing_pct)
    return tid

def cancel_rebalance(trigger_id: str) -> bool:
    return REBALANCE_REGISTRY.pop(trigger_id, None) is not None

def list_rebalances(portfolio_id: str = None) -> List[Dict[str, Any]]:
    if portfolio_id:
        return [r for r in REBALANCE_REGISTRY.values() if r["portfolio_id"] == portfolio_id]
    return list(REBALANCE_REGISTRY.values())

def evaluate_rebalances(order_manager, portfolio_id: str, mid_price: Decimal, product_id: str = "AERO-USDC"):
    current_time = time.time()
    
    policies = [p for p in REBALANCE_REGISTRY.values() if p["portfolio_id"] == portfolio_id and p["active"]]
    if not policies:
        return

    active_orders = list(getattr(order_manager, "active_orders", {}).values())
    portfolio_orders = []
    
    for o in active_orders:
        try:
            if o.get("portfolio_id") != portfolio_id: continue
            if product_id and o.get("product_id") and product_id != o.get("product_id"): continue
            
            price = _to_decimal(o.get("price") or "0")
            side = (o.get("side") or "").upper()
            cid = o.get("client_order_id")
            exchange_id = o.get("exchange_id")
            
            if not exchange_id: continue
                
            portfolio_orders.append({"order": o, "price": price, "side": side, "client_order_id": cid, "exchange_id": exchange_id})
        except Exception:
            continue

    if not portfolio_orders:
        return

    for policy in policies:
        if current_time - policy.get("last_rebalance_time", 0) < policy.get("cooldown_seconds", 300):
            continue

        max_dist = policy["max_distance_pct"]
        keep_n = policy["keep_closest_n"]
        replace_levels = policy["replace_levels"]

        for p in portfolio_orders:
            try:
                if mid_price == 0: p["dist_frac"] = Decimal("1")
                else: p["dist_frac"] = (abs(p["price"] - mid_price) / mid_price)
            except Exception:
                p["dist_frac"] = Decimal("1")

        sorted_by_dist = sorted(portfolio_orders, key=lambda x: x["dist_frac"], reverse=True)
        to_cancel = [p for p in sorted_by_dist if p["dist_frac"] > max_dist]
        
        keep_candidates = sorted(portfolio_orders, key=lambda x: x["dist_frac"])[:keep_n]
        keep_client_ids = {k["client_order_id"] for k in keep_candidates if k["client_order_id"]}
        final_cancel = [p for p in to_cancel if p["client_order_id"] not in keep_client_ids]

        pending_ids = {q.get("client_order_id") for q in getattr(order_manager, "execution_queue", []) if isinstance(q, dict)}
        pending_ids.update({q.get("client_order_id") for q in getattr(order_manager, "priority_queue", []) if isinstance(q, dict)})

        cancelled_count = 0
        last_cancelled_size = "0.1"
        cancelled_sides = set() # Track which sides were actually cancelled

        for item in final_cancel:
            cid = item.get("client_order_id")
            exchange_id = item.get("exchange_id")
            side = item.get("side")
            
            if not cid or not exchange_id: continue
                
            cancel_payload = {
                "action": "CANCEL_ORDER",
                "source_pid": portfolio_id,
                "client_order_id": cid,
                "exchange_id": exchange_id, 
                "product_id": product_id
            }
            if cid in pending_ids: continue
                
            try:
                order_manager.enqueue(cancel_payload)
                last_cancelled_size = str(item["order"].get("size", "0.1"))
                cancelled_sides.add(side)
                cancelled_count += 1
            except Exception as e:
                logger.warning("Failed to enqueue cancel for %s: %s", cid, e)

        if cancelled_count > 0:
            policy["last_rebalance_time"] = current_time
            
            spacing_ratio = policy.get("spacing_pct", Decimal("0.01"))
            inc = mid_price * spacing_ratio
            replacements = []
            quantize_target = Decimal("0.00001")
            
            for i in range(1, replace_levels + 1):
                # Only generate replacements for the sides that experienced a cancellation
                if "BUY" in cancelled_sides:
                    raw_buy = mid_price - (inc * i)
                    buy_price = raw_buy.quantize(quantize_target, rounding=ROUND_HALF_EVEN)
                    if buy_price > 0:
                        replacements.append({"side": "BUY", "price": buy_price})
                
                if "SELL" in cancelled_sides:
                    raw_sell = mid_price + (inc * i)
                    sell_price = raw_sell.quantize(quantize_target, rounding=ROUND_HALF_EVEN)
                    replacements.append({"side": "SELL", "price": sell_price})

            for r in replacements:
                payload = {
                    "action": "PLACE_ORDER",
                    "source_pid": portfolio_id,
                    "client_order_id": f"repl-{uuid.uuid4()}",
                    "product_id": product_id,
                    "side": r["side"],
                    "price": str(r["price"]),
                    "size": last_cancelled_size, 
                    "post_only": True,
                    "strategy": "REBALANCER",
                    "stp_id": str(uuid.uuid4())
                }
                try: order_manager.enqueue(payload)
                except Exception: pass

            logger.info("Rebalance %s for %s: cancelled=%d replacements=%d (Cooldown engaged). Sides rebuilt: %s", 
                        policy["id"], portfolio_id[:8], cancelled_count, len(replacements), list(cancelled_sides))