import uuid
import logging
from decimal import Decimal
from utils import format_by_increment

def run_strategy(order_manager, market_data):
    """Fulfills required strategy template standard interface rules."""
    try:
        current_price = market_data["current_price"]
        product_id = market_data["product_id"] # Internal target is 'AERO-USDC'
        
        # Simple execution condition to illustrate clean functionality
        if len(order_manager.active_orders) == 0 and len(order_manager.execution_queue) == 0:
            price_step = Decimal("0.0001")
            size_step = Decimal("0.1")
            
            # Format numbers safely matching asset criteria
            target_buy_price = format_by_increment(current_price * Decimal("0.99"), price_step)
            target_size = format_by_increment(Decimal("10.0"), size_step)
            
            mock_order = {
                "client_order_id": str(uuid.uuid4()),
                "product_id": product_id,
                "side": "BUY",
                "price": target_buy_price,
                "size": target_size
            }
            
            logging.info(f"Strategy Strategy Module logic triggered limit queue entry for {product_id}")
            order_manager.execution_queue.append(mock_order)
            
    except Exception as e:
        logging.error(f"Strategy execution logic exception encountered: {e}")