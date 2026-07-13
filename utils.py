# utils.py
import os
import time
import logging
import asyncio
import requests
import json
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional

try:
    from coinbase.rest import RESTClient  # type: ignore
except Exception:
    RESTClient = None  # pragma: no cover

logger = logging.getLogger("utils")

PRODUCT_ID_ALLOWLIST = ["AERO-USDC", "AERO-USD"]
DEFAULT_BACKOFF_SECONDS = 2
MAX_RETRIES = 3
TIME_OFFSET = 0.0

async def calculate_clock_drift():
    global TIME_OFFSET
    try:
        logger.info("Synchronizing system clocks with Coinbase servers...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json"
        }
        response = await asyncio.to_thread(
            requests.get,
            "https://api.coinbase.com/api/v3/brokerage/time", 
            headers=headers, 
            timeout=5
        )
        response.raise_for_status()
        json_resp = response.json()
        
        server_time_raw = json_resp.get("epochSeconds") or json_resp.get("data", {}).get("epochSeconds")
        if not server_time_raw:
            raise ValueError(f"Unexpected time payload structure: {json_resp}")
            
        server_epoch = float(server_time_raw)
        local_epoch = time.time()
        TIME_OFFSET = server_epoch - local_epoch
        
        if abs(TIME_OFFSET) > 2.0:
            logger.critical(f"⚠️ HIGH CLOCK DRIFT DETECTED: {TIME_OFFSET:.4f}s.")
            logger.critical("⚠️ The Coinbase SDK relies on system time for signatures. PLEASE SYNC YOUR WINDOWS CLOCK!")
        else:
            logger.info(f"✅ Time Sync Active. Clock drift offset: {TIME_OFFSET:.4f} seconds.")
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch Coinbase time: {e}. Defaulting offset to 0.0.")
        TIME_OFFSET = 0.0

def format_by_increment(value: Any, increment: Any) -> str:
    try:
        val = value if isinstance(value, Decimal) else Decimal(str(value))
        inc = increment if isinstance(increment, Decimal) else Decimal(str(increment))
        exponent = inc.normalize().as_tuple().exponent
        quant = Decimal((0, (1,), exponent))
        quantized = val.quantize(quant, rounding=ROUND_DOWN)
        decimals = abs(exponent)
        return f"{quantized:.{decimals}f}"
    except Exception as e:
        logger.warning("format_by_increment failed for value=%s increment=%s: %s", value, increment, e)
        return str(value)

def _normalize_to_dict(obj: Any) -> Dict:
    """Helper to convert SDK objects/responses into standard dictionaries."""
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if hasattr(obj, "json"):
        try:
            return obj.json()
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        return vars(obj)
    return {"raw_data": str(obj)}

async def safe_api_call(client: Any, method: str, endpoint: str, payload: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Non-destructive, async-safe wrapper for SDK calls.
    ALWAYS returns {"success": bool, "response": dict, "error": str}
    """
    if payload and "product_id" in payload:
        if payload["product_id"] not in PRODUCT_ID_ALLOWLIST:
            logger.critical("CIRCUIT BREAKER: Blocked unauthorized pair: %s", payload["product_id"])
            return {"success": False, "response": {}, "error": "UNAUTHORIZED_PRODUCT"}

    backoff = DEFAULT_BACKOFF_SECONDS
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            api_result = None
            
            # --- ROUTE: PLACE ORDERS ---
            if method.upper() == "POST" and endpoint.endswith("/orders"):
                api_payload = payload.copy() if payload else {}
                client_order_id = api_payload.get("client_order_id")
                product_id = api_payload.get("product_id")
                side = api_payload.get("side")
                order_conf = api_payload.get("order_configuration", {})
                limit_conf = order_conf.get("limit_limit_gtc", {}) if isinstance(order_conf, dict) else {}
                base_size = limit_conf.get("base_size")
                limit_price = limit_conf.get("limit_price")
                post_only = limit_conf.get("post_only", True)
                stp_id = api_payload.get("self_trade_prevention_id")

                try:
                    if side and side.upper() == "BUY" and hasattr(client, "limit_order_gtc_buy"):
                        sdk_kwargs = {
                            "client_order_id": client_order_id, "product_id": product_id,
                            "limit_price": limit_price, "base_size": base_size,
                            "post_only": post_only, "self_trade_prevention_id": stp_id
                        }
                        api_result = await asyncio.to_thread(client.limit_order_gtc_buy, **sdk_kwargs)
                    elif side and side.upper() == "SELL" and hasattr(client, "limit_order_gtc_sell"):
                        sdk_kwargs = {
                            "client_order_id": client_order_id, "product_id": product_id,
                            "limit_price": limit_price, "base_size": base_size,
                            "post_only": post_only, "self_trade_prevention_id": stp_id
                        }
                        api_result = await asyncio.to_thread(client.limit_order_gtc_sell, **sdk_kwargs)
                except Exception as e:
                    logger.warning("Typed helper call failed, falling back to create_order: %s", e)

                if api_result is None:
                    sdk_kwargs = {
                        "client_order_id": client_order_id, "product_id": product_id, "side": side,
                        "order_configuration": {"limit_limit_gtc": {"base_size": base_size, "limit_price": limit_price, "post_only": post_only}},
                        "self_trade_prevention_id": stp_id
                    }
                    api_result = await asyncio.to_thread(client.create_order, **sdk_kwargs)

            # --- ROUTE: FETCH ACCOUNTS ---
            elif method.upper() == "GET" and endpoint.endswith("/accounts"):
                if hasattr(client, "get_accounts"):
                    api_result = await asyncio.to_thread(client.get_accounts)

            # --- ROUTE: FETCH PORTFOLIO (DUST SKIMMER) ---
            elif method.upper() == "GET" and "/portfolios/" in endpoint and "move_funds" not in endpoint:
                portfolio_id = endpoint.split("/")[-1]
                
                # Try the official SDK methods first, then fallback to generic get
                if hasattr(client, "get_portfolio_breakdown"):
                    api_result = await asyncio.to_thread(client.get_portfolio_breakdown, portfolio_uuid=portfolio_id)
                elif hasattr(client, "get_portfolio"):
                    try:
                        api_result = await asyncio.to_thread(client.get_portfolio, portfolio_uuid=portfolio_id)
                    except TypeError:
                        # Some versions of the SDK use portfolio_id instead of portfolio_uuid
                        api_result = await asyncio.to_thread(client.get_portfolio, portfolio_id=portfolio_id)
                elif hasattr(client, "get"):
                    api_result = await asyncio.to_thread(client.get, endpoint)

                    
            # --- ROUTE: FETCH OPEN ORDERS / FILLS ---
            elif method.upper() == "GET" and "orders/historical" in endpoint:
                limit_val = payload.get("limit", 1000) if payload else 1000
                if "batch" in endpoint:
                    if hasattr(client, "list_orders"):
                        api_result = await asyncio.to_thread(client.list_orders, order_status=["OPEN"], limit=limit_val)
                    elif hasattr(client, "get"):
                        api_result = await asyncio.to_thread(client.get, endpoint)
                elif "fills" in endpoint:
                    if payload and "order_id" in payload:
                        api_result = await asyncio.to_thread(client.get, f"/api/v3/brokerage/orders/historical/fills", params=payload)
                    elif hasattr(client, "list_fills"):
                        api_result = await asyncio.to_thread(client.list_fills, limit=limit_val)
                    elif hasattr(client, "get"):
                        api_result = await asyncio.to_thread(client.get, endpoint, params=payload)
                
            # --- ROUTE: MOVE PORTFOLIO FUNDS ---
            elif method.upper() == "POST" and "move_funds" in endpoint:
                if hasattr(client, "move_portfolio_funds"):
                    api_payload = payload.copy() if payload else {}
                    api_result = await asyncio.to_thread(client.move_portfolio_funds, **api_payload)

            if api_result is None:
                logger.error("safe_api_call unsupported method/endpoint: %s %s", method, endpoint)
                return {"success": False, "response": {}, "error": "unsupported method"}

            normalized = _normalize_to_dict(api_result)
            
            # Extract true success metric
            is_success = normalized.get("success", True) if isinstance(normalized, dict) else True
            if "error_response" in normalized or "error" in normalized:
                is_success = False
                
            if is_success:
                return {"success": True, "response": normalized, "error": ""}
            else:
                err_msg = normalized.get("error_response", {}).get("message", str(normalized))
                raise Exception(err_msg)

        except Exception as e:
            err_text = str(e)
            logger.warning("API attempt %d failed for %s %s: %s", attempt, method, endpoint, err_text)
            
            resp = getattr(e, "response", None)
            if resp is not None:
                status = getattr(resp, "status_code", None)
                body = getattr(resp, "text", "") or ""
                if status and (status >= 400):
                    snippet = body[:400].replace("\n", " ")
                    if "cloudflare" in snippet.lower() or "<html" in snippet.lower():
                        logger.error("CLOUDFLARE/HTML response detected (status=%s)", status)
            
            if "INVALID_LIMIT_PRICE" in err_text or "INSUFFICIENT_FUND" in err_text:
                return {"success": False, "response": {}, "error": err_text}

            if attempt == MAX_RETRIES:
                return {"success": False, "response": {}, "error": err_text}
            
            await asyncio.sleep(backoff)
            backoff *= 2

    return {"success": False, "response": {}, "error": "unreachable"}