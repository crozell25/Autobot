# utils.py
import os
import time
import logging
import asyncio
import requests
import json
from urllib.parse import urlparse
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
        
        server_time_raw = (
            json_resp.get("epochSeconds")
            or json_resp.get("epoch_seconds")
            or json_resp.get("data", {}).get("epochSeconds")
            or json_resp.get("data", {}).get("epoch_seconds")
            or json_resp.get("data", {}).get("timestamp")
        )
        
        if not server_time_raw:
            raise ValueError(f"Unexpected time payload structure: {json_resp}")
            
        server_epoch = float(server_time_raw)
        local_epoch = time.time()
        TIME_OFFSET = server_epoch - local_epoch
        
        if abs(TIME_OFFSET) > 2.0:
            logger.critical(f"⚠️ HIGH CLOCK DRIFT DETECTED: {TIME_OFFSET:.4f}s.")
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
        quant = Decimal(f"1e{exponent}")
        quantized = val.quantize(quant, rounding=ROUND_DOWN)
        decimals = abs(exponent)
        return f"{quantized:.{decimals}f}"
    except Exception as e:
        logger.warning("format_by_increment failed for value=%s increment=%s: %s", value, increment, e)
        return str(value)

def _normalize_to_dict(obj: Any) -> Any:
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        return {"items": [ _normalize_to_dict(i) if not isinstance(i, (str,int)) else i for i in obj ]}
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
    if payload and "product_id" in payload:
        if payload["product_id"] not in PRODUCT_ID_ALLOWLIST:
            logger.critical("CIRCUIT BREAKER: Blocked unauthorized pair: %s", payload["product_id"])
            return {"success": False, "response": {}, "error": "UNAUTHORIZED_PRODUCT"}

    backoff = DEFAULT_BACKOFF_SECONDS
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            api_result = None
            
            if method.upper() == "POST" and endpoint.endswith("/orders"):
                # Order routing logic remains...
                api_payload = payload.copy() if payload else {}
                sdk_kwargs = {
                    "client_order_id": api_payload.get("client_order_id"), 
                    "product_id": api_payload.get("product_id"), 
                    "side": api_payload.get("side"),
                    "order_configuration": api_payload.get("order_configuration", {}),
                    "self_trade_prevention_id": api_payload.get("stp_id") or api_payload.get("self_trade_prevention_id")
                }
                api_result = await asyncio.to_thread(client.create_order, **sdk_kwargs)

            elif method.upper() == "GET" and endpoint.endswith("/accounts"):
                if hasattr(client, "get_accounts"):
                    api_result = await asyncio.to_thread(client.get_accounts)

            elif method.upper() == "GET" and "/portfolios/" in endpoint and "move_funds" not in endpoint:
                parsed = urlparse(endpoint)
                path = parsed.path
                portfolio_id = path.rstrip("/").split("/")[-1]
                
                if hasattr(client, "get_portfolio_breakdown"):
                    api_result = await asyncio.to_thread(client.get_portfolio_breakdown, portfolio_uuid=portfolio_id)
                elif hasattr(client, "get"):
                    api_result = await asyncio.to_thread(client.get, path)

            elif method.upper() == "GET" and "orders/historical" in endpoint:
                if hasattr(client, "get"):
                    api_result = await asyncio.to_thread(client.get, endpoint, params=payload)
                
            elif method.upper() == "POST" and "move_funds" in endpoint:
                if hasattr(client, "move_portfolio_funds"):
                    api_payload = payload.copy() if payload else {}
                    api_result = await asyncio.to_thread(client.move_portfolio_funds, **api_payload)

            normalized = _normalize_to_dict(api_result)
            
            # --- FIXED SUCCESS DETECTION ---
            # Default to True for dict responses unless an explicit error footprint exists
            is_success = True if isinstance(normalized, dict) else False
            
            if isinstance(normalized, dict):
                if "success" in normalized:
                    is_success = normalized["success"]
                
                # If explicit error markers or API error blocks are present, drop success
                if "error_response" in normalized or "error" in normalized or "errors" in normalized:
                    is_success = False
                    
            if is_success:
                return {"success": True, "response": normalized, "error": ""}
            else:
                err_msg = normalized.get("error_response", {}).get("message", str(normalized)) if isinstance(normalized, dict) else str(normalized)
                raise Exception(err_msg)

        except Exception as e:
            err_text = str(e)
            logger.warning("API attempt %d failed for %s %s: %s", attempt, method, endpoint, err_text)
            
            if "INVALID_LIMIT_PRICE" in err_text or "INSUFFICIENT_FUND" in err_text:
                return {"success": False, "response": {}, "error": err_text}

            if attempt == MAX_RETRIES:
                return {"success": False, "response": {}, "error": err_text}
            
            await asyncio.sleep(backoff)
            backoff *= 2

    return {"success": False, "response": {}, "error": "unreachable"}