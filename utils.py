# utils.py
import json
import logging
import asyncio
import requests
import time
from urllib.parse import urlparse
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Union

logger = logging.getLogger("utils")
TIME_OFFSET = 0.0

def _normalize_to_dict(payload: Any) -> Dict[str, Any]:
    if payload is None: return {}
    if isinstance(payload, dict): return payload
    if isinstance(payload, str):
        try: return json.loads(payload)
        except Exception: return {"raw": payload}
    if hasattr(payload, "__dict__"):
        try: return dict(payload.__dict__)
        except Exception: return {"raw": str(payload)}
    return {"raw": str(payload)}

def format_by_increment(value: Any, increment: Any) -> str:
    try:
        val = value if isinstance(value, Decimal) else Decimal(str(value))
        inc = increment if isinstance(increment, Decimal) else Decimal(str(increment))
        exponent = inc.normalize().as_tuple().exponent
        quant = Decimal(f"1e{exponent}")
        if exponent > 0: return str(int(val))
        quantized = val.quantize(quant, rounding=ROUND_DOWN)
        decimals = abs(exponent)
        return f"{quantized:.{decimals}f}"
    except Exception as e:
        logger.warning("format_by_increment failed for value=%s increment=%s: %s", value, increment, e)
        return str(value)

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
            raise ValueError(f"Unexpected time payload: {json_resp}")
            
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
    return TIME_OFFSET

async def safe_api_call(
    client: Any,
    method: str,
    endpoint: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    method = method.upper()
    payload = payload or {}

    try:
        # FIREWALL: Block Forbidden Active Status Queries
        if method == "GET" and "historical/batch" in endpoint:
            status_val = payload.get("order_status") or payload.get("status")
            if status_val:
                statuses = status_val if isinstance(status_val, list) else [s.strip().upper() for s in str(status_val).split(',')]
                if any(s in ["OPEN", "PENDING"] for s in statuses):
                    logger.warning("🛡️ Firewall intercepted forbidden REST query for active statuses. Aborting network call.")
                    return {"success": False, "response": None, "error": "Query request does not support querying active status orders"}

        # GET requests
        if method == "GET":
            params: Dict[str, Any] = {}
            for key, value in payload.items():
                if isinstance(value, list):
                    params[key] = value
                else:
                    params[key] = value

            async def _do_get():
                return client.get(endpoint, params=params)

            raw = await asyncio.wait_for(_do_get(), timeout=timeout)

        # POST requests
        elif method == "POST":
            async def _do_post():
                # The SDK client.post() requires kwargs unwrapping, NOT json=
                return client.post(endpoint, **payload)

            raw = await asyncio.wait_for(_do_post(), timeout=timeout)

        else:
            return {"success": False, "response": None, "error": f"Unsupported method {method}"}

        if raw is None:
            return {"success": False, "response": None, "error": "Empty response"}

        normalized = _normalize_to_dict(raw)

        if "error" in normalized or "error_details" in normalized:
            logger.error("API Call failed [%s %s]: %s", method, endpoint, normalized)
            return {
                "success": False,
                "response": None,
                "error": normalized.get("error_details") or normalized.get("error") or "Unknown error",
            }

        return {"success": True, "response": normalized, "error": None}

    except asyncio.TimeoutError:
        logger.error("API timeout [%s %s]", method, endpoint)
        return {"success": False, "response": None, "error": "Timeout"}
    except Exception as e:
        logger.error("API exception [%s %s]: %s", method, endpoint, e, exc_info=True)
        return {"success": False, "response": None, "error": str(e)}