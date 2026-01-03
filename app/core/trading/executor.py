# app/core/trading/executor.py

import logging
import requests
from typing import Dict, List, Optional
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

HFT_BASE = "https://api-hft.upstox.com"
API_BASE = "https://api.upstox.com"


class TradeExecutor:
    """
    REST-only Upstox Trade Executor
    Aligned strictly with official Upstox REST API (v2/v3 HFT)
    """

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # ==========================================================
    # POSITIONS
    # ==========================================================
    def get_positions(self) -> List[Dict]:
        """
        GET /v2/portfolio/short-term-positions
        """
        url = f"{API_BASE}/v2/portfolio/short-term-positions"
        try:
            resp = requests.get(url, headers=self.headers, timeout=5)
            resp.raise_for_status()
            data = resp.json().get("data", [])

            positions = []
            for p in data:
                if p.get("quantity", 0) == 0:
                    continue

                token = p.get("instrument_token")
                meta = registry.get_instrument_details(token)

                positions.append({
                    "position_id": token,
                    "instrument_key": token,
                    "symbol": p.get("trading_symbol"),
                    "quantity": int(p.get("quantity", 0)),
                    "side": "BUY" if int(p.get("quantity", 0)) > 0 else "SELL",
                    "average_price": float(p.get("average_price", 0)),
                    "current_price": float(p.get("last_price", 0)),
                    "pnl": float(p.get("pnl", 0)),
                    "strike": meta.get("strike"),
                    "expiry": meta.get("expiry"),
                    "lot_size": meta.get("lot_size"),
                    "option_type": "CE" if "CE" in p.get("trading_symbol", "") else "PE",
                })

            return positions

        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            return []

    # ==========================================================
    # ORDER PLACEMENT (V3 HFT)
    # ==========================================================
    def place_order(self, order: Dict) -> Dict:
        """
        POST /v3/order/place
        """
        url = f"{HFT_BASE}/v3/order/place"

        payload = {
            "quantity": int(order["quantity"]),
            "product": "D",
            "validity": "DAY",
            "price": float(order.get("price", 0)),
            "tag": order.get("strategy", "VolGuard"),
            "instrument_token": order["instrument_key"],
            "order_type": order.get("order_type", "MARKET"),
            "transaction_type": order["side"],
            "disclosed_quantity": 0,
            "trigger_price": float(order.get("trigger_price", 0)),
            "is_amo": False,
            "slice": True,
        }

        try:
            resp = requests.post(url, headers=self.headers, json=payload, timeout=5)
            resp.raise_for_status()
            return {"status": "SUCCESS", "response": resp.json()}
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    # ==========================================================
    # ORDER MODIFY (V3 HFT)
    # ==========================================================
    def modify_order(self, order_id: str, updates: Dict) -> Dict:
        """
        PUT /v3/order/modify
        """
        url = f"{HFT_BASE}/v3/order/modify"

        payload = {
            "order_id": order_id,
            "quantity": int(updates["quantity"]),
            "validity": "DAY",
            "price": float(updates["price"]),
            "order_type": updates.get("order_type", "LIMIT"),
            "disclosed_quantity": 0,
            "trigger_price": float(updates.get("trigger_price", 0)),
        }

        try:
            resp = requests.put(url, headers=self.headers, json=payload, timeout=5)
            resp.raise_for_status()
            return {"status": "SUCCESS", "response": resp.json()}
        except Exception as e:
            logger.error(f"Order modify failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    # ==========================================================
    # ORDER CANCEL (V3 HFT)
    # ==========================================================
    def cancel_order(self, order_id: str) -> Dict:
        """
        DELETE /v3/order/cancel
        """
        url = f"{HFT_BASE}/v3/order/cancel"
        params = {"order_id": order_id}

        try:
            resp = requests.delete(url, headers=self.headers, params=params, timeout=5)
            resp.raise_for_status()
            return {"status": "SUCCESS", "response": resp.json()}
        except Exception as e:
            logger.error(f"Order cancel failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    # ==========================================================
    # EXIT ALL POSITIONS (EMERGENCY / EXPIRY)
    # ==========================================================
    def exit_all_positions(self) -> Dict:
        """
        POST /v2/order/positions/exit
        """
        url = f"{API_BASE}/v2/order/positions/exit"
        try:
            resp = requests.post(url, headers=self.headers, json={}, timeout=5)
            resp.raise_for_status()
            return {"status": "SUCCESS", "response": resp.json()}
        except Exception as e:
            logger.error(f"Exit all positions failed: {e}")
            return {"status": "FAILED", "error": str(e)}
