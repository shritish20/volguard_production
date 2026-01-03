import requests
import logging
import uuid
from typing import Dict, List, Optional
from app.database import AsyncSessionLocal, TradeRecord
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

# -------------------------------
# BASE URLS
# -------------------------------
BASE_HFT = "https://api-hft.upstox.com/v3"
BASE_V2  = "https://api.upstox.com/v2"

ALGO_NAME = "VolGuard"


class TradeExecutor:
    """
    REST-only Upstox Trade Executor
    Fully aligned with v2 / v3 official endpoints
    """

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Algo-Name": ALGO_NAME
        }

    # ==========================================================
    # POSITIONS (v2)
    # ==========================================================
    async def get_positions(self) -> List[Dict]:
        url = f"{BASE_V2}/portfolio/short-term-positions"

        try:
            resp = requests.get(url, headers=self.headers, timeout=5)
            resp.raise_for_status()

            data = resp.json().get("data", [])
            positions = []

            for p in data:
                if p.get("quantity", 0) == 0:
                    continue

                details = registry.get_instrument_details(p["instrument_token"])

                positions.append({
                    "position_id": p["instrument_token"],
                    "instrument_key": p["instrument_token"],
                    "symbol": p.get("trading_symbol"),
                    "quantity": abs(int(p["quantity"])),
                    "side": "BUY" if int(p["quantity"]) > 0 else "SELL",
                    "average_price": float(p.get("buy_price") or p.get("sell_price") or 0),
                    "current_price": float(p.get("last_price", 0)),
                    "pnl": float(p.get("pnl", 0)),
                    "strike": details.get("strike"),
                    "expiry": details.get("expiry"),
                    "lot_size": details.get("lot_size", 50),
                    "option_type": "CE" if "CE" in p.get("trading_symbol", "") else "PE"
                })

            return positions

        except Exception as e:
            logger.error(f"Position fetch failed: {e}")
            return []

    # ==========================================================
    # PLACE ORDER (v3)
    # ==========================================================
    async def execute_adjustment(self, adj: Dict) -> Dict:
        instrument = adj["instrument_key"]

        # Resolve dynamic future
        if instrument == "NIFTY_FUT_CURRENT":
            instrument = registry.get_current_future("NIFTY")
            if not instrument:
                return {"status": "FAILED", "reason": "Future not found"}

        qty  = int(adj["quantity"])
        side = adj["side"]

        payload = {
            "quantity": qty,
            "product": "D",
            "validity": "DAY",
            "price": 0,
            "tag": ALGO_NAME,
            "instrument_token": instrument,
            "order_type": "MARKET",
            "transaction_type": side,
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False,
            "slice": True
        }

        try:
            resp = requests.post(
                f"{BASE_HFT}/order/place",
                headers=self.headers,
                json=payload,
                timeout=5
            )
            resp.raise_for_status()

            order_id = resp.json()["data"]["order_id"]
            await self._persist_trade(order_id, instrument, qty, side, adj.get("strategy"))

            return {"status": "SUCCESS", "order_id": order_id}

        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    # ==========================================================
    # MODIFY ORDER (v3)
    # ==========================================================
    def modify_order(self, order_id: str, price: float, qty: int):
        payload = {
            "order_id": order_id,
            "price": round(price, 2),
            "quantity": qty,
            "validity": "DAY",
            "order_type": "LIMIT",
            "disclosed_quantity": 0,
            "trigger_price": 0
        }

        return requests.put(
            f"{BASE_HFT}/order/modify",
            headers=self.headers,
            json=payload,
            timeout=5
        ).json()

    # ==========================================================
    # CANCEL ORDER (v3)
    # ==========================================================
    def cancel_order(self, order_id: str):
        return requests.delete(
            f"{BASE_HFT}/order/cancel",
            headers=self.headers,
            params={"order_id": order_id},
            timeout=5
        ).json()

    # ==========================================================
    # EXIT ALL POSITIONS (v2)
    # ==========================================================
    async def close_all_positions(self, reason: str):
        logger.critical(f"CLOSING ALL POSITIONS: {reason}")

        try:
            requests.post(
                f"{BASE_V2}/order/positions/exit",
                headers=self.headers,
                json={},
                timeout=6
            )
        except Exception as e:
            logger.critical(f"FORCED EXIT FAILED: {e}")

    # ==========================================================
    # PERSISTENCE
    # ==========================================================
    async def _persist_trade(self, order_id, token, qty, side, strategy):
        try:
            details = registry.get_instrument_details(token)
            async with AsyncSessionLocal() as session:
                session.add(
                    TradeRecord(
                        id=str(uuid.uuid4()),
                        trade_tag=order_id,
                        instrument_key=token,
                        quantity=qty,
                        side=side,
                        strategy=strategy or "AUTO",
                        strike=details.get("strike"),
                        expiry=details.get("expiry"),
                        lot_size=details.get("lot_size")
                    )
                )
                await session.commit()
        except Exception as e:
            logger.error(f"Trade persistence failed: {e}")
