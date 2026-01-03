# app/core/trading/executor.py

import requests
import logging
import uuid
from typing import Dict, List
from app.database import AsyncSessionLocal, TradeRecord
from app.services.instrument_registry import registry
from app.config import settings

logger = logging.getLogger(__name__)

HFT_BASE = "https://api-hft.upstox.com"
API_BASE = "https://api.upstox.com"


class TradeExecutor:
    """
    REST-only Upstox Executor
    Aligned 100% with official Upstox v2 / v3 endpoints
    """

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    # ------------------------------------------------
    # POSITIONS
    # ------------------------------------------------
    async def get_positions(self) -> List[Dict]:
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

                details = registry.get_instrument_details(p["instrument_token"])

                positions.append({
                    "position_id": p["instrument_token"],
                    "instrument_key": p["instrument_token"],
                    "symbol": p.get("trading_symbol"),
                    "quantity": int(p["quantity"]),
                    "side": "BUY" if int(p["quantity"]) > 0 else "SELL",
                    "average_price": float(p.get("average_price", 0)),
                    "current_price": float(p.get("last_price", 0)),
                    "pnl": float(p.get("pnl", 0)),
                    "strike": details.get("strike"),
                    "expiry": details.get("expiry"),
                    "lot_size": details.get("lot_size", 0),
                    "option_type": "CE" if "CE" in p.get("trading_symbol", "") else "PE"
                })

            return positions

        except Exception as e:
            logger.error(f"Position fetch failed: {e}")
            return []

    # ------------------------------------------------
    # LTP (for limit protection)
    # ------------------------------------------------
    def _get_ltp(self, instrument_key: str) -> float:
        url = f"{API_BASE}/v3/market-quote/ltp"
        try:
            resp = requests.get(
                url,
                headers=self.headers,
                params={"instrument_key": instrument_key},
                timeout=3
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            return float(next(iter(data.values()))["last_price"])
        except Exception:
            return 0.0

    # ------------------------------------------------
    # PLACE ORDER
    # ------------------------------------------------
    async def execute_adjustment(self, adjustment: Dict) -> Dict:
        """
        POST /v3/order/place
        """
        url = f"{HFT_BASE}/v3/order/place"

        instrument_key = adjustment["instrument_key"]
        qty = abs(int(adjustment["quantity"]))
        side = adjustment["side"]

        ltp = self._get_ltp(instrument_key)

        order_type = "MARKET"
        price = 0.0

        if ltp > 0:
            order_type = "LIMIT"
            price = round(ltp * (1.05 if side == "BUY" else 0.95), 2)

        payload = {
            "quantity": qty,
            "product": "D",
            "validity": "DAY",
            "price": price,
            "instrument_token": instrument_key,
            "order_type": order_type,
            "transaction_type": side,
            "disclosed_quantity": 0,
            "trigger_price": 0.0,
            "is_amo": False,
            "slice": True,
            "tag": "VolGuard_Auto"
        }

        try:
            resp = requests.post(url, headers=self.headers, json=payload, timeout=5)
            resp.raise_for_status()
            order_id = resp.json()["data"]["order_id"]

            await self._persist_trade(order_id, instrument_key, qty, side, adjustment)

            return {"status": "SUCCESS", "order_id": order_id}

        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    # ------------------------------------------------
    # EXIT ALL POSITIONS
    # ------------------------------------------------
    async def close_all_positions(self, reason: str):
        """
        POST /v2/order/positions/exit
        """
        url = f"{API_BASE}/v2/order/positions/exit"

        try:
            resp = requests.post(url, headers=self.headers, json={}, timeout=5)
            resp.raise_for_status()
            logger.critical(f"GLOBAL EXIT triggered: {reason}")
        except Exception as e:
            logger.critical(f"GLOBAL EXIT FAILED: {e}")

    # ------------------------------------------------
    # DB PERSISTENCE
    # ------------------------------------------------
    async def _persist_trade(self, order_id, token, qty, side, adjustment):
        try:
            details = registry.get_instrument_details(token)
            async with AsyncSessionLocal() as session:
                trade = TradeRecord(
                    id=str(uuid.uuid4()),
                    trade_tag=order_id,
                    instrument_key=token,
                    quantity=qty,
                    side=side,
                    strategy=adjustment.get("strategy", "AUTO"),
                    strike=details.get("strike"),
                    expiry=details.get("expiry"),
                    lot_size=details.get("lot_size"),
                    reason=adjustment.get("reason")
                )
                session.add(trade)
                await session.commit()
        except Exception as e:
            logger.error(f"Trade persistence failed: {e}")
