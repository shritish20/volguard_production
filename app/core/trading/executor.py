# app/core/trading/trade_executor.py

import upstox_client
from upstox_client.rest import ApiException
import asyncio
import uuid
import logging
from typing import Dict, List
from app.database import AsyncSessionLocal, TradeRecord
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)


class TradeExecutor:
    """
    SINGLE SOURCE OF TRUTH FOR ORDER EXECUTION.
    Never lie about execution state.
    """

    def __init__(self, access_token: str):
        cfg = upstox_client.Configuration()
        cfg.access_token = access_token
        self.api_client = upstox_client.ApiClient(cfg)

        self.order_api = upstox_client.OrderApi(self.api_client)
        self.portfolio_api = upstox_client.PortfolioApi(self.api_client)
        self.quote_api = upstox_client.MarketQuoteApi(self.api_client)

        # Idempotency cache (in-memory, supervisor-scoped)
        self._order_cache = set()

    # --------------------------------------------------
    # POSITIONS
    # --------------------------------------------------
    async def get_positions(self) -> List[Dict]:
        try:
            response = await asyncio.to_thread(
                self.portfolio_api.get_positions, api_version="2.0"
            )
            data = response.data or []

            positions = []
            for p in data:
                if int(p.quantity) == 0:
                    continue

                details = registry.get_instrument_details(p.instrument_token)
                side = "BUY" if p.net_quantity > 0 else "SELL"

                positions.append(
                    {
                        "position_id": p.instrument_token,
                        "instrument_key": p.instrument_token,
                        "symbol": p.trading_symbol,
                        "quantity": abs(int(p.net_quantity)),
                        "side": side,
                        "average_price": float(p.buy_price or p.sell_price),
                        "current_price": float(p.last_price),
                        "pnl": float(p.pnl),
                        "strike": details.get("strike"),
                        "expiry": details.get("expiry"),
                        "lot_size": details.get("lot_size"),
                        "option_type": "CE" if "CE" in p.trading_symbol else "PE",
                    }
                )

            return positions

        except Exception as e:
            logger.error(f"Position fetch failed: {e}")
            return []

    # --------------------------------------------------
    # EXECUTION
    # --------------------------------------------------
    async def execute_adjustment(self, adj: Dict) -> Dict:
        instrument_key = adj.get("instrument_key")
        qty = abs(int(adj.get("quantity", 0)))
        side = adj.get("side")
        strategy = adj.get("strategy", "AUTO")

        if qty <= 0 or side not in ("BUY", "SELL"):
            return {"status": "FAILED", "reason": "Invalid order params"}

        # Resolve dynamic future
        if instrument_key == "NIFTY_FUT_CURRENT":
            instrument_key = registry.get_current_future("NIFTY")
            if not instrument_key:
                return {"status": "FAILED", "reason": "Future not found"}

        # Idempotency key
        idem_key = f"{instrument_key}:{qty}:{side}:{strategy}"
        if idem_key in self._order_cache:
            logger.warning(f"Duplicate order blocked: {idem_key}")
            return {"status": "DUPLICATE"}

        self._order_cache.add(idem_key)

        # Fetch LTP
        ltp = await self._safe_ltp(instrument_key)

        # Escalating protection
        price = 0.0
        order_type = "MARKET"

        if ltp > 0:
            buffer = 0.02  # 2%
            price = round(ltp * (1 + buffer if side == "BUY" else 1 - buffer), 2)
            order_type = "LIMIT"

        try:
            req = upstox_client.PlaceOrderRequest(
                instrument_token=instrument_key,
                transaction_type=side,
                quantity=qty,
                order_type=order_type,
                price=price,
                product="D",
                validity="DAY",
                disclosed_quantity=0,
                trigger_price=0.0,
                is_amo=False,
                tag="VolGuard",
            )

            resp = await asyncio.to_thread(
                self.order_api.place_order, req, api_version="2.0"
            )

            await self._persist_trade(resp.data.order_id, instrument_key, qty, side, strategy)

            return {
                "status": "PLACED",
                "order_id": resp.data.order_id,
                "order_type": order_type,
                "price": price,
            }

        except ApiException as e:
            logger.error(f"Order rejected: {e}")
            return {"status": "FAILED", "error": str(e)}

    # --------------------------------------------------
    # HELPERS
    # --------------------------------------------------
    async def _safe_ltp(self, key: str) -> float:
        try:
            q = await asyncio.to_thread(
                self.quote_api.ltp, instrument_key=key, api_version="2.0"
            )
            return float(q.data[key.replace(":", "")].last_price)
        except Exception:
            return 0.0

    async def _persist_trade(self, order_id, token, qty, side, strategy):
        try:
            details = registry.get_instrument_details(token)
            async with AsyncSessionLocal() as session:
                trade = TradeRecord(
                    id=str(uuid.uuid4()),
                    trade_tag=order_id,
                    instrument_key=token,
                    quantity=qty,
                    side=side,
                    strategy=strategy,
                    strike=details.get("strike"),
                    expiry=details.get("expiry"),
                    lot_size=details.get("lot_size"),
                )
                session.add(trade)
                await session.commit()
        except Exception as e:
            logger.error(f"Trade persistence failed: {e}")

    # --------------------------------------------------
    # PANIC CLOSE
    # --------------------------------------------------
    async def close_all_positions(self, reason: str):
        positions = await self.get_positions()
        for p in positions:
            await self.execute_adjustment(
                {
                    "instrument_key": p["instrument_key"],
                    "quantity": p["quantity"],
                    "side": "SELL" if p["side"] == "BUY" else "BUY",
                    "strategy": reason,
                }
            )
