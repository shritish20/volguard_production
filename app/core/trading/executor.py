import upstox_client
from upstox_client.rest import ApiException
from app.database import AsyncSessionLocal, TradeRecord
from app.services.instrument_registry import registry
import uuid
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class TradeExecutor:
    def __init__(self, access_token: str):
        config = upstox_client.Configuration()
        config.access_token = access_token
        self.api_client = upstox_client.ApiClient(config)
        self.order_api = upstox_client.OrderApi(self.api_client)
        self.portfolio_api = upstox_client.PortfolioApi(self.api_client)
        self.quote_api = upstox_client.MarketQuoteApi(self.api_client) # Needed for Limit Protection

    async def get_positions(self) -> List[Dict]:
        """Fetch positions and ENRICH with Registry Data"""
        try:
            response = self.portfolio_api.get_positions(api_version="2.0")
            data = response.data
            if not data: return []
            
            positions = []
            for p in data:
                if p.quantity != 0:
                    details = registry.get_instrument_details(p.instrument_token)
                    positions.append({
                        "position_id": p.instrument_token,
                        "instrument_key": p.instrument_token,
                        "symbol": p.trading_symbol,
                        "quantity": int(p.quantity),
                        "side": "BUY" if int(p.quantity) > 0 else "SELL",
                        "average_price": float(p.buy_price) if int(p.quantity) > 0 else float(p.sell_price),
                        "current_price": float(p.last_price),
                        "pnl": float(p.pnl),
                        "strike": details.get("strike", 0.0),
                        "expiry": details.get("expiry"),
                        "lot_size": details.get("lot_size", 0),
                        "option_type": "CE" if "CE" in p.trading_symbol else "PE"
                    })
            return positions
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    async def execute_adjustment(self, adjustment: Dict) -> Dict:
        key = adjustment.get("instrument_key")
        
        # Resolve Dynamic Token
        if key == "NIFTY_FUT_CURRENT":
            key = registry.get_current_future("NIFTY")
            if not key: return {"status": "FAILED", "reason": "Future Not Found"}

        qty = abs(int(adjustment.get("quantity", 0)))
        side = adjustment.get("side", "BUY")
        
        # 1. Fetch LTP for Limit Protection
        try:
            quote = self.quote_api.ltp(instrument_key=key, api_version="2.0")
            ltp = float(quote.data[key.replace(':', '')].last_price)
        except:
            ltp = 0.0

        # 2. Calculate Limit Price (Marketable Limit)
        # Buy: LTP + 5% | Sell: LTP - 5%
        # This guarantees execution like a Market order but prevents filling at infinity.
        if side == "BUY":
            price = ltp * 1.05 if ltp > 0 else 0
        else:
            price = ltp * 0.95 if ltp > 0 else 0
            
        # If LTP failed, fall back to MARKET (Risky, but necessary if data is down)
        order_type = "LIMIT" if price > 0 else "MARKET"

        try:
            req = upstox_client.PlaceOrderRequest(
                quantity=qty,
                product="D", 
                validity="DAY", 
                price=round(price, 2),
                tag="VolGuard_Auto",
                instrument_token=key,
                order_type=order_type,
                transaction_type=side,
                disclosed_quantity=0,
                trigger_price=0.0,
                is_amo=False
            )
            
            resp = self.order_api.place_order(req, api_version="2.0")
            
            await self._persist_trade(resp.data.order_id, key, qty, side, adjustment.get("strategy"))
            return {"status": "SUCCESS", "order_id": resp.data.order_id}
            
        except ApiException as e:
            logger.error(f"API Error: {e}")
            return {"status": "FAILED", "error": str(e)}

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
                    strategy=strategy or "AUTO",
                    strike=details.get("strike"),
                    expiry=details.get("expiry"),
                    lot_size=details.get("lot_size")
                )
                session.add(trade)
                await session.commit()
        except Exception as e:
            logger.error(f"Persistence Failed: {e}")

    async def close_all_positions(self, reason: str):
        # Implementation of panic close logic
        positions = await self.get_positions()
        for p in positions:
            side = "SELL" if p['side'] == "BUY" else "BUY" # Inverse side
            await self.execute_adjustment({
                "instrument_key": p['instrument_key'],
                "quantity": p['quantity'],
                "side": side,
                "strategy": reason
            })
