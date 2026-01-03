import upstox_client
from upstox_client.rest import ApiException
from app.database import AsyncSessionLocal, TradeRecord 
from app.services.instrument_registry import registry
import uuid
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class TradeExecutor:
    def __init__(self, access_token: str):
        config = upstox_client.Configuration()
        config.access_token = access_token
        self.api_client = upstox_client.ApiClient(config)
        self.order_api = upstox_client.OrderApi(self.api_client) 
        self.portfolio_api = upstox_client.PortfolioApi(self.api_client)

    async def get_positions(self) -> List[Dict]:
        """Fetch positions and ENRICH with Registry Data"""
        try:
            # API V2 Call
            response = self.portfolio_api.get_positions(api_version="2.0")
            data = response.data 
            if not data: return []
            
            positions = []
            for p in data:
                if p.quantity != 0:
                    # DATA ENRICHMENT (Fixes the "Blind" issue)
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
                        # CRITICAL: Add Metadata for Risk Engine
                        "strike": details.get("strike", 0.0),
                        "expiry": details.get("expiry"),
                        "lot_size": details.get("lot_size", 0),
                        "option_type": "CE" if "CE" in p.trading_symbol else "PE" # Simple inference
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

        try:
            req = upstox_client.PlaceOrderRequest(
                quantity=qty, product="D", validity="DAY", price=0.0, 
                tag="VolGuard_Auto", instrument_token=key, 
                order_type="MARKET", transaction_type=side, 
                disclosed_quantity=0, trigger_price=0.0, is_amo=False
            )
            resp = self.order_api.place_order(req, api_version="2.0")
            await self._persist_trade(resp.data.order_id, key, qty, side, adjustment.get("strategy"))
            return {"status": "SUCCESS", "order_id": resp.data.order_id}
        except ApiException as e:
            logger.error(f"API Error: {e}")
            return {"status": "FAILED", "error": str(e)}

    async def _persist_trade(self, order_id, token, qty, side, strategy):
        # Saves with full schema
        try:
            details = registry.get_instrument_details(token)
            async with AsyncSessionLocal() as session:
                trade = TradeRecord(
                    id=str(uuid.uuid4()), trade_tag=order_id,
                    instrument_key=token, quantity=qty, side=side,
                    strategy=strategy or "AUTO",
                    strike=details.get("strike"), expiry=details.get("expiry"),
                    lot_size=details.get("lot_size")
                )
                session.add(trade)
                await session.commit()
        except Exception as e:
            logger.error(f"Persistence Failed: {e}")
            
    async def get_active_trades(self): return {} # Stub
    async def place_emergency_order(self, d): return await self.execute_adjustment(d)
    async def close_all_positions(self, r): pass # Implement logic similar to above
