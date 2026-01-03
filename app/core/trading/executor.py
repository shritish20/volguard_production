import httpx
import logging
import asyncio
from typing import List, Dict, Optional
from datetime import datetime
import uuid

# Setup logging
logger = logging.getLogger(__name__)

class TradeExecutor:
    """
    The 'Hands' of the system.
    Executes orders via Upstox API and manages position state.
    """
    def __init__(self, access_token: str):
        self.base_url = "https://api.upstox.com/v2"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=5.0)
        self.active_trades_cache = {}  # Local cache of system-initiated trades

    async def get_positions(self) -> List[Dict]:
        """Fetch current open positions from Upstox"""
        try:
            response = await self.client.get(f"{self.base_url}/portfolio/short-term-positions")
            if response.status_code != 200:
                logger.error(f"Failed to fetch positions: {response.text}")
                return []
            
            data = response.json().get("data", [])
            # Normalize Upstox format to VolGuard format
            positions = []
            for p in data:
                if p['quantity'] != 0:  # Filter closed positions
                    positions.append({
                        "position_id": p.get('instrument_token', str(uuid.uuid4())),
                        "instrument_key": p['instrument_token'],
                        "symbol": p['trading_symbol'],
                        "quantity": int(p['quantity']),
                        "side": "BUY" if int(p['quantity']) > 0 else "SELL",
                        "average_price": float(p['buy_price']) if int(p['quantity']) > 0 else float(p['sell_price']),
                        "current_price": float(p['last_price']),
                        "pnl": float(p['pnl']),
                        "product": p['product'],
                        "exchange": p['exchange']
                    })
            return positions
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    async def get_active_trades(self) -> Dict[str, Dict]:
        """Return the internal tracking of complex trades (like Iron Condors)"""
        # In a real DB-backed system, this would query the 'trades' table.
        # For this implementation, we return the cleaned positions as individual 'trades'
        # or the local cache if you implement strategy-level tracking.
        return self.active_trades_cache

    async def execute_adjustment(self, adjustment: Dict) -> Dict:
        """
        Execute a specific adjustment (Buy/Sell/Close).
        This is called by the Supervisor after passing safety gates.
        """
        action = adjustment.get("action")
        logger.info(f"EXECUTING ADJUSTMENT: {action}")
        
        try:
            if action in ["CLOSE_POSITION", "REDUCE_EXPOSURE"]:
                return await self._place_order(
                    instrument_key=adjustment["instrument_key"],
                    quantity=abs(adjustment["quantity"]),
                    transaction_type="SELL" if adjustment["quantity"] > 0 else "BUY", # Close means opposite side
                    order_type="MARKET",
                    tag="ADJUST_CLOSE"
                )
            
            elif action in ["OPEN_TRADE", "DELTA_HEDGE", "ADD_LEG"]:
                return await self._place_order(
                    instrument_key=adjustment["instrument_key"],
                    quantity=abs(adjustment["quantity"]),
                    transaction_type=adjustment.get("side", "BUY"), # Explicit side needed
                    order_type="LIMIT",
                    price=adjustment.get("price", 0.0),
                    tag="ADJUST_OPEN"
                )
                
            else:
                logger.warning(f"Unknown adjustment action: {action}")
                return {"status": "FAILED", "reason": "Unknown Action"}
                
        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise e

    async def place_emergency_order(self, order_details: Dict) -> Dict:
        """
        High-priority synchronous-style order for emergencies.
        Force MARKET order.
        """
        logger.critical(f"PLACING EMERGENCY ORDER: {order_details}")
        return await self._place_order(
            instrument_key=order_details["instrument"],
            quantity=order_details["quantity"],
            transaction_type=order_details["side"],
            order_type="MARKET",
            tag="EMERGENCY"
        )

    async def close_all_positions(self, reason: str = "EMERGENCY"):
        """GLOBAL KILL SWITCH: Close everything immediately."""
        positions = await self.get_positions()
        logger.critical(f"CLOSING ALL {len(positions)} POSITIONS. REASON: {reason}")
        
        tasks = []
        for p in positions:
            # Place opposite order to close
            side = "SELL" if p['quantity'] > 0 else "BUY"
            qty = abs(p['quantity'])
            tasks.append(self._place_order(
                instrument_key=p['instrument_key'],
                quantity=qty,
                transaction_type=side,
                order_type="MARKET",
                tag="KILL_SWITCH"
            ))
        
        # Execute all closes in parallel
        await asyncio.gather(*tasks)
        return {"status": "CLOSED", "count": len(tasks)}

    async def _place_order(self, instrument_key: str, quantity: int, transaction_type: str, 
                          order_type: str = "LIMIT", price: float = 0.0, tag: str = "VolGuard") -> Dict:
        """Core Upstox Order Placement Logic"""
        url = f"{self.base_url}/order/place"
        
        payload = {
            "quantity": quantity,
            "product": "D",  # Delivery/Margin
            "validity": "DAY",
            "price": price,
            "tag": tag,
            "instrument_token": instrument_key,
            "order_type": order_type,
            "transaction_type": transaction_type,
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False
        }
        
        try:
            # Note: In production, check response carefully
            response = await self.client.post(url, json=payload)
            resp_json = response.json()
            
            if response.status_code == 200 and resp_json.get("status") == "success":
                logger.info(f"Order Placed: {resp_json['data']['order_id']}")
                return {"status": "SUCCESS", "order_id": resp_json['data']['order_id']}
            else:
                logger.error(f"Order Failed: {resp_json}")
                return {"status": "FAILED", "error": resp_json}
        except Exception as e:
            logger.error(f"API Error placing order: {e}")
            return {"status": "FAILED", "error": str(e)}

    async def close(self):
        await self.client.aclose()
