# app/core/trading/executor.py

import logging
import httpx
import asyncio
import uuid
import time
from typing import Dict, List, Optional, Any
from tenacity import retry, stop_after_attempt, wait_fixed
from app.database import AsyncSessionLocal, TradeRecord
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

class TradeExecutor:
    """
    VolGuard Smart Trade Executor (VolGuard 3.0)
    
    Architecture:
    - V3: Order Placement (with Freeze Slicing), Modifications, Cancellations
    - V2: Position Reporting (Short Term / F&O)
    - Protocol: Strict Async HTTP (No SDK)
    """

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_v3 = "https://api.upstox.com/v3"
        self.base_v2 = "https://api.upstox.com/v2"
        
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Idempotency cache (Prevent duplicate orders in same cycle)
        self._order_cache = set()
        
        # Async Client
        self.client = httpx.AsyncClient(
            headers=self.headers, 
            timeout=10.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

    async def close(self):
        await self.client.aclose()

    # ==================================================================
    # 1. POSITIONS (V2 - SOURCE OF TRUTH)
    # ==================================================================

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_positions(self) -> List[Dict]:
        """
        Fetches current F&O positions.
        Endpoint: /v2/portfolio/short-term-positions
        """
        url = f"{self.base_v2}/portfolio/short-term-positions"
        
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            
            positions = []
            for p in data:
                # Filter out closed positions (quantity 0)
                net_qty = int(p.get("net_quantity", 0))
                if net_qty == 0:
                    continue
                    
                # Normalize Data
                instrument_token = p.get("instrument_token")
                details = registry.get_instrument_details(instrument_token)
                
                side = "BUY" if net_qty > 0 else "SELL"
                
                positions.append({
                    "position_id": instrument_token,
                    "instrument_key": instrument_token,
                    "symbol": p.get("trading_symbol"),
                    "quantity": abs(net_qty),
                    "side": side,
                    "average_price": float(p.get("buy_price", 0) if net_qty > 0 else p.get("sell_price", 0)),
                    "current_price": float(p.get("last_price", 0.0)),
                    "pnl": float(p.get("pnl", 0.0)),
                    "strike": details.get("strike"),
                    "expiry": details.get("expiry"),
                    "lot_size": details.get("lot_size"),
                    "option_type": "CE" if "CE" in str(p.get("trading_symbol")) else "PE",
                    "product": p.get("product")
                })
                
            return positions
            
        except Exception as e:
            logger.error(f"Position fetch failed: {e}")
            return []

    # ==================================================================
    # 2. EXECUTION ENGINE (V3)
    # ==================================================================

    async def execute_adjustment(self, adj: Dict) -> Dict:
        """
        Main Entry Point for Trading.
        Handles Futures resolution, Idempotency, and Execution.
        """
        instrument_key = adj.get("instrument_key")
        qty = abs(int(adj.get("quantity", 0)))
        side = adj.get("side")
        strategy = adj.get("strategy", "AUTO")
        is_hedge = adj.get("is_hedge", False)
        
        if qty <= 0 or side not in ("BUY", "SELL"):
            return {"status": "FAILED", "reason": "Invalid Order Params"}

        # 1. Resolve Dynamic Futures
        if instrument_key == "NIFTY_FUT_CURRENT":
            instrument_key = registry.get_current_future("NIFTY")
            if not instrument_key:
                return {"status": "FAILED", "reason": "Future Not Found"}

        # 2. Idempotency Check
        # Key: "KEY:QTY:SIDE:STRATEGY"
        idem_key = f"{instrument_key}:{qty}:{side}:{strategy}"
        if idem_key in self._order_cache:
            logger.warning(f"Duplicate order blocked: {idem_key}")
            return {"status": "DUPLICATE"}
        self._order_cache.add(idem_key)

        # 3. Determine Order Type & Price
        # If 'price' is 0, we treat it as MARKET or Smart LIMIT
        target_price = float(adj.get("price", 0.0))
        order_type = "MARKET"
        
        # Smart Limit Logic: Avoid Market Orders on Options to prevent slippage
        # If no price provided, fetch LTP and buffer it
        if target_price <= 0 and "FUT" not in instrument_key: # Options
            ltp = await self._fetch_ltp_v3(instrument_key)
            if ltp > 0:
                buffer = 0.03 # 3% buffer for guaranteed fill (Pseudo-Market)
                if side == "BUY":
                    target_price = round(ltp * (1 + buffer), 1)
                else:
                    target_price = round(ltp * (1 - buffer), 1)
                order_type = "LIMIT"
            else:
                # If LTP fails, fall back to Market
                order_type = "MARKET"
        elif target_price > 0:
            order_type = "LIMIT"

        # 4. Execute V3 Order
        try:
            order_id = await self._place_order_v3(
                instrument_key, qty, side, order_type, target_price
            )
            
            # 5. Persist
            await self._persist_trade(order_id, instrument_key, qty, side, strategy, is_hedge)
            
            return {
                "status": "PLACED",
                "order_id": order_id,
                "type": order_type,
                "price": target_price
            }
            
        except Exception as e:
            logger.error(f"Execution Failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def _place_order_v3(self, key: str, qty: int, side: str, order_type: str, price: float) -> str:
        """
        Sends Order to Upstox V3 API.
        Enables 'slice' for handling large quantities automatically.
        """
        url = f"{self.base_v3}/order/place"
        
        payload = {
            "quantity": qty,
            "product": "D", # Intraday
            "validity": "DAY",
            "price": price,
            "tag": "VolGuard",
            "instrument_token": key,
            "order_type": order_type,
            "transaction_type": side,
            "disclosed_quantity": 0,
            "trigger_price": 0.0,
            "is_amo": False,
            "slice": True  # <--- SMART FEATURE: Auto-slice large orders
        }
        
        resp = await self.client.post(url, json=payload)
        resp_json = resp.json()
        
        if resp.status_code == 200 and resp_json.get("status") == "success":
            return resp_json.get("data", {}).get("order_id")
        else:
            errors = resp_json.get("errors", [])
            err_msg = errors[0].get("message") if errors else "Unknown Error"
            raise RuntimeError(f"Upstox Error: {err_msg}")

    # ==================================================================
    # 3. UTILITIES
    # ==================================================================

    async def _fetch_ltp_v3(self, key: str) -> float:
        """Quick LTP fetch for Limit Pricing"""
        url = f"{self.base_v3}/market-quote/ltp"
        params = {"instrument_key": key}
        try:
            resp = await self.client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                # Handle varying response formats (key with or without Exchange prefix)
                for k, v in data.items():
                    if key in k or k in key:
                        return float(v.get("last_price", 0.0))
            return 0.0
        except Exception:
            return 0.0

    async def _persist_trade(self, order_id, token, qty, side, strategy, is_hedge):
        """Log trade to Postgres"""
        try:
            details = registry.get_instrument_details(token)
            async with AsyncSessionLocal() as session:
                trade = TradeRecord(
                    id=str(uuid.uuid4()),
                    trade_tag=str(order_id),
                    instrument_key=token,
                    quantity=qty,
                    side=side,
                    strategy=strategy,
                    strike=details.get("strike"),
                    expiry=details.get("expiry"),
                    lot_size=details.get("lot_size"),
                    status="OPEN",
                    is_hedge=is_hedge,
                    timestamp=time.time() # Make sure your Model accepts this or uses default
                )
                session.add(trade)
                await session.commit()
        except Exception as e:
            logger.error(f"Trade Persistence Failed: {e}")

    async def close_all_positions(self, reason: str):
        """Panic Button: Close everything"""
        positions = await self.get_positions()
        results = []
        for p in positions:
            # Reverse side
            exit_side = "SELL" if p["side"] == "BUY" else "BUY"
            res = await self.execute_adjustment({
                "instrument_key": p["instrument_key"],
                "quantity": p["quantity"],
                "side": exit_side,
                "strategy": f"PANIC_{reason}",
                "price": 0.0 # Market Exit
            })
            results.append(res)
        return results
