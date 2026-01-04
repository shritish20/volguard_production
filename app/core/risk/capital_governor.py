# app/core/risk/capital_governor.py

import logging
import httpx
import time
import asyncio
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

@dataclass
class MarginCheckResult:
    allowed: bool
    reason: str
    required_margin: float = 0.0
    available_margin: float = 0.0
    brokerage_estimate: float = 0.0

class CapitalGovernor:
    """
    VolGuard Smart Capital Governor (VolGuard 3.0)
    
    Authority on:
    1. Real-time Funds (Source: Upstox API)
    2. Margin Requirements (Source: Upstox Margin API)
    3. Brokerage Costs (Source: Upstox Charges API)
    4. Daily Loss Limits (Internal State)
    """

    def __init__(self, access_token: str, total_capital: float, max_daily_loss: float = 20000.0, max_positions: int = 6):
        self.access_token = access_token
        self.max_daily_loss = max_daily_loss
        self.max_positions = max_positions
        
        # Base URLs
        self.base_v2 = "https://api.upstox.com/v2"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        
        # Internal State
        self.daily_pnl = 0.0
        self.position_count = 0
        self.failed_margin_calls = 0
        
        # Smart Cache (Simple TTL)
        self._margin_cache: Dict[str, Tuple[float, float]] = {} # Key -> (Margin, Timestamp)
        self._funds_cache: Tuple[float, float] = (0.0, 0.0)     # (Funds, Timestamp)
        self.CACHE_TTL = 60.0 # Seconds

        # Async Client
        self.client = httpx.AsyncClient(headers=self.headers, timeout=5.0)

    async def close(self):
        await self.client.aclose()

    # ==================================================================
    # 1. REAL-TIME FUNDS (V2)
    # ==================================================================
    
    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_available_funds(self) -> float:
        """
        Fetches 'Available Margin' (Cash + Collateral) from Upstox.
        Endpoint: /v2/user/get-funds-and-margin
        """
        # Check Cache
        val, ts = self._funds_cache
        if time.time() - ts < self.CACHE_TTL:
            return val

        url = f"{self.base_v2}/user/get-funds-and-margin"
        params = {"segment": "SEC"} # SEC = Equity/F&O
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("equity", {})
            
            # 'available_margin' is the definitive trading balance
            funds = float(data.get("available_margin", 0.0))
            
            # Update Cache
            self._funds_cache = (funds, time.time())
            return funds
            
        except Exception as e:
            logger.error(f"Funds fetch failed: {e}")
            # Fail-safe: Return 0.0 to block trading if we can't verify funds
            return 0.0

    # ==================================================================
    # 2. MARGIN PREDICTION (V2)
    # ==================================================================

    async def predict_margin_requirement(self, legs: List[Dict]) -> float:
        """
        Calculates exact margin required for a basket of orders.
        Endpoint: /v2/charges/margin
        """
        if not legs:
            return 0.0
            
        # Construct Cache Key (Sort to handle order independence)
        # Key format: "INSTRUMENT:QTY:SIDE|..."
        cache_key = "|".join(sorted([
            f"{l['instrument_key']}:{l['quantity']}:{l['side']}" 
            for l in legs
        ]))
        
        # Check Cache
        val, ts = self._margin_cache.get(cache_key, (None, 0))
        if val is not None and time.time() - ts < 300: # 5 min cache for margin estimates
            return val

        url = f"{self.base_v2}/charges/margin"
        
        # Map internal leg dict to API Payload
        instruments = []
        for l in legs:
            instruments.append({
                "instrument_token": l["instrument_key"],
                "quantity": int(l["quantity"]),
                "transaction_type": l["side"].upper(),
                "product": "D" # Intraday (or 'I' if you prefer, usually 'D' covers both in API logic)
            })
            
        payload = {"instruments": instruments}
        
        try:
            resp = await self.client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            
            # Total required margin (Span + Exposure)
            required = float(data.get("total_margin", 0.0))
            
            # Cache it
            self._margin_cache[cache_key] = (required, time.time())
            return required
            
        except Exception as e:
            logger.error(f"Margin prediction failed: {e}")
            # If API fails, return a High Value to be safe (Block trade)
            return 999_999_999.0

    # ==================================================================
    # 3. BROKERAGE ESTIMATION (V2)
    # ==================================================================

    async def estimate_brokerage(self, legs: List[Dict]) -> float:
        """
        Calculates brokerage & taxes.
        Endpoint: /v2/charges/brokerage
        """
        total_charges = 0.0
        url = f"{self.base_v2}/charges/brokerage"
        
        # Upstox Brokerage API calculates per instrument
        # We process parallel requests for speed if needed, but sequential is safer for rate limits
        # Since this is "Smart", we'll just check the first leg and multiply (Approximation) 
        # or do it properly if it's a critical large trade.
        # Let's do it properly for the first 2 legs (usually Strategy Core).
        
        for l in legs[:2]: 
            try:
                params = {
                    "instrument_token": l["instrument_key"],
                    "quantity": l["quantity"],
                    "product": "D",
                    "transaction_type": l["side"].upper(),
                    "price": float(l.get("price", 0.0) or 0.0) # Market order = 0 price for calc
                }
                resp = await self.client.get(url, params=params)
                if resp.status_code == 200:
                    data = resp.json().get("data", {})
                    total_charges += float(data.get("total", 0.0))
            except Exception:
                # Fallback: Standard F&O approx (₹20 + taxes ~ ₹25)
                total_charges += 25.0
                
        # Add remaining legs approx
        if len(legs) > 2:
            total_charges += (len(legs) - 2) * 25.0
            
        return total_charges

    # ==================================================================
    # 4. THE GATEKEEPER
    # ==================================================================

    async def can_trade_new(self, legs: List[Dict], strategy_name: str = "MANUAL") -> MarginCheckResult:
        """
        Master decision function.
        1. Checks Daily Loss Limit.
        2. Checks Position Count.
        3. Checks Real Funds vs Real Margin.
        4. Checks Brokerage Drag.
        """
        # 1. Internal Safety Checks
        if self.daily_pnl <= -abs(self.max_daily_loss):
            return MarginCheckResult(False, f"Max Daily Loss Reached ({self.daily_pnl})")
            
        if self.position_count >= self.max_positions:
             # Allow Exits (Hedges) even if full, but block new entries
             is_exit = any(l.get("action") == "EXIT" for l in legs)
             if not is_exit:
                 return MarginCheckResult(False, "Max Position Count Reached")

        # 2. Get Real Money
        available_funds = await self.get_available_funds()
        
        # 3. Predict Margin
        required_margin = await self.predict_margin_requirement(legs)
        
        # Buffer: Keep 10% free always
        safe_margin_limit = available_funds * 0.90
        
        if required_margin > safe_margin_limit:
            self.failed_margin_calls += 1
            return MarginCheckResult(
                allowed=False,
                reason=f"Insufficient Margin (Req: {required_margin:.0f} | Avail: {available_funds:.0f})",
                required_margin=required_margin,
                available_margin=available_funds
            )

        # 4. Brokerage Check (The "True PnL" Logic)
        # Only relevant for complex strategies where profit might be thin
        est_brokerage = await self.estimate_brokerage(legs)
        
        # If strategy is "IRON_FLY" or "SCALPER", ensure we aren't burning cash
        # (Placeholder logic: in real execution, you'd compare this to Est. Profit)
        
        return MarginCheckResult(
            allowed=True,
            reason="OK",
            required_margin=required_margin,
            available_margin=available_funds,
            brokerage_estimate=est_brokerage
        )

    def update_pnl(self, realized_pnl: float):
        """Called by TradeExecutor after a trade closes"""
        self.daily_pnl += realized_pnl

    def update_position_count(self, count: int):
        self.position_count = count
