import asyncio
import logging
from typing import List, Dict, Optional
from datetime import datetime, date
import numpy as np  # Added for margin heuristics

from app.core.risk.schemas import MarginCheckResult
from app.config import settings

logger = logging.getLogger(__name__)

class CapitalGovernor:
    def __init__(self, access_token: str, total_capital: float, max_daily_loss: float = 5000.0, max_positions: int = 4):
        self.access_token = access_token
        self.total_capital = total_capital
        self.max_daily_loss = max_daily_loss
        self.max_positions = max_positions
        
        self.daily_pnl = 0.0
        self.position_count = 0
        self.failed_margin_calls = 0
        
        # FIX: Margin learning system
        self.margin_history = []  # Stores {"margin_per_lot": float, "timestamp": datetime}

    async def get_available_funds(self) -> float:
        """
        Mockable method to get available funds from Broker API
        """
        # In a real implementation, this would call the broker API
        # For this showcase, we return a simulation or cache
        return self.total_capital + self.daily_pnl

    async def predict_margin_requirement(self, legs: List[Dict]) -> float:
        """
        Mockable method to call Broker Margin API
        """
        # This acts as the interface to the Upstox Margin API
        # Implementation would use the self.access_token to fetch real data
        # Raising NotImplementedError to ensure the mock/real implementation is used
        raise NotImplementedError("This method must be implemented by the API client wrapper")

    async def estimate_brokerage(self, legs: List[Dict]) -> float:
        """
        Estimate brokerage charges (approx ₹20 per order + taxes)
        """
        num_orders = len(legs)
        return num_orders * 25.0  # Conservative estimate

    # ------------------------------------------------------------------
    # FIX #1: Margin Fallback - Hard Fail in Production
    # ------------------------------------------------------------------

    async def can_trade_new(self, legs: List[Dict], strategy_name: str = "MANUAL") -> MarginCheckResult:
        """
        Master decision function with STRICT margin validation.
        """
        # 1. Internal Safety Checks
        if self.daily_pnl <= -abs(self.max_daily_loss):
            return MarginCheckResult(False, f"Max Daily Loss Reached ({self.daily_pnl})")
        
        if self.position_count >= self.max_positions:
            is_exit = any(l.get("action") == "EXIT" for l in legs)
            if not is_exit:
                return MarginCheckResult(False, "Max Position Count Reached")
        
        # 2. Get Real Money
        try:
            available_funds = await asyncio.wait_for(
                self.get_available_funds(), 
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.error("Funds fetch timeout")
            return MarginCheckResult(False, "Cannot verify available funds (timeout)")
        except Exception as e:
            logger.error(f"Funds fetch failed: {e}")
            return MarginCheckResult(False, f"Cannot verify available funds: {e}")
        
        # 3. Predict Margin - CRITICAL SECTION
        margin_source = "UNKNOWN"
        try:
            required_margin = await asyncio.wait_for(
                self.predict_margin_requirement(legs),
                timeout=5.0
            )
            margin_source = "UPSTOX_API"
            logger.info(f"Margin requirement: ₹{required_margin:,.0f} (source: {margin_source})")
            
        except asyncio.TimeoutError:
            logger.error("⚠️ CRITICAL: Margin API timeout")
            return MarginCheckResult(
                allowed=False,
                reason="Margin API timeout - cannot verify safety",
                required_margin=0.0,
                available_margin=available_funds
            )
            
        except Exception as e:
            logger.error(f"⚠️ CRITICAL: Margin API failed: {e}")
            
            # 🔴 HARD FAIL IN PRODUCTION MODES
            from app.lifecycle.safety_controller import ExecutionMode
            
            # Check execution mode from settings
            current_mode = getattr(settings, 'EXECUTION_MODE', 'SHADOW')
            
            if current_mode in ['FULL_AUTO', 'production_live']:
                logger.critical("🛑 BLOCKING TRADE: Margin API unavailable in FULL_AUTO mode")
                return MarginCheckResult(
                    allowed=False,
                    reason="CRITICAL: Margin API unavailable in FULL_AUTO mode",
                    required_margin=0.0,
                    available_margin=available_funds
                )
            
            elif current_mode in ['SEMI_AUTO', 'production_semi']:
                logger.error("⚠️ Using margin heuristic in SEMI_AUTO (requires manual approval)")
                required_margin = self._estimate_margin_heuristic(legs)
                margin_source = "HEURISTIC_FALLBACK"
                
            else:  # SHADOW mode
                logger.warning("⚠️ Using margin heuristic in SHADOW mode")
                required_margin = self._estimate_margin_heuristic(legs)
                margin_source = "HEURISTIC_FALLBACK"
        
        # Buffer: Keep 10% free always
        safe_margin_limit = available_funds * 0.90
        
        if required_margin > safe_margin_limit:
            self.failed_margin_calls += 1
            return MarginCheckResult(
                allowed=False,
                reason=f"Insufficient Margin (Req: ₹{required_margin:,.0f} | Avail: ₹{available_funds:,.0f} | Source: {margin_source})",
                required_margin=required_margin,
                available_margin=available_funds
            )
        
        # 4. Brokerage Check
        est_brokerage = await self.estimate_brokerage(legs)
        
        # 5. Log margin source for audit trail
        logger.info(f"✅ Margin check PASSED - Source: {margin_source}, Required: ₹{required_margin:,.0f}, Available: ₹{available_funds:,.0f}")
        
        return MarginCheckResult(
            allowed=True,
            reason=f"OK (margin_source={margin_source})",
            required_margin=required_margin,
            available_margin=available_funds,
            brokerage_estimate=est_brokerage
        )

    def _estimate_margin_heuristic(self, legs: List[Dict]) -> float:
        """
        IMPROVED: Learning-based fallback with historical data
        """
        total = 0.0
        
        # Use historical margin data if available
        if hasattr(self, 'margin_history') and len(self.margin_history) >= 10:
            # Calculate per-lot average from last 100 trades
            historical_margins = [m['margin_per_lot'] for m in self.margin_history]
            avg_margin_per_lot = np.percentile(historical_margins, 95)  # 95th percentile for safety
            
            total_lots = sum(leg.get('quantity', 0) // 50 for leg in legs)  # Assuming 50 lot size
            estimated = total_lots * avg_margin_per_lot * 1.20  # 20% safety buffer
            
            logger.warning(f"Using learned margin estimate: ₹{estimated:,.0f} (based on {len(self.margin_history)} historical trades)")
            return estimated
        
        # Cold-start fallback: Conservative estimates
        for leg in legs:
            qty = leg.get("quantity", 0)
            side = leg.get("side", "BUY")
            
            if side == "SELL":
                # Check if it's expiry day
                expiry = leg.get("expiry")
                if expiry:
                    if isinstance(expiry, str):
                        try:
                            expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                        except ValueError:
                            # Handle cases where expiry might be in a different format or invalid
                            expiry_date = date.today() # Fail safe to high margin
                    else:
                        expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
                    
                    # 🔴 CRITICAL FIX: Higher margin on expiry day
                    if expiry_date == date.today():
                        total += qty * 5000  # 250k per lot on expiry day
                        logger.warning(f"⚠️ EXPIRY DAY: Using 5k per unit margin")
                    else:
                        total += qty * 2400  # 120k per lot normal
                else:
                    total += qty * 2400
            else:
                total += qty * 200  # Max premium for buys
        
        # Add 30% buffer for uncertainty
        buffered_total = total * 1.30
        
        logger.warning(f"⚠️ Using COLD START margin estimate: ₹{buffered_total:,.0f} (NO HISTORICAL DATA)")
        return buffered_total

    def record_actual_margin(self, required_margin: float, num_lots: int):
        """
        Call this after successful trade execution to learn actual margins
        """
        if num_lots > 0:
            margin_per_lot = required_margin / num_lots
            self.margin_history.append({
                "margin_per_lot": margin_per_lot,
                "timestamp": datetime.now(),
                "total_margin": required_margin,
                "lots": num_lots
            })
            
            # Keep only last 100 records
            if len(self.margin_history) > 100:
                self.margin_history.pop(0)
            
            logger.info(f"📊 Learned margin: ₹{margin_per_lot:,.0f} per lot")
