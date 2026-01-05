import asyncio
import logging
from typing import List, Dict, Optional
from datetime import datetime, date
import numpy as np
from app.core.risk.schemas import MarginCheckResult
from app.config import settings

logger = logging.getLogger(__name__)

class MarginPredictor:
    """
    Machine learning-based margin predictor.
    Uses historical data to predict actual margin requirements.
    Falls back to conservative estimates if no data available.
    """
    
    def __init__(self):
        self.historical_data = []
        self.min_samples = 10
    
    def record_actual_margin(self, margin: float, strike: float, spot: float, 
                            dte: int, iv: float, side: str):
        """Record actual margin from successful trade"""
        moneyness = strike / spot if spot > 0 else 1.0
        
        self.historical_data.append({
            'margin_per_lot': margin / 50,  # Normalize to per lot
            'moneyness': moneyness,
            'dte': dte,
            'iv': iv,
            'side': side,
            'timestamp': datetime.now()
        })
        
        # Keep only last 500 records
        if len(self.historical_data) > 500:
            self.historical_data = self.historical_data[-500:]
    
    def predict(self, strike: float, spot: float, dte: int, 
                iv: float, side: str, qty: int) -> float:
        """
        Predict margin requirement using historical data.
        Falls back to conservative estimate if insufficient data.
        """
        
        if len(self.historical_data) < self.min_samples:
            return self._conservative_estimate(strike, spot, dte, side, qty)
        
        # Calculate features
        moneyness = strike / spot if spot > 0 else 1.0
        
        # Filter similar trades
        similar = [
            d for d in self.historical_data
            if abs(d['moneyness'] - moneyness) < 0.05  # Within 5% moneyness
            and abs(d['dte'] - dte) < 7  # Within 1 week
            and d['side'] == side
        ]
        
        if len(similar) < 3:
            # Not enough similar trades, use all data with weighting
            margins = [d['margin_per_lot'] for d in self.historical_data 
                      if d['side'] == side]
            
            if not margins:
                return self._conservative_estimate(strike, spot, dte, side, qty)
            
            # Use 95th percentile (conservative)
            margin_per_lot = np.percentile(margins, 95)
        else:
            # Use mean of similar trades + 20% buffer
            margins = [d['margin_per_lot'] for d in similar]
            margin_per_lot = np.mean(margins) * 1.20
        
        lots = qty // 50
        total_margin = margin_per_lot * lots * 1.10  # Add 10% safety buffer
        
        logger.debug(f"Predicted margin: ₹{total_margin:,.0f} based on {len(similar)} similar trades")
        return total_margin
    
    def _conservative_estimate(self, strike: float, spot: float, 
                               dte: int, side: str, qty: int) -> float:
        """
        Conservative margin estimates based on exchange guidelines.
        Used when no historical data available.
        """
        lots = qty // 50
        
        if side == "BUY":
            # Premium + 20% buffer
            moneyness = abs(strike - spot) / spot
            estimated_premium = spot * 0.03 * (1 - moneyness)  # Rough estimate
            return estimated_premium * qty * 1.20
        
        # SELL side - actual margin calculation
        moneyness = strike / spot
        
        # ATM/ITM options have higher margin
        if 0.95 <= moneyness <= 1.05:  # ATM ±5%
            if dte <= 2:  # Expiry week
                margin_per_lot = 280000  # Conservative expiry margin
            else:
                margin_per_lot = 220000  # ATM normal margin
        elif moneyness < 0.90 or moneyness > 1.10:  # Deep OTM
            margin_per_lot = 150000
        else:  # Slightly OTM
            margin_per_lot = 180000
        
        # Expiry day multiplier
        if dte == 0:
            margin_per_lot *= 1.5
        
        total = margin_per_lot * lots * 1.15  # 15% safety buffer
        
        logger.warning(f"Using conservative margin estimate: ₹{total:,.0f} (no historical data)")
        return total


class CapitalGovernor:
    def __init__(self, access_token: str, total_capital: float, max_daily_loss: float = 5000.0, max_positions: int = 4):
        self.access_token = access_token
        self.total_capital = total_capital
        self.max_daily_loss = max_daily_loss
        self.max_positions = max_positions
        
        self.daily_pnl = 0.0
        self.position_count = 0
        self.failed_margin_calls = 0
        
        # NEW: Margin predictor with ML-based estimation
        self.margin_predictor = MarginPredictor()

    async def get_available_funds(self) -> float:
        """
        Get available funds from broker API
        """
        # TODO: Implement actual API call
        # For now, use simple calculation
        return self.total_capital + self.daily_pnl
    
    async def predict_margin_requirement(self, legs: List[Dict]) -> float:
        """
        Call Broker Margin API or use ML predictor as fallback
        """
        # TODO: Implement actual Upstox margin API call
        # For now, using predictor
        
        total_margin = 0.0
        
        for leg in legs:
            strike = leg.get('strike', 0)
            spot = leg.get('spot', 21500)  # Should come from market data
            qty = leg.get('quantity', 0)
            side = leg.get('side', 'BUY')
            
            # Calculate DTE
            expiry = leg.get('expiry')
            if expiry:
                if isinstance(expiry, str):
                    try:
                        expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                    except:
                        expiry_date = date.today()
                else:
                    expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
                
                dte = (expiry_date - date.today()).days
            else:
                dte = 7  # Default assumption
            
            # Get IV from leg data or use default
            iv = leg.get('iv', 0.15)
            
            # Predict margin for this leg
            leg_margin = self.margin_predictor.predict(
                strike, spot, dte, iv, side, qty
            )
            total_margin += leg_margin
        
        return total_margin
    
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
            margin_source = "ML_PREDICTOR"
            logger.info(f"Margin requirement: ₹{required_margin:,.0f} (source: {margin_source})")
            
        except asyncio.TimeoutError:
            logger.error("⚠ CRITICAL: Margin prediction timeout")
            return MarginCheckResult(
                allowed=False,
                reason="Margin prediction timeout - cannot verify safety",
                required_margin=0.0,
                available_margin=available_funds
            )
        except Exception as e:
            logger.error(f"⚠ CRITICAL: Margin prediction failed: {e}")
            
            # Check environment
            current_env = str(settings.ENVIRONMENT).lower()
            if hasattr(settings.ENVIRONMENT, 'value'):
                current_env = str(settings.ENVIRONMENT.value).lower()
            
            if current_env in ['full_auto', 'production_live']:
                logger.critical("🛑 BLOCKING TRADE: Margin prediction unavailable in FULL_AUTO mode")
                return MarginCheckResult(
                    allowed=False,
                    reason="CRITICAL: Margin prediction unavailable in FULL_AUTO mode",
                    required_margin=0.0,
                    available_margin=available_funds
                )
            else:
                logger.error("⚠ Using conservative estimate in non-production mode")
                # Use most conservative estimate possible
                required_margin = sum(
                    self.margin_predictor._conservative_estimate(
                        leg.get('strike', 21500),
                        leg.get('spot', 21500),
                        7,  # Conservative DTE
                        leg.get('side', 'SELL'),
                        leg.get('quantity', 50)
                    ) for leg in legs
                )
                margin_source = "CONSERVATIVE_FALLBACK"
        
        # 4. Buffer: Keep 15% free always (increased from 10%)
        safe_margin_limit = available_funds * 0.85  # Use max 85% of available funds
        
        if required_margin > safe_margin_limit:
            self.failed_margin_calls += 1
            return MarginCheckResult(
                allowed=False,
                reason=f"Insufficient Margin (Req: ₹{required_margin:,.0f} | Avail: ₹{available_funds:,.0f} | Source: {margin_source})",
                required_margin=required_margin,
                available_margin=available_funds
            )
        
        # 5. Brokerage Check
        est_brokerage = await self.estimate_brokerage(legs)
        
        # 6. Log margin source for audit trail
        logger.info(f"✅ Margin check PASSED - Source: {margin_source}, Required: ₹{required_margin:,.0f}, Available: ₹{available_funds:,.0f}")
        
        return MarginCheckResult(
            allowed=True,
            reason=f"OK (margin_source={margin_source})",
            required_margin=required_margin,
            available_margin=available_funds,
            brokerage_estimate=est_brokerage
        )
    
    async def estimate_brokerage(self, legs: List[Dict]) -> float:
        """Estimate brokerage charges (approx ₹20 per order + taxes)"""
        num_orders = len(legs)
        return num_orders * 25.0  # Conservative estimate
    
    def record_actual_margin(self, required_margin: float, legs: List[Dict]):
        """
        Record actual margin after successful execution.
        This trains the margin predictor.
        """
        for leg in legs:
            strike = leg.get('strike', 0)
            spot = leg.get('spot', 21500)
            qty = leg.get('quantity', 0)
            side = leg.get('side', 'BUY')
            
            # Calculate DTE
            expiry = leg.get('expiry')
            if expiry:
                if isinstance(expiry, str):
                    try:
                        expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                    except:
                        expiry_date = date.today()
                else:
                    expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
                
                dte = (expiry_date - date.today()).days
            else:
                dte = 7
            
            iv = leg.get('iv', 0.15)
            
            # Record for this leg
            self.margin_predictor.record_actual_margin(
                margin=required_margin / len(legs),  # Split margin across legs
                strike=strike,
                spot=spot,
                dte=dte,
                iv=iv,
                side=side
            )
        
        logger.info(f"📊 Recorded actual margin for {len(legs)} legs")
    
    def update_pnl(self, realized_pnl: float):
        """Update daily PnL"""
        self.daily_pnl += realized_pnl
    
    def update_position_count(self, count: int):
        """Update position count"""
        self.position_count = count
    
    # Backward compatibility method for supervisor
    def record_actual_margin_legacy(self, required_margin: float, num_lots: int):
        """
        Legacy method for backward compatibility with supervisor.
        Converts to new legs format.
        """
        logger.warning(f"Using legacy margin recording method: {required_margin} for {num_lots} lots")
        # Create dummy leg structure for backward compatibility
        dummy_legs = [{
            'strike': 21500,
            'spot': 21500,
            'quantity': num_lots * 50,
            'side': 'SELL',  # Most conservative assumption
            'expiry': date.today().isoformat(),
            'iv': 0.15
        }]
        
        self.record_actual_margin(required_margin, dummy_legs)
