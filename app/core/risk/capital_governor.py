# app/core/risk/capital_governor.py

import asyncio
import logging
from typing import List, Dict, Optional, Union
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
            
    def record_simple_margin(self, margin: float, lots: int):
        """Fallback for when we only know total margin and lots (Legacy Supervisor support)"""
        # We synthesize a generic entry so the predictor has *some* data
        if lots <= 0: return
        self.historical_data.append({
            'margin_per_lot': margin / (lots * 50),
            'moneyness': 1.0, # Assumed ATM
            'dte': 7,         # Assumed Weekly
            'iv': 0.15,
            'side': 'SELL',   # Assume Sell as it dominates margin
            'timestamp': datetime.now()
        })
    
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
        
        lots = max(1, qty // 50)
        total_margin = margin_per_lot * lots * 50 * 1.10  # Add 10% safety buffer
        
        return total_margin
    
    def _conservative_estimate(self, strike: float, spot: float, 
                               dte: int, side: str, qty: int) -> float:
        """
        Conservative margin estimates based on exchange guidelines.
        Used when no historical data available.
        """
        lots = max(1, qty // 50)
        
        if side == "BUY":
            # Premium + 50% buffer (Updated from 20% for safety against Vega expansion)
            moneyness = abs(strike - spot) / spot
            estimated_premium = spot * 0.03 * (1 - moneyness)  # Rough estimate
            return estimated_premium * qty * 1.50
        
        # SELL side - actual margin calculation
        moneyness = strike / spot
        
        # ATM/ITM options have higher margin
        if 0.95 <= moneyness <= 1.05:  # ATM ±5%
            if dte <= 2:  # Expiry week
                margin_per_lot = 280000.0  # Conservative expiry margin
            else:
                margin_per_lot = 220000.0  # ATM normal margin
        elif moneyness < 0.90 or moneyness > 1.10:  # Deep OTM
            margin_per_lot = 150000.0
        else:  # Slightly OTM
            margin_per_lot = 180000.0
        
        # Expiry day multiplier
        if dte == 0:
            margin_per_lot *= 1.5
        
        total = margin_per_lot * lots * 1.15  # 15% safety buffer
        
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
        # In a real impl, this calls the broker. 
        # For now, we simulate based on Config + PnL
        return self.total_capital + self.daily_pnl
    
    async def predict_margin_requirement(self, legs: List[Dict]) -> float:
        """
        Call Broker Margin API or use ML predictor as fallback
        """
        total_margin = 0.0
        
        for leg in legs:
            strike = leg.get('strike', 21500)
            spot = leg.get('spot', 21500)  
            qty = leg.get('quantity', 50)
            side = leg.get('side', 'BUY')
            
            # Calculate DTE
            expiry = leg.get('expiry')
            dte = 7 # Default
            if expiry:
                try:
                    if isinstance(expiry, str):
                        expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                    elif hasattr(expiry, 'date'):
                        expiry_date = expiry.date()
                    else:
                        expiry_date = expiry
                    
                    if isinstance(expiry_date, date):
                        dte = (expiry_date - date.today()).days
                except:
                    pass
            
            # Get IV from leg data or use default
            iv = leg.get('iv', 0.15)
            
            # Predict margin for this leg
            leg_margin = self.margin_predictor.predict(
                float(strike), float(spot), int(dte), float(iv), side, int(qty)
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
        
        # 3. Predict Margin - CRITICAL SECTION
        margin_source = "UNKNOWN"
        try:
            required_margin = await asyncio.wait_for(
                self.predict_margin_requirement(legs),
                timeout=5.0
            )
            margin_source = "ML_PREDICTOR"
            
        except asyncio.TimeoutError:
            logger.error("⚠ CRITICAL: Margin prediction timeout")
            return MarginCheckResult(False, "Margin prediction timeout", 0.0, available_funds)
            
        except Exception as e:
            logger.error(f"⚠ CRITICAL: Margin prediction failed: {e}")
            
            # Check environment
            current_env = str(settings.ENVIRONMENT).lower()
            if 'full_auto' in current_env or 'production' in current_env:
                logger.critical("🛑 BLOCKING TRADE: Margin prediction unavailable in FULL_AUTO")
                return MarginCheckResult(False, "CRITICAL: Margin prediction failed", 0.0, available_funds)
            else:
                # Fallback for SHADOW mode
                required_margin = 200000.0 * (len(legs)) # Rough fallback
                margin_source = "EMERGENCY_FALLBACK"
        
        # 4. Buffer: Keep 15% free always
        safe_margin_limit = available_funds * 0.85
        
        if required_margin > safe_margin_limit:
            self.failed_margin_calls += 1
            return MarginCheckResult(
                allowed=False,
                reason=f"Insufficient Margin (Req: ₹{required_margin:,.0f} | Limit: ₹{safe_margin_limit:,.0f})",
                required_margin=required_margin,
                available_margin=available_funds
            )
        
        # 5. Brokerage Check
        est_brokerage = len(legs) * 25.0
        
        return MarginCheckResult(
            allowed=True,
            reason=f"OK (source={margin_source})",
            required_margin=required_margin,
            available_margin=available_funds,
            brokerage_estimate=est_brokerage
        )
    
    def record_actual_margin(self, arg1, arg2):
        """
        Hybrid method to handle calls from both Supervisor (legacy) and Executor (detailed).
        Supervisor calls: record_actual_margin(margin: float, lots: int)
        Executor calls:   record_actual_margin(margin: float, legs: List[Dict])
        """
        try:
            margin = float(arg1)
            
            if isinstance(arg2, int):
                # Called by Supervisor with 'lots'
                self.margin_predictor.record_simple_margin(margin, arg2)
            elif isinstance(arg2, list):
                # Called by Executor with 'legs'
                self._record_detailed(margin, arg2)
            else:
                logger.warning(f"Unknown arguments for record_actual_margin: {type(arg2)}")
                
        except Exception as e:
            logger.error(f"Failed to record margin: {e}")

    def _record_detailed(self, required_margin: float, legs: List[Dict]):
        """Internal helper for detailed recording"""
        for leg in legs:
            strike = leg.get('strike', 0)
            spot = leg.get('spot', 21500)
            qty = leg.get('quantity', 0)
            side = leg.get('side', 'BUY')
            iv = leg.get('iv', 0.15)
            
            # Calculate DTE
            dte = 7
            expiry = leg.get('expiry')
            if expiry:
                 try:
                    if isinstance(expiry, str):
                        ed = datetime.strptime(expiry, "%Y-%m-%d").date()
                    elif hasattr(expiry, 'date'):
                        ed = expiry.date()
                    else:
                        ed = expiry
                    if isinstance(ed, date):
                        dte = (ed - date.today()).days
                 except: pass

            self.margin_predictor.record_actual_margin(
                margin=required_margin / max(1, len(legs)),
                strike=float(strike),
                spot=float(spot),
                dte=int(dte),
                iv=float(iv),
                side=side
            )
    
    def update_pnl(self, realized_pnl: float):
        """Update daily PnL"""
        self.daily_pnl += realized_pnl
    
    def update_position_count(self, count: int):
        """Update position count"""
        self.position_count = count
