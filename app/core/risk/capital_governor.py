# app/core/risk/capital_governor.py

import asyncio
import logging
import httpx
import numpy as np
from typing import List, Dict, Optional, Union, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass
import json

from app.core.risk.schemas import MarginCheckResult
from app.config import settings

logger = logging.getLogger(__name__)

# ==== DATA STRUCTURES ====
@dataclass
class MarginRecord:
    """Detailed margin record for ML training"""
    timestamp: datetime
    margin_per_lot: float
    strike: float
    spot: float
    dte: int
    iv: float
    side: str
    option_type: str
    moneyness: float
    strategy_type: str
    actual_vs_predicted: float  # Ratio of actual/predicted
    broker_reported: Optional[float] = None


class MarginPredictor:
    """
    Enhanced Machine learning-based margin predictor with audit capabilities
    
    NEW FEATURES:
    1. Margin drift detection
    2. Broker vs internal comparison
    3. Historical accuracy tracking
    4. Conservative fallbacks with dynamic buffers
    """
    
    def __init__(self, min_samples: int = 10, max_samples: int = 1000):
        self.historical_data: List[MarginRecord] = []
        self.min_samples = min_samples
        self.max_samples = max_samples
        
        # Accuracy tracking
        self.prediction_errors: List[float] = []
        self.avg_error = 0.0
        self.error_std = 0.0
        
        # Drift detection
        self.last_audit_result: Optional[Dict] = None
        self.consecutive_drift_detected = 0
        
    def record_actual_margin(self, margin: float, strike: float, spot: float, 
                            dte: int, iv: float, side: str, option_type: str = "CE",
                            strategy_type: str = "UNKNOWN", predicted_margin: Optional[float] = None,
                            broker_reported: Optional[float] = None):
        """
        Record actual margin from successful trade with comprehensive metadata
        
        Args:
            margin: Actual margin charged
            strike: Strike price
            spot: Spot price at trade time
            dte: Days to expiry
            iv: Implied volatility
            side: BUY or SELL
            option_type: CE or PE
            strategy_type: Strategy name
            predicted_margin: Our predicted margin (for accuracy tracking)
            broker_reported: Broker's reported margin (for audit)
        """
        moneyness = strike / spot if spot > 0 else 1.0
        
        # Calculate accuracy if prediction available
        actual_vs_predicted = 1.0
        if predicted_margin and predicted_margin > 0:
            actual_vs_predicted = margin / predicted_margin
            self._update_accuracy_stats(actual_vs_predicted)
        
        record = MarginRecord(
            timestamp=datetime.now(),
            margin_per_lot=margin / 50,  # Normalize to per lot
            strike=strike,
            spot=spot,
            dte=dte,
            iv=iv,
            side=side,
            option_type=option_type,
            moneyness=moneyness,
            strategy_type=strategy_type,
            actual_vs_predicted=actual_vs_predicted,
            broker_reported=broker_reported
        )
        
        self.historical_data.append(record)
        
        # Maintain sample limit
        if len(self.historical_data) > self.max_samples:
            self.historical_data = self.historical_data[-self.max_samples:]
            
        # Log if significant prediction error
        if abs(actual_vs_predicted - 1.0) > 0.2:  # >20% error
            logger.warning(f"Margin prediction error: {actual_vs_predicted:.2%} "
                          f"(Predicted: {predicted_margin:,.0f}, Actual: {margin:,.0f})")
    
    def record_simple_margin(self, margin: float, lots: int):
        """
        Fallback for when we only know total margin and lots (Legacy Supervisor support)
        
        Args:
            margin: Total margin charged
            lots: Number of lots
        """
        if lots <= 0:
            return
            
        # Create synthetic record
        synthetic_margin_per_lot = margin / (lots * 50)
        
        record = MarginRecord(
            timestamp=datetime.now(),
            margin_per_lot=synthetic_margin_per_lot,
            strike=21500.0,  # Assumed
            spot=21500.0,    # Assumed
            dte=7,           # Assumed weekly
            iv=0.15,
            side='SELL',     # Most common for margin
            option_type='CE',
            moneyness=1.0,
            strategy_type='LEGACY',
            actual_vs_predicted=1.0
        )
        
        self.historical_data.append(record)
        
        # Maintain sample limit
        if len(self.historical_data) > self.max_samples:
            self.historical_data = self.historical_data[-self.max_samples:]
    
    def predict(self, strike: float, spot: float, dte: int, 
                iv: float, side: str, qty: int, option_type: str = "CE",
                strategy_type: str = "UNKNOWN", use_conservative: bool = False) -> Tuple[float, Dict]:
        """
        Predict margin requirement with confidence metrics
        
        Returns:
            Tuple of (predicted_margin, confidence_metrics)
        """
        lots = max(1, qty // 50)
        confidence = "HIGH"
        
        # If we have high error rate or drift, use conservative
        if use_conservative or self.avg_error > 0.3 or self.consecutive_drift_detected > 2:
            margin_per_lot = self._conservative_estimate_base(strike, spot, dte, side)
            confidence = "LOW_DRIFT_DETECTED" if self.consecutive_drift_detected > 2 else "LOW_HIGH_ERROR"
        elif len(self.historical_data) < self.min_samples:
            margin_per_lot = self._conservative_estimate_base(strike, spot, dte, side)
            confidence = "LOW_INSUFFICIENT_DATA"
        else:
            # Calculate features
            moneyness = strike / spot if spot > 0 else 1.0
            
            # Filter similar trades
            similar = self._find_similar_trades(moneyness, dte, side, option_type, strategy_type)
            
            if len(similar) < 3:
                # Not enough similar trades
                similar_sides = [d for d in self.historical_data if d.side == side]
                
                if not similar_sides:
                    margin_per_lot = self._conservative_estimate_base(strike, spot, dte, side)
                    confidence = "MEDIUM_NO_SIMILAR_TRADES"
                else:
                    # Use 95th percentile (conservative)
                    margins = [d.margin_per_lot for d in similar_sides]
                    margin_per_lot = np.percentile(margins, 95)
                    confidence = "MEDIUM_USING_ALL_DATA"
            else:
                # Use mean of similar trades with dynamic buffer based on error
                margins = [d.margin_per_lot for d in similar]
                base_margin = np.mean(margins)
                
                # Dynamic buffer based on prediction accuracy
                error_buffer = max(1.0, 1.0 + self.avg_error)
                margin_per_lot = base_margin * error_buffer
                confidence = "HIGH_SIMILAR_TRADES"
        
        # Apply DTE adjustments
        margin_per_lot = self._apply_dte_adjustment(margin_per_lot, dte)
        
        # Calculate total with safety buffer
        safety_buffer = 1.10  # 10% safety buffer
        total_margin = margin_per_lot * lots * 50 * safety_buffer
        
        # Confidence metrics
        confidence_metrics = {
            "confidence_level": confidence,
            "sample_count": len(self.historical_data),
            "similar_trades_count": len(self._find_similar_trades(strike/spot, dte, side, option_type, strategy_type)),
            "avg_prediction_error": self.avg_error,
            "error_std": self.error_std,
            "conservative_used": confidence.startswith("LOW"),
            "safety_buffer_pct": (safety_buffer - 1.0) * 100
        }
        
        return total_margin, confidence_metrics
    
    def _find_similar_trades(self, moneyness: float, dte: int, side: str, 
                            option_type: str, strategy_type: str) -> List[MarginRecord]:
        """Find historically similar trades"""
        similar = []
        
        for record in self.historical_data:
            # Match criteria
            moneyness_match = abs(record.moneyness - moneyness) < 0.05  # Within 5%
            dte_match = abs(record.dte - dte) < 7  # Within 1 week
            side_match = record.side == side
            option_match = record.option_type == option_type
            strategy_match = record.strategy_type == strategy_type or strategy_type == "UNKNOWN"
            
            if moneyness_match and dte_match and side_match and option_match and strategy_match:
                similar.append(record)
        
        return similar
    
    def _conservative_estimate_base(self, strike: float, spot: float, 
                                   dte: int, side: str) -> float:
        """
        Base conservative margin estimate
        """
        # SELL side - higher margin requirements
        if side == "SELL":
            moneyness = strike / spot if spot > 0 else 1.0
            
            if 0.95 <= moneyness <= 1.05:  # ATM ±5%
                if dte <= 2:  # Expiry week
                    return 280000.0  # Conservative expiry margin
                else:
                    return 220000.0  # ATM normal margin
            elif moneyness < 0.90 or moneyness > 1.10:  # Deep OTM
                return 150000.0
            else:  # Slightly OTM
                return 180000.0
        else:
            # BUY side - premium based
            moneyness = abs(strike - spot) / spot if spot > 0 else 0.1
            estimated_premium = spot * 0.03 * (1 - moneyness)
            return estimated_premium * 50 * 1.5  # Per lot with buffer
    
    def _apply_dte_adjustment(self, base_margin: float, dte: int) -> float:
        """Apply days-to-expiry adjustments"""
        if dte == 0:  # Expiry day
            return base_margin * 1.5
        elif dte <= 2:  # Expiry week
            return base_margin * 1.25
        elif dte <= 7:  # Weekly expiry
            return base_margin * 1.1
        else:
            return base_margin
    
    def _update_accuracy_stats(self, actual_vs_predicted: float):
        """Update accuracy tracking statistics"""
        self.prediction_errors.append(actual_vs_predicted)
        
        # Keep only recent errors
        if len(self.prediction_errors) > 100:
            self.prediction_errors = self.prediction_errors[-100:]
        
        # Update statistics
        if self.prediction_errors:
            self.avg_error = np.mean([abs(e - 1.0) for e in self.prediction_errors])
            self.error_std = np.std([abs(e - 1.0) for e in self.prediction_errors]) if len(self.prediction_errors) > 1 else 0.0
    
    def get_accuracy_report(self) -> Dict:
        """Get margin prediction accuracy report"""
        recent_records = [r for r in self.historical_data 
                         if (datetime.now() - r.timestamp) < timedelta(days=30)]
        
        return {
            "total_samples": len(self.historical_data),
            "recent_samples": len(recent_records),
            "avg_prediction_error": self.avg_error,
            "error_std": self.error_std,
            "consecutive_drift_detected": self.consecutive_drift_detected,
            "last_audit_result": self.last_audit_result,
            "confidence_level": "HIGH" if self.avg_error < 0.1 else "MEDIUM" if self.avg_error < 0.2 else "LOW"
        }


class CapitalGovernor:
    """
    Enhanced Capital Governor with Margin Audit Capability
    
    NEW FEATURES:
    1. 🔄 Margin Audit: Broker vs Internal comparison
    2. 🚨 Drift Detection: Automatic emergency stop on mismatch
    3. 📊 Enhanced ML Predictor: With accuracy tracking
    4. 🔐 Token-based API calls: For broker margin verification
    """
    
    def __init__(self, token_manager, total_capital: float, 
                 max_daily_loss: float = 5000.0, max_positions: int = 4):
        """
        Args:
            token_manager: TokenManager for authenticated API calls
            total_capital: Total trading capital
            max_daily_loss: Maximum daily loss limit
            max_positions: Maximum concurrent positions
        """
        self.token_manager = token_manager
        self.total_capital = total_capital
        self.max_daily_loss = max_daily_loss
        self.max_positions = max_positions
        
        # State tracking
        self.daily_pnl = 0.0
        self.position_count = 0
        self.failed_margin_calls = 0
        
        # Enhanced Margin predictor with audit capabilities
        self.margin_predictor = MarginPredictor()
        
        # Local margin tracker (simplified)
        self.local_tracker = LocalMarginTracker()
        
        # Audit history
        self.audit_history: List[Dict] = []
        self.last_audit_time: Optional[datetime] = None
        
        # Emergency triggers
        self.margin_drift_threshold_pct = 5.0  # 5% drift threshold
        self.consecutive_drift_count = 0
        
        # API timeouts
        self.broker_api_timeout = 10.0
        self.margin_check_timeout = 15.0

    async def audit_margin_integrity(self) -> Dict:
        """
        🔄 MARGIN AUDIT: Compare broker-reported margin with internal tracking
        
        Returns:
            Audit result with drift detection
        """
        logger.info("💰 Performing margin integrity audit...")
        
        try:
            # 1. Get broker-reported margin
            broker_margin = await self._get_broker_margin()
            
            # 2. Get internal tracking
            internal_margin = self.local_tracker.get_available()
            
            # 3. Calculate drift
            if broker_margin > 0 and internal_margin > 0:
                drift_amount = broker_margin - internal_margin
                drift_pct = abs(drift_amount) / broker_margin * 100
                
                audit_result = {
                    "timestamp": datetime.now().isoformat(),
                    "broker_margin": round(broker_margin, 2),
                    "internal_margin": round(internal_margin, 2),
                    "drift_amount": round(drift_amount, 2),
                    "drift_pct": round(drift_pct, 2),
                    "within_threshold": drift_pct <= self.margin_drift_threshold_pct,
                    "threshold_pct": self.margin_drift_threshold_pct,
                    "status": "PASS" if drift_pct <= self.margin_drift_threshold_pct else "FAIL"
                }
                
                # 4. Handle drift detection
                if drift_pct > self.margin_drift_threshold_pct:
                    self.consecutive_drift_count += 1
                    logger.critical(f"🚨 MARGIN DRIFT DETECTED: {drift_pct:.1f}% "
                                   f"(Broker: ₹{broker_margin:,.0f}, Internal: ₹{internal_margin:,.0f})")
                    
                    # Update predictor's drift tracking
                    self.margin_predictor.consecutive_drift_detected = self.consecutive_drift_count
                    self.margin_predictor.last_audit_result = audit_result
                    
                    # Emergency escalation
                    if self.consecutive_drift_count >= 3:
                        logger.critical("🔴 CRITICAL: Consecutive margin drifts detected!")
                        audit_result["emergency_level"] = "CRITICAL"
                    elif self.consecutive_drift_count >= 2:
                        audit_result["emergency_level"] = "HIGH"
                    else:
                        audit_result["emergency_level"] = "MEDIUM"
                        
                else:
                    # Reset drift counter on clean audit
                    self.consecutive_drift_count = 0
                    audit_result["emergency_level"] = "NONE"
                    logger.info(f"✅ Margin audit clean: {drift_pct:.1f}% drift")
                
                # 5. Store audit result
                self.audit_history.append(audit_result)
                self.last_audit_time = datetime.now()
                
                # Keep only last 100 audits
                if len(self.audit_history) > 100:
                    self.audit_history = self.audit_history[-100:]
                
                return audit_result
                
            else:
                logger.warning("Margin audit skipped: Invalid margin values")
                return {"status": "SKIPPED", "reason": "Invalid margin values"}
                
        except Exception as e:
            logger.error(f"Margin audit failed: {e}")
            return {"status": "ERROR", "error": str(e)}

    async def _get_broker_margin(self) -> float:
        """
        Get margin information from broker API
        
        Returns:
            Available margin as reported by broker
        """
        try:
            headers = self.token_manager.get_headers()
            
            async with httpx.AsyncClient(timeout=self.broker_api_timeout) as client:
                # Upstox v2 API for funds and margin
                response = await client.get(
                    "https://api.upstox.com/v2/user/get-funds-and-margin",
                    params={"segment": "SEC"},
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract available margin
                    equity_data = data.get('data', {}).get('equity', {})
                    available_margin = equity_data.get('available_margin', 0.0)
                    
                    if isinstance(available_margin, str):
                        return float(available_margin)
                    return available_margin
                    
                else:
                    logger.error(f"Broker margin API error: {response.status_code}")
                    return 0.0
                    
        except httpx.TimeoutException:
            logger.error("Broker margin API timeout")
            return 0.0
        except Exception as e:
            logger.error(f"Broker margin fetch failed: {e}")
            return 0.0

    async def get_available_funds(self) -> float:
        """
        Get available funds from broker API with fallback
        
        Returns:
            Available funds for trading
        """
        try:
            broker_margin = await self._get_broker_margin()
            
            # Update local tracker
            self.local_tracker.update_available(broker_margin)
            
            return broker_margin
            
        except Exception as e:
            logger.error(f"Available funds fetch failed, using local tracker: {e}")
            return self.local_tracker.get_available()
    
    async def predict_margin_requirement(self, legs: List[Dict]) -> Tuple[float, Dict]:
        """
        Enhanced margin prediction with confidence metrics
        
        Returns:
            Tuple of (predicted_margin, confidence_metrics)
        """
        total_margin = 0.0
        all_confidence = []
        
        for leg in legs:
            strike = leg.get('strike', 21500.0)
            spot = leg.get('spot', 21500.0)  
            qty = leg.get('quantity', 50)
            side = leg.get('side', 'BUY')
            option_type = leg.get('option_type', 'CE')
            strategy_type = leg.get('strategy', 'UNKNOWN')
            
            # Calculate DTE
            expiry = leg.get('expiry')
            dte = 7  # Default
            
            if expiry:
                try:
                    if isinstance(expiry, str):
                        expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                    elif hasattr(expiry, 'date'):
                        expiry_date = expiry.date()
                    else:
                        expiry_date = expiry
                    
                    if isinstance(expiry_date, date):
                        dte = max(0, (expiry_date - date.today()).days)
                except Exception as e:
                    logger.debug(f"DTE calculation failed: {e}")
            
            # Get IV from leg data or use default
            iv = leg.get('iv', 0.15)
            
            # Check if we should use conservative mode
            use_conservative = (
                self.consecutive_drift_count > 0 or
                self.margin_predictor.avg_error > 0.25
            )
            
            # Predict margin for this leg
            leg_margin, confidence = self.margin_predictor.predict(
                float(strike), float(spot), int(dte), float(iv), 
                side, int(qty), option_type, strategy_type, use_conservative
            )
            
            total_margin += leg_margin
            all_confidence.append(confidence)
        
        # Combine confidence metrics
        combined_confidence = {
            "total_predicted_margin": total_margin,
            "leg_count": len(legs),
            "lowest_confidence": min([c.get("confidence_level", "UNKNOWN") for c in all_confidence], 
                                     key=lambda x: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get(x, 3)),
            "use_conservative": any("LOW" in c.get("confidence_level", "") for c in all_confidence),
            "avg_prediction_error": self.margin_predictor.avg_error,
            "consecutive_drift_count": self.consecutive_drift_count
        }
        
        return total_margin, combined_confidence
    
    async def can_trade_new(self, legs: List[Dict], strategy_name: str = "MANUAL") -> MarginCheckResult:
        """
        Master decision function with enhanced margin validation
        
        NEW: Includes drift-aware margin checks
        """
        # 1. Internal Safety Checks
        if self.daily_pnl <= -abs(self.max_daily_loss):
            return MarginCheckResult(
                allowed=False, 
                reason=f"Max Daily Loss Reached (₹{self.daily_pnl:,.0f})",
                emergency_level="HIGH"
            )
        
        if self.position_count >= self.max_positions:
            is_exit = any(l.get("action") in ["EXIT", "CLOSE"] for l in legs)
            if not is_exit:
                return MarginCheckResult(
                    allowed=False, 
                    reason=f"Max Position Count Reached ({self.position_count}/{self.max_positions})",
                    emergency_level="MEDIUM"
                )
        
        # 2. Get Real Money (with broker verification)
        try:
            available_funds = await asyncio.wait_for(
                self.get_available_funds(),
                timeout=self.margin_check_timeout
            )
        except asyncio.TimeoutError:
            logger.error("Funds fetch timeout - using conservative check")
            # In timeout, we assume worst-case
            available_funds = self.total_capital * 0.5
        
        # 3. Predict Margin with confidence metrics
        margin_source = "ML_PREDICTOR"
        confidence_metrics = {}
        
        try:
            required_margin, confidence_metrics = await asyncio.wait_for(
                self.predict_margin_requirement(legs),
                timeout=self.margin_check_timeout
            )
            
            # If we have drift or low confidence, be extra conservative
            if (self.consecutive_drift_count > 0 or 
                confidence_metrics.get("lowest_confidence", "").startswith("LOW")):
                required_margin *= 1.25  # Add 25% buffer
                margin_source = "CONSERVATIVE_DRIFT_AWARE"
                
        except asyncio.TimeoutError:
            logger.error("⚠️ Margin prediction timeout")
            return MarginCheckResult(
                allowed=False, 
                reason="Margin prediction timeout",
                required_margin=0.0,
                available_margin=available_funds,
                emergency_level="MEDIUM"
            )
        except Exception as e:
            logger.error(f"⚠️ Margin prediction failed: {e}")
            
            # Environment-aware fallback
            if settings.ENVIRONMENT in ["PRODUCTION", "FULL_AUTO"]:
                logger.critical("🛑 BLOCKING TRADE: Margin prediction unavailable in production")
                return MarginCheckResult(
                    allowed=False, 
                    reason="CRITICAL: Margin prediction failed in production",
                    emergency_level="CRITICAL"
                )
            else:
                # Conservative fallback for non-production
                required_margin = 200000.0 * len(legs)
                margin_source = "EMERGENCY_FALLBACK"
                confidence_metrics = {"emergency_fallback": True}
        
        # 4. Buffer: Keep dynamic buffer based on confidence
        buffer_pct = 0.15  # Default 15%
        if confidence_metrics.get("lowest_confidence", "").startswith("LOW"):
            buffer_pct = 0.25  # 25% buffer for low confidence
        elif self.consecutive_drift_count > 0:
            buffer_pct = 0.30  # 30% buffer when drift detected
            
        safe_margin_limit = available_funds * (1 - buffer_pct)
        
        if required_margin > safe_margin_limit:
            self.failed_margin_calls += 1
            
            # Check if this is critical
            is_critical = (
                required_margin > available_funds or  # Would exceed total
                self.failed_margin_calls > 3 or       # Repeated failures
                self.consecutive_drift_count > 0      # Drift context
            )
            
            emergency_level = "CRITICAL" if is_critical else "HIGH"
            
            return MarginCheckResult(
                allowed=False,
                reason=f"Insufficient Margin (Req: ₹{required_margin:,.0f} | Limit: ₹{safe_margin_limit:,.0f})",
                required_margin=required_margin,
                available_margin=available_funds,
                emergency_level=emergency_level,
                confidence_metrics=confidence_metrics
            )
        
        # 5. All checks passed
        brokerage_estimate = len(legs) * 25.0  # Simplified
        
        return MarginCheckResult(
            allowed=True,
            reason=f"OK (source={margin_source}, confidence={confidence_metrics.get('lowest_confidence', 'UNKNOWN')})",
            required_margin=required_margin,
            available_margin=available_funds,
            brokerage_estimate=brokerage_estimate,
            confidence_metrics=confidence_metrics
        )
    
    def record_actual_margin(self, arg1, arg2, **kwargs):
        """
        Enhanced margin recording with audit support
        
        Supports multiple calling patterns:
        1. Supervisor (legacy): record_actual_margin(margin: float, lots: int)
        2. Executor (detailed): record_actual_margin(margin: float, legs: List[Dict])
        3. Enhanced: record_actual_margin(margin: float, legs: List[Dict], broker_reported: float)
        """
        try:
            margin = float(arg1)
            
            if isinstance(arg2, int):
                # Legacy Supervisor call with 'lots'
                self.margin_predictor.record_simple_margin(margin, arg2)
                
            elif isinstance(arg2, list):
                # Executor call with 'legs' list
                broker_reported = kwargs.get('broker_reported')
                
                for leg in arg2:
                    strike = leg.get('strike', 0.0)
                    spot = leg.get('spot', 21500.0)
                    qty = leg.get('quantity', 0)
                    side = leg.get('side', 'BUY')
                    option_type = leg.get('option_type', 'CE')
                    strategy_type = leg.get('strategy', 'UNKNOWN')
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
                                dte = max(0, (ed - date.today()).days)
                        except Exception:
                            pass
                    
                    # Get predicted margin for accuracy tracking
                    predicted_margin = None
                    if 'predicted_margin' in kwargs:
                        predicted_margin = kwargs['predicted_margin']
                    
                    self.margin_predictor.record_actual_margin(
                        margin=margin / max(1, len(arg2)),
                        strike=float(strike),
                        spot=float(spot),
                        dte=int(dte),
                        iv=float(iv),
                        side=side,
                        option_type=option_type,
                        strategy_type=strategy_type,
                        predicted_margin=predicted_margin,
                        broker_reported=broker_reported
                    )
            else:
                logger.warning(f"Unknown arguments for record_actual_margin: {type(arg2)}")
                
        except Exception as e:
            logger.error(f"Failed to record margin: {e}")
    
    def update_pnl(self, realized_pnl: float):
        """Update daily PnL and local tracker"""
        self.daily_pnl += realized_pnl
        self.local_tracker.update_pnl(realized_pnl)
    
    def update_position_count(self, count: int):
        """Update position count"""
        self.position_count = count
    
    def get_margin_health_report(self) -> Dict:
        """Get comprehensive margin health report"""
        accuracy_report = self.margin_predictor.get_accuracy_report()
        
        recent_audits = [a for a in self.audit_history 
                        if (datetime.now() - datetime.fromisoformat(a["timestamp"])) < timedelta(hours=24)]
        
        return {
            "timestamp": datetime.now().isoformat(),
            "margin_accuracy": accuracy_report,
            "local_tracker": self.local_tracker.get_status(),
            "recent_audits_24h": len(recent_audits),
            "failed_margin_calls": self.failed_margin_calls,
            "consecutive_drift_count": self.consecutive_drift_count,
            "drift_threshold_pct": self.margin_drift_threshold_pct,
            "daily_pnl": self.daily_pnl,
            "position_count": self.position_count,
            "max_daily_loss": self.max_daily_loss,
            "max_positions": self.max_positions,
            "health_status": (
                "HEALTHY" if (accuracy_report["confidence_level"] == "HIGH" and 
                             self.consecutive_drift_count == 0 and 
                             self.failed_margin_calls < 3) 
                else "DEGRADED" if (accuracy_report["confidence_level"] == "MEDIUM" or 
                                   self.consecutive_drift_count > 0) 
                else "CRITICAL"
            )
        }


class LocalMarginTracker:
    """
    Simplified local margin tracker for internal accounting
    """
    def __init__(self, initial_capital: float = 1000000.0):
        self.available_margin = initial_capital
        self.initial_capital = initial_capital
        self.daily_pnl = 0.0
        self.margin_used = 0.0
    
    def update_available(self, new_available: float):
        """Update available margin (from broker)"""
        self.available_margin = new_available
    
    def get_available(self) -> float:
        """Get available margin"""
        return self.available_margin
    
    def update_pnl(self, pnl_delta: float):
        """Update PnL"""
        self.daily_pnl += pnl_delta
        self.available_margin += pnl_delta
    
    def record_margin_used(self, margin: float):
        """Record margin usage"""
        self.margin_used += margin
    
    def get_status(self) -> Dict:
        """Get tracker status"""
        return {
            "available_margin": self.available_margin,
            "daily_pnl": self.daily_pnl,
            "margin_used": self.margin_used,
            "initial_capital": self.initial_capital
                                      }
