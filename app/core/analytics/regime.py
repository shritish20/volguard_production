from dataclasses import dataclass, field
from typing import List, Optional
from datetime import date
from app.core.analytics.volatility import VolMetrics
from app.core.analytics.structure import StructMetrics
from app.core.analytics.edge import EdgeMetrics
from app.core.market.participant_client import ExternalMetrics # We will build this next
from app.utils.logger import logger

@dataclass
class RegimeScore:
    vol_score: float
    struct_score: float
    edge_score: float
    risk_score: float
    composite: float
    confidence: str  # VERY_HIGH, HIGH, MODERATE, LOW

@dataclass
class TradingMandate:
    expiry_type: str
    regime_name: str        # AGGRESSIVE_SHORT, MODERATE_SHORT, DEFENSIVE, CASH
    strategy_type: str      # STRANGLE, IRON_CONDOR, IRON_FLY, CREDIT_SPREAD
    allocation_pct: float   # 0-100%
    rationale: List[str]
    warnings: List[str]

class RegimeEngine:
    """
    VolGuard 4.1 Regime Engine.
    The Central Brain. Integrates all signals to produce a Trading Mandate.
    """

    def __init__(self):
        # Weights from v30.1 Backtest
        self.W_VOL = 0.40
        self.W_STRUCT = 0.30
        self.W_EDGE = 0.20
        self.W_RISK = 0.10
        
        # Thresholds
        self.VOV_CRASH_ZSCORE = 2.5
        self.VOV_WARNING_ZSCORE = 2.0
        self.GAMMA_DANGER_DTE = 1

    def analyze_regime(self, 
                      vol: VolMetrics, 
                      struct: StructMetrics, 
                      edge: EdgeMetrics, 
                      ext: ExternalMetrics,
                      expiry_type: str,
                      dte: int) -> TradingMandate:
        try:
            # 1. Calculate Component Scores
            vol_score = self._score_volatility(vol)
            struct_score = self._score_structure(struct, expiry_type, dte)
            edge_score = edge.edge_score # Already calculated in EdgeEngine
            risk_score = self._score_risk(ext, expiry_type, dte)

            # 2. Composite Score
            composite = (
                (vol_score * self.W_VOL) +
                (struct_score * self.W_STRUCT) +
                (edge_score * self.W_EDGE) +
                (risk_score * self.W_RISK)
            )

            # 3. Determine Confidence
            if composite >= 8.0: confidence = "VERY_HIGH"
            elif composite >= 6.5: confidence = "HIGH"
            elif composite >= 4.0: confidence = "MODERATE"
            else: confidence = "LOW"

            score = RegimeScore(vol_score, struct_score, edge_score, risk_score, composite, confidence)

            # 4. Generate Mandate
            return self._generate_mandate(score, vol, struct, edge, ext, dte, expiry_type)

        except Exception as e:
            logger.error(f"Regime Analysis Failed: {str(e)}")
            return self._get_fallback_mandate(expiry_type)

    def _score_volatility(self, vol: VolMetrics) -> float:
        score = 5.0
        
        # The Holy Grail Filter (VoV Z-Score)
        if vol.vov_zscore > self.VOV_CRASH_ZSCORE:
            return 0.0 # KILL SWITCH
        elif vol.vov_zscore > self.VOV_WARNING_ZSCORE:
            score -= 3.0
        elif vol.vov_zscore < 1.5:
            score += 1.5 # Reward Stability
            
        # IV Percentile Context
        if vol.ivp_1yr > 75:
            score += 0.5 # Mean reversion potential
        elif vol.ivp_1yr < 25:
            score -= 2.5 # Too cheap to sell
        else:
            score += 1.0 # Sweet spot
            
        return max(0, min(10, score))

    def _score_structure(self, struct: StructMetrics, expiry_type: str, dte: int) -> float:
        score = 5.0
        
        # Sticky Markets are good for selling
        if struct.gex_regime == "STICKY":
            # Gamma week + Sticky = Safe(r)
            if expiry_type == "WEEKLY" and dte <= 1:
                score += 2.5 
            else:
                score += 1.0
        elif struct.gex_regime == "SLIPPERY":
            score -= 1.0
            
        # PCR Balance
        if 0.9 < struct.pcr < 1.1:
            score += 1.0
        elif struct.pcr > 1.3 or struct.pcr < 0.7:
            score -= 0.5 # Directional risk
            
        return max(0, min(10, score))

    def _score_risk(self, ext: ExternalMetrics, expiry_type: str, dte: int) -> float:
        score = 10.0
        
        # Event Risk
        if ext.event_risk == "HIGH": score -= 3.0
        elif ext.event_risk == "MEDIUM": score -= 1.5
        
        # FII Flow (The Live Fuse)
        if ext.flow_regime == "STRONG_SHORT":
            score -= 3.0
        elif ext.flow_regime == "STRONG_LONG":
            score += 1.0
            
        # Gamma Risk (Time-based)
        if expiry_type == "WEEKLY" and dte <= self.GAMMA_DANGER_DTE:
            score -= 2.0
            
        return max(0, min(10, score))

    def _generate_mandate(self, 
                         score: RegimeScore, 
                         vol: VolMetrics, 
                         struct: StructMetrics, 
                         edge: EdgeMetrics, 
                         ext: ExternalMetrics,
                         dte: int,
                         expiry_type: str) -> TradingMandate:
        
        rationale = []
        warnings = []
        
        # Strategy Selection Logic
        if score.composite >= 7.5:
            regime = "AGGRESSIVE_SHORT"
            strategy = "STRANGLE"
            alloc = 60.0
            rationale.append(f"High Confidence ({score.composite:.1f}): Volatility is rich & stable")
        elif score.composite >= 6.0:
            regime = "MODERATE_SHORT"
            strategy = "IRON_CONDOR" if dte > 1 else "IRON_FLY"
            alloc = 40.0
            rationale.append("Moderate Edge: Defined risk recommended")
        elif score.composite >= 4.0:
            regime = "DEFENSIVE"
            strategy = "CREDIT_SPREAD"
            alloc = 20.0
            rationale.append("Weak Edge: Tight stops required")
        else:
            regime = "CASH"
            strategy = "NONE"
            alloc = 0.0
            rationale.append("Regime Unfavorable: Cash is a position")

        # Specific Warning Injection
        if vol.vov_zscore > self.VOV_WARNING_ZSCORE:
            warnings.append(f"High Vol-of-Vol ({vol.vov_zscore:.1f}z)")
        if ext.flow_regime == "STRONG_SHORT":
            warnings.append("FII Heavy Selling Detected")
            alloc = min(alloc, 30.0) # Cap allocation
            
        return TradingMandate(
            expiry_type=expiry_type,
            regime_name=regime,
            strategy_type=strategy,
            allocation_pct=alloc,
            rationale=rationale,
            warnings=warnings
        )

    def _get_fallback_mandate(self, expiry_type: str) -> TradingMandate:
        return TradingMandate(
            expiry_type=expiry_type,
            regime_name="ERROR_FALLBACK",
            strategy_type="CASH",
            allocation_pct=0.0,
            rationale=["System Error during analysis"],
            warnings=["Check logs immediately"]
        )
