from dataclasses import dataclass
import pandas as pd
import numpy as np
from app.core.analytics.volatility import VolMetrics
from app.utils.logger import logger

@dataclass
class EdgeMetrics:
    # --- Weekly Edge (Gamma Focus) ---
    iv_weekly: float
    vrp_weighted_weekly: float  # The 70/15/15 Blend
    vrp_garch_weekly: float
    vrp_park_weekly: float
    vrp_rv_weekly: float
    
    # --- Monthly Edge (Vega Focus) ---
    iv_monthly: float
    vrp_weighted_monthly: float # The 70/15/15 Blend
    vrp_garch_monthly: float
    vrp_park_monthly: float
    vrp_rv_monthly: float

    # --- Term Structure ---
    term_spread: float          # Monthly IV - Weekly IV
    term_regime: str            # BACKWARDATION / CONTANGO / FLAT

    # --- Classification ---
    primary_edge: str           # SHORT_GAMMA, SHORT_VEGA, LONG_VOL, etc.
    edge_score: float           # 0-10 Score for the Regime Engine

class EdgeEngine:
    """
    VolGuard 4.1 Edge Engine.
    Implements Weighted VRP (70% GARCH | 15% Park | 15% RV) logic.
    """

    def calculate_edge(self, 
                       vol: VolMetrics, 
                       weekly_chain: pd.DataFrame, 
                       monthly_chain: pd.DataFrame) -> EdgeMetrics:
        try:
            # 1. Extract ATM Implied Volatility
            iv_weekly = self._get_atm_iv(weekly_chain, vol.spot)
            iv_monthly = self._get_atm_iv(monthly_chain, vol.spot)

            # 2. Calculate Raw VRP Components (IV - Realized Metric)
            # Weekly (Gamma Horizon -> 7 Days)
            vrp_rv_w = iv_weekly - vol.rv7
            vrp_ga_w = iv_weekly - vol.garch7
            vrp_pk_w = iv_weekly - vol.park7
            
            # Monthly (Vega Horizon -> 28 Days)
            vrp_rv_m = iv_monthly - vol.rv28
            vrp_ga_m = iv_monthly - vol.garch28
            vrp_pk_m = iv_monthly - vol.park28

            # 3. THE "WEIGHTED VRP" FORMULA (70% Garch / 15% Park / 15% RV)
            # This smooths out model error as per your backtest.
            weighted_vrp_weekly = (vrp_ga_w * 0.70) + (vrp_pk_w * 0.15) + (vrp_rv_w * 0.15)
            weighted_vrp_monthly = (vrp_ga_m * 0.70) + (vrp_pk_m * 0.15) + (vrp_rv_m * 0.15)

            # 4. Term Structure Analysis
            term_spread = iv_monthly - iv_weekly
            if term_spread < -1.0:
                term_regime = "BACKWARDATION"
            elif term_spread > 1.0:
                term_regime = "CONTANGO"
            else:
                term_regime = "FLAT"

            # 5. Determine Primary Edge & Score
            primary_edge, edge_score = self._classify_edge(
                vol, weighted_vrp_weekly, weighted_vrp_monthly, term_regime, term_spread
            )

            return EdgeMetrics(
                iv_weekly=iv_weekly,
                vrp_weighted_weekly=weighted_vrp_weekly,
                vrp_garch_weekly=vrp_ga_w,
                vrp_park_weekly=vrp_pk_w,
                vrp_rv_weekly=vrp_rv_w,
                
                iv_monthly=iv_monthly,
                vrp_weighted_monthly=weighted_vrp_monthly,
                vrp_garch_monthly=vrp_ga_m,
                vrp_park_monthly=vrp_pk_m,
                vrp_rv_monthly=vrp_rv_m,
                
                term_spread=term_spread,
                term_regime=term_regime,
                primary_edge=primary_edge,
                edge_score=edge_score
            )

        except Exception as e:
            logger.error(f"Edge Calculation Failed: {str(e)}")
            return self._get_fallback_edge()

    def _get_atm_iv(self, chain: pd.DataFrame, spot: float) -> float:
        """Finds the IV of the strike closest to Spot."""
        if chain.empty or spot == 0:
            return 0.0
        # Sort by distance to spot
        atm_idx = (chain['strike'] - spot).abs().argsort()[:1]
        if len(atm_idx) > 0:
            return chain.iloc[atm_idx]['ce_iv'].values[0]
        return 0.0

    def _classify_edge(self, 
                       vol: VolMetrics, 
                       vrp_w: float, 
                       vrp_m: float, 
                       term_regime: str,
                       term_spread: float):
        """
        Classifies the specific market opportunity based on v30.1 logic.
        """
        # Base Score Calculation
        # We use Weekly VRP as the primary driver for scoring, as per your script's focus on gamma
        score = 5.0
        
        if vrp_w > 4.0: score += 3.0       # Aggressive Edge
        elif vrp_w > 2.0: score += 2.0     # Solid Edge
        elif vrp_w > 1.0: score += 1.0     # Minimal Edge
        elif vrp_w < 0: score -= 3.0       # Negative Edge (Long Vol Territory)

        # Term Structure Bonuses
        if term_regime == "BACKWARDATION" and term_spread < -2.0:
            score += 1.0  # Panic selling of front week -> Short opp
        elif term_regime == "CONTANGO":
            score += 0.5  # Healthy market

        score = max(0, min(10, score))

        # Primary Classification Tag
        if vol.ivp_1yr < 25:
            edge_type = "LONG_VOL"
        elif vrp_w > 4.0 and vol.ivp_1yr > 50:
            edge_type = "SHORT_GAMMA"  # Front week is overpriced
        elif vrp_m > 3.0 and vol.ivp_1yr > 50:
            edge_type = "SHORT_VEGA"   # Back month is overpriced
        elif term_regime == "BACKWARDATION" and term_spread < -2.0:
            edge_type = "CALENDAR_SPREAD" # Sell Front / Buy Back? (Or just Short Front)
        elif vol.ivp_1yr > 75:
            edge_type = "MEAN_REVERSION"
        else:
            edge_type = "NONE"

        return edge_type, score

    def _get_fallback_edge(self) -> EdgeMetrics:
        return EdgeMetrics(
            iv_weekly=0, vrp_weighted_weekly=0, vrp_garch_weekly=0, vrp_park_weekly=0, vrp_rv_weekly=0,
            iv_monthly=0, vrp_weighted_monthly=0, vrp_garch_monthly=0, vrp_park_monthly=0, vrp_rv_monthly=0,
            term_spread=0, term_regime="NEUTRAL", primary_edge="ERROR", edge_score=0
        )
