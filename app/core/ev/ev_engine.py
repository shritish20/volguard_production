# app/core/ev/ev_engine.py

from dataclasses import dataclass
from typing import Dict, List

# =====================================================
# DATA CONTRACTS
# =====================================================

@dataclass(frozen=True)
class RawEdgeInputs:
    atm_iv: float
    rv: float
    garch: float
    parkinson: float
    ivp: float
    fast_vol: bool

@dataclass(frozen=True)
class StrategyProfile:
    name: str
    theta_score: float          # Ability to monetize decay
    tail_risk_score: float      # Tail containment
    hedge_efficiency: float     # How well hedges work
    capital_lock: float         # Capital friction
    margin_required: float      # Estimated margin

@dataclass(frozen=True)
class EVResult:
    strategy: str
    raw_edge: float
    structural_edge: float
    regime_multiplier: float
    capital_efficiency: float
    final_ev: float

# =====================================================
# TRUE EV ENGINE
# =====================================================

class TrueEVEngine:
    """
    True Expected Value Engine for Option Sellers
    EV = Raw Edge × Structural Edge × Regime Multiplier × Capital Efficiency
    """

    # STRATEGY PROFILES
    STRATEGY_PROFILES: Dict[str, StrategyProfile] = {
        "SHORT_STRANGLE": StrategyProfile(
            name="SHORT_STRANGLE",
            theta_score=8.0,
            tail_risk_score=2.0,
            hedge_efficiency=3.0,
            capital_lock=6.0,
            margin_required=350_000
        ),
        "IRON_CONDOR": StrategyProfile(
            name="IRON_CONDOR",
            theta_score=6.0,
            tail_risk_score=6.0,
            hedge_efficiency=6.0,
            capital_lock=5.0,
            margin_required=180_000
        ),
        "BROKEN_WING_FLY": StrategyProfile(
            name="BROKEN_WING_FLY",
            theta_score=5.0,
            tail_risk_score=8.0,
            hedge_efficiency=8.0,
            capital_lock=3.0,
            margin_required=120_000
        ),
        "CALENDAR": StrategyProfile(
            name="CALENDAR",
            theta_score=4.0,
            tail_risk_score=9.0,
            hedge_efficiency=7.0,
            capital_lock=2.0,
            margin_required=100_000
        ),
    }

    # REGIME MULTIPLIERS
    REGIME_MULTIPLIERS: Dict[str, float] = {
        "AGGRESSIVE_SHORT": 1.00,
        "MODERATE_SHORT": 0.75,
        "DEFENSIVE": 0.50,
        "LONG_VOL": 0.25,
        "CASH": 0.00
    }

    def evaluate(
        self,
        raw: RawEdgeInputs,
        regime: str,
        expected_theta: Dict[str, float]
    ) -> List[EVResult]:
        """Returns ranked EVResult list. Empty if NO TRADE."""
        
        # 1. Base Filters
        if not self._raw_edge_allowed(raw):
            return []

        raw_edge_score = self._calculate_raw_edge(raw)
        if raw_edge_score <= 0:
            return []

        regime_mult = self.REGIME_MULTIPLIERS.get(regime, 0.0)
        if regime_mult <= 0:
            return []

        results: List[EVResult] = []

        # 2. Score Strategies
        for strategy, profile in self.STRATEGY_PROFILES.items():
            ses = self._structural_edge(profile)
            if ses < 6.0: continue

            theta = expected_theta.get(strategy, 0.0)
            ces = self._capital_efficiency(theta, profile.margin_required)
            if ces <= 0: continue

            final_ev = raw_edge_score * ses * regime_mult * ces

            if final_ev > 0:
                results.append(
                    EVResult(
                        strategy=strategy,
                        raw_edge=round(raw_edge_score, 4),
                        structural_edge=round(ses, 4),
                        regime_multiplier=regime_mult,
                        capital_efficiency=round(ces, 6),
                        final_ev=round(final_ev, 6)
                    )
                )

        return sorted(results, key=lambda x: x.final_ev, reverse=True)

    # INTERNAL LOGIC
    def _raw_edge_allowed(self, raw: RawEdgeInputs) -> bool:
        if raw.fast_vol: return False
        if raw.ivp < 20: return False
        return True

    def _calculate_raw_edge(self, raw: RawEdgeInputs) -> float:
        vrp_rv = raw.atm_iv - raw.rv
        vrp_ga = raw.atm_iv - raw.garch
        vrp_pk = raw.atm_iv - raw.parkinson
        return max((0.4 * vrp_rv) + (0.4 * vrp_ga) + (0.2 * vrp_pk), 0.0)

    def _structural_edge(self, profile: StrategyProfile) -> float:
        return (profile.theta_score * 0.4 + 
                profile.tail_risk_score * 0.4 + 
                profile.hedge_efficiency * 0.2)

    def _capital_efficiency(self, theta: float, margin: float) -> float:
        if margin <= 0: return 0.0
        return max(theta / margin, 0.000001)
