# app/core/trading/strategies.py

from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class StrategyDefinition:
    """
    Blueprint for a Trading Strategy.
    Consumed by: StrategySelector (to pick) and LegBuilder (to build orders).
    """
    name: str
    allowed_regimes: List[str]
    structure: str                  # STRANGLE, CONDOR, FLY, SPREAD, RATIO, CONDOR_BWB
    risk_type: str                  # DEFINED, UNDEFINED
    
    # Deltas
    # Core: The main legs (usually Short). 
    # Hedge: The wing legs (usually Long).
    # For Ratio/Spreads, logic is handled by LegBuilder based on 'structure'
    core_deltas: List[float]        
    hedge_deltas: List[float]       
    
    # Ratios [Core1, Core2, Hedge1, Hedge2] or specific mapping
    ratios: List[int]               
    
    # Entry Filters
    min_ivp: float                  # Minimum IV Percentile
    min_vrp: float                  # Minimum Vol Risk Premium
    max_vol_of_vol: float           # Max VoV allowed
    
    priority: int = 1               # Higher = Preferred

# ==========================================
# 📘 STRATEGY REGISTRY (AUTHORITATIVE)
# ==========================================

STRATEGY_REGISTRY = [
    # ----------------------------------------
    # 🟢 DEFENSIVE REGIME
    # Purpose: Capital Protection, Defined Risk
    # ----------------------------------------
    StrategyDefinition(
        name="WIDE_IRON_CONDOR",
        allowed_regimes=["LONG_VOL", "DEFENSIVE", "NEUTRAL"],
        structure="CONDOR",
        risk_type="DEFINED",
        core_deltas=[0.20, -0.20],  # Sell 20d Call/Put
        hedge_deltas=[0.05, -0.05], # Buy 5d Wings
        ratios=[1, 1, 1, 1],
        min_ivp=10.0,
        min_vrp=0.0,
        max_vol_of_vol=200.0,
        priority=10
    ),
    StrategyDefinition(
        name="PUT_CREDIT_SPREAD",
        allowed_regimes=["DEFENSIVE", "NEUTRAL"],
        structure="SPREAD",
        risk_type="DEFINED",
        core_deltas=[-0.25],        # Sell 25d Put
        hedge_deltas=[-0.10],       # Buy 10d Put
        ratios=[1, 1],
        min_ivp=15.0,
        min_vrp=0.5,
        max_vol_of_vol=150.0,
        priority=8
    ),

    # ----------------------------------------
    # 🟡 MODERATE REGIME
    # Purpose: Balanced VRP Harvesting
    # ----------------------------------------
    StrategyDefinition(
        name="IRON_CONDOR",
        allowed_regimes=["MODERATE_SHORT"],
        structure="CONDOR",
        risk_type="DEFINED",
        core_deltas=[0.25, -0.25],  # Sell 25d
        hedge_deltas=[0.10, -0.10], # Buy 10d
        ratios=[1, 1, 1, 1],
        min_ivp=20.0,
        min_vrp=1.0,
        max_vol_of_vol=120.0,
        priority=10
    ),
    StrategyDefinition(
        name="BROKEN_WING_CONDOR",
        allowed_regimes=["MODERATE_SHORT"],
        structure="CONDOR_BWB",    # Custom handling in LegBuilder
        risk_type="DEFINED",
        core_deltas=[0.30, -0.30],
        hedge_deltas=[0.10, -0.05], # Asymmetric Wings
        ratios=[1, 1, 1, 1],
        min_ivp=25.0,
        min_vrp=1.5,
        max_vol_of_vol=100.0,
        priority=8
    ),

    # ----------------------------------------
    # 🟠 AGGRESSIVE REGIME
    # Purpose: High Theta, Controlled Structure
    # ----------------------------------------
    StrategyDefinition(
        name="IRON_FLY",
        allowed_regimes=["AGGRESSIVE_SHORT"],
        structure="FLY",
        risk_type="DEFINED",
        core_deltas=[0.50, -0.50],  # Sell ATM
        hedge_deltas=[0.20, -0.20], # Buy Wings
        ratios=[1, 1, 1, 1],
        min_ivp=40.0,
        min_vrp=2.0,
        max_vol_of_vol=80.0,        # Requires stable structure
        priority=10
    ),
    StrategyDefinition(
        name="RATIO_PUT_SPREAD",
        allowed_regimes=["AGGRESSIVE_SHORT"],
        structure="RATIO",
        risk_type="UNDEFINED",      # Naked legs involved
        core_deltas=[-0.30, -0.15], # Buy Closer / Sell Further
        hedge_deltas=[],            # Usually implicit
        ratios=[1, 2],              # Buy 1, Sell 2
        min_ivp=50.0,
        min_vrp=3.0,
        max_vol_of_vol=100.0,
        priority=9
    ),

    # ----------------------------------------
    # 🔴 ULTRA_AGGRESSIVE REGIME
    # Purpose: Expert VRP Extraction
    # ----------------------------------------
    StrategyDefinition(
        name="HEDGED_STRANGLE",
        allowed_regimes=["AGGRESSIVE_SHORT", "ULTRA_AGGRESSIVE"],
        structure="STRANGLE",
        risk_type="DEFINED",        # Technically defined by tail hedges
        core_deltas=[0.25, -0.25],
        hedge_deltas=[0.05, -0.05], # Cheap tail protection
        ratios=[1, 1, 1, 1],
        min_ivp=60.0,
        min_vrp=4.0,
        max_vol_of_vol=150.0,
        priority=10
    ),
    StrategyDefinition(
        name="SHORT_STRANGLE",
        allowed_regimes=["ULTRA_AGGRESSIVE"],
        structure="STRANGLE",
        risk_type="UNDEFINED",
        core_deltas=[0.20, -0.20],
        hedge_deltas=[],            # Naked
        ratios=[1, 1],
        min_ivp=70.0,
        min_vrp=5.0,
        max_vol_of_vol=100.0,
        priority=5                  # Lower priority than hedged
    ),
]
