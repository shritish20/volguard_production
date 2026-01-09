# app/core/trading/strategies.py

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum

class StrategyType(Enum):
    """Strategy types for hybrid logic"""
    SHORT_STRANGLE = "SHORT_STRANGLE"
    IRON_CONDOR = "IRON_CONDOR"
    IRON_FLY = "IRON_FLY"
    CREDIT_SPREAD = "CREDIT_SPREAD"
    RATIO_SPREAD = "RATIO_SPREAD"
    WAIT = "WAIT"

@dataclass
class HybridStrategyDefinition:
    """
    NEW: Hybrid Strategy Blueprint for VolGuard 5.0
    
    Hybrid Logic:
    - Short Strikes: Based on ATM Straddle Price (Expected Move)
    - Long Wings: Based on Delta (Protection)
    
    Replaces old delta-based StrategyDefinition
    """
    name: str
    type: StrategyType
    allowed_regimes: List[str]
    
    # HYBRID CONFIGURATION
    straddle_multiplier: float       # Multiplier for ATM straddle cost
    wing_delta: Optional[float]     # Delta for wings (None for naked strategies)
    wing_distance_points: Optional[int]  # Alternative: Fixed point distance
    
    # RISK MANAGEMENT
    risk_type: str                  # DEFINED, UNDEFINED
    max_position_size: int          # Max lots for this strategy
    max_capital_allocation: float   # Max % of capital (0.0 to 1.0)
    
    # ENTRY FILTERS
    min_ivp: float                  # Minimum IV Percentile
    min_vrp: float                  # Minimum Vol Risk Premium
    max_vov_zscore: float           # Max VoV Z-Score (kill switch)
    min_gex_support: Optional[float] # Minimum GEX support (in Cr)
    
    # EXECUTION PARAMETERS
    buffer_pct: float               # Price buffer % for limit orders
    require_weekly_expiry: bool     # True for gamma-sensitive strategies
    allow_monthly_expiry: bool      # True for vega-sensitive strategies
    
    # MONITORING
    max_loss_pct: float             # Max loss % before auto-exit
    max_delta_exposure: float       # Max portfolio delta allowed
    priority: int = 1               # Higher = Preferred

# ==========================================
# 📘 HYBRID STRATEGY REGISTRY (VolGuard 5.0)
# ==========================================

HYBRID_STRATEGY_REGISTRY = [
    # ==========================================
    # 🟢 DEFENSIVE REGIME (Capital Protection)
    # ==========================================
    HybridStrategyDefinition(
        name="WIDE_IRON_CONDOR_HYBRID",
        type=StrategyType.IRON_CONDOR,
        allowed_regimes=["DEFENSIVE", "MODERATE_SHORT", "NEUTRAL"],
        
        # HYBRID CONFIG
        straddle_multiplier=1.2,    # Use 120% of straddle cost (wider)
        wing_delta=0.10,            # 10 Delta wings
        wing_distance_points=None,  # Use delta-based
        
        # RISK
        risk_type="DEFINED",
        max_position_size=2,
        max_capital_allocation=0.25,
        
        # FILTERS
        min_ivp=20.0,
        min_vrp=0.5,
        max_vov_zscore=1.5,         # Very conservative
        min_gex_support=5.0,        # Require positive GEX support
        
        # EXECUTION
        buffer_pct=0.03,            # 3% buffer
        require_weekly_expiry=True,
        allow_monthly_expiry=False,
        
        # MONITORING
        max_loss_pct=0.15,          # 15% max loss
        max_delta_exposure=0.10,
        priority=10
    ),
    
    HybridStrategyDefinition(
        name="CREDIT_SPREAD_DEFENSIVE",
        type=StrategyType.CREDIT_SPREAD,
        allowed_regimes=["DEFENSIVE", "NEUTRAL"],
        
        # HYBRID CONFIG (delta-based for spreads)
        straddle_multiplier=0.5,    # 50% of straddle for short leg
        wing_delta=0.15,            # 15 delta wing
        wing_distance_points=200,   # Or fixed 200 point width
        
        # RISK
        risk_type="DEFINED",
        max_position_size=3,
        max_capital_allocation=0.30,
        
        # FILTERS
        min_ivp=15.0,
        min_vrp=0.3,
        max_vov_zscore=2.0,
        min_gex_support=None,
        
        # EXECUTION
        buffer_pct=0.02,
        require_weekly_expiry=True,
        allow_monthly_expiry=True,
        
        # MONITORING
        max_loss_pct=0.20,
        max_delta_exposure=0.15,
        priority=8
    ),

    # ==========================================
    # 🟡 MODERATE REGIME (Balanced VRP)
    # ==========================================
    HybridStrategyDefinition(
        name="IRON_CONDOR_HYBRID",
        type=StrategyType.IRON_CONDOR,
        allowed_regimes=["MODERATE_SHORT", "AGGRESSIVE_SHORT"],
        
        # HYBRID CONFIG
        straddle_multiplier=1.0,    # Use 100% of straddle cost
        wing_delta=0.10,            # 10 Delta wings
        wing_distance_points=None,
        
        # RISK
        risk_type="DEFINED",
        max_position_size=3,
        max_capital_allocation=0.40,
        
        # FILTERS
        min_ivp=25.0,
        min_vrp=1.0,
        max_vov_zscore=2.0,
        min_gex_support=0.0,        # Neutral or positive GEX
        
        # EXECUTION
        buffer_pct=0.025,
        require_weekly_expiry=True,
        allow_monthly_expiry=False,
        
        # MONITORING
        max_loss_pct=0.25,
        max_delta_exposure=0.20,
        priority=10
    ),
    
    HybridStrategyDefinition(
        name="SHORT_STRANGLE_HYBRID",
        type=StrategyType.SHORT_STRANGLE,
        allowed_regimes=["MODERATE_SHORT", "AGGRESSIVE_SHORT"],
        
        # HYBRID CONFIG
        straddle_multiplier=1.0,    # Use full straddle range
        wing_delta=None,            # Naked - no wings
        wing_distance_points=None,
        
        # RISK
        risk_type="UNDEFINED",
        max_position_size=2,
        max_capital_allocation=0.20,
        
        # FILTERS
        min_ivp=30.0,
        min_vrp=1.5,
        max_vov_zscore=1.8,
        min_gex_support=-5.0,       # Can tolerate some negative GEX
        
        # EXECUTION
        buffer_pct=0.035,
        require_weekly_expiry=True,
        allow_monthly_expiry=False,
        
        # MONITORING
        max_loss_pct=0.30,
        max_delta_exposure=0.25,
        priority=7
    ),

    # ==========================================
    # 🟠 AGGRESSIVE REGIME (High Theta)
    # ==========================================
    HybridStrategyDefinition(
        name="IRON_FLY_HYBRID",
        type=StrategyType.IRON_FLY,
        allowed_regimes=["AGGRESSIVE_SHORT", "ULTRA_AGGRESSIVE"],
        
        # HYBRID CONFIG
        straddle_multiplier=0.5,    # Use 50% of straddle (tighter)
        wing_delta=0.15,            # 15 Delta wings (tighter than condor)
        wing_distance_points=None,
        
        # RISK
        risk_type="DEFINED",
        max_position_size=2,
        max_capital_allocation=0.35,
        
        # FILTERS
        min_ivp=40.0,
        min_vrp=2.0,
        max_vov_zscore=1.5,         # Need stable volatility
        min_gex_support=10.0,       # Strong GEX support required
        
        # EXECUTION
        buffer_pct=0.02,
        require_weekly_expiry=True,
        allow_monthly_expiry=False,
        
        # MONITORING
        max_loss_pct=0.20,
        max_delta_exposure=0.15,
        priority=10
    ),
    
    HybridStrategyDefinition(
        name="RATIO_SPREAD_HYBRID",
        type=StrategyType.RATIO_SPREAD,
        allowed_regimes=["AGGRESSIVE_SHORT", "ULTRA_AGGRESSIVE"],
        
        # HYBRID CONFIG
        straddle_multiplier=0.3,    # Close to ATM
        wing_delta=0.20,            # Further out wing
        wing_distance_points=100,   # Tight 100 point width
        
        # RISK
        risk_type="UNDEFINED",      # Ratio spreads have undefined risk
        max_position_size=1,
        max_capital_allocation=0.15,
        
        # FILTERS
        min_ivp=50.0,
        min_vrp=2.5,
        max_vov_zscore=1.2,         # Very stable vol required
        min_gex_support=None,
        
        # EXECUTION
        buffer_pct=0.015,
        require_weekly_expiry=True,
        allow_monthly_expiry=False,
        
        # MONITORING
        max_loss_pct=0.40,
        max_delta_exposure=0.30,
        priority=6
    ),

    # ==========================================
    # 🔴 ULTRA_AGGRESSIVE REGIME (Expert Only)
    # ==========================================
    HybridStrategyDefinition(
        name="NAKED_STRADDLE_HYBRID",
        type=StrategyType.SHORT_STRANGLE,
        allowed_regimes=["ULTRA_AGGRESSIVE"],
        
        # HYBRID CONFIG
        straddle_multiplier=0.8,    # Slightly inside expected move
        wing_delta=None,            # Completely naked
        wing_distance_points=None,
        
        # RISK
        risk_type="UNDEFINED",
        max_position_size=1,
        max_capital_allocation=0.10,
        
        # FILTERS
        min_ivp=60.0,
        min_vrp=3.0,
        max_vov_zscore=1.0,         # Extremely stable vol
        min_gex_support=-20.0,      # Can handle negative GEX
        
        # EXECUTION
        buffer_pct=0.05,
        require_weekly_expiry=True,
        allow_monthly_expiry=False,
        
        # MONITORING
        max_loss_pct=0.50,
        max_delta_exposure=0.40,
        priority=5
    ),
]

# ==========================================
# 🔧 HELPER FUNCTIONS
# ==========================================

def get_strategies_for_regime(regime_name: str) -> List[HybridStrategyDefinition]:
    """Get all strategies allowed for a given regime"""
    return [
        strategy for strategy in HYBRID_STRATEGY_REGISTRY
        if regime_name in strategy.allowed_regimes
    ]

def get_strategy_by_name(name: str) -> Optional[HybridStrategyDefinition]:
    """Get strategy definition by name"""
    for strategy in HYBRID_STRATEGY_REGISTRY:
        if strategy.name == name:
            return strategy
    return None

def get_strategy_by_type(strategy_type: StrategyType) -> List[HybridStrategyDefinition]:
    """Get all strategies of a given type"""
    return [
        strategy for strategy in HYBRID_STRATEGY_REGISTRY
        if strategy.type == strategy_type
    ]

def get_default_strategy_for_regime(regime_name: str) -> Optional[HybridStrategyDefinition]:
    """Get highest priority strategy for a regime"""
    strategies = get_strategies_for_regime(regime_name)
    if not strategies:
        return None
    
    # Sort by priority (highest first)
    strategies.sort(key=lambda s: s.priority, reverse=True)
    return strategies[0]

# ==========================================
# 📊 STRATEGY VALIDATION
# ==========================================

def validate_strategy_for_market(
    strategy: HybridStrategyDefinition,
    market_metrics: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Validate if a strategy can be deployed given current market conditions
    
    Returns:
        Dict with validation results
    """
    validation = {
        "strategy": strategy.name,
        "valid": True,
        "reasons": [],
        "warnings": []
    }
    
    ivp = market_metrics.get("ivp_1yr", 0)
    vrp = market_metrics.get("vrp_weighted_weekly", 0)
    vov_zscore = market_metrics.get("vov_zscore", 0)
    gex = market_metrics.get("weekly_gex_cr", 0)
    
    # Check IVP
    if ivp < strategy.min_ivp:
        validation["valid"] = False
        validation["reasons"].append(f"IVP {ivp:.1f} < min {strategy.min_ivp}")
    
    # Check VRP
    if vrp < strategy.min_vrp:
        validation["valid"] = False
        validation["reasons"].append(f"VRP {vrp:.2f} < min {strategy.min_vrp}")
    
    # Check VoV Z-Score
    if vov_zscore > strategy.max_vov_zscore:
        validation["valid"] = False
        validation["reasons"].append(f"VoV Z-Score {vov_zscore:.1f} > max {strategy.max_vov_zscore}")
    
    # Check GEX support
    if strategy.min_gex_support is not None and gex < strategy.min_gex_support:
        validation["warnings"].append(f"GEX {gex:.1f}Cr < preferred {strategy.min_gex_support}Cr")
    
    return validation

# ==========================================
# 🎯 REGIME MAPPING
# ==========================================

REGIME_STRATEGY_MAP = {
    "CASH": StrategyType.WAIT,
    "DEFENSIVE": StrategyType.IRON_CONDOR,
    "NEUTRAL": StrategyType.IRON_CONDOR,
    "MODERATE_SHORT": StrategyType.IRON_CONDOR,
    "AGGRESSIVE_SHORT": StrategyType.IRON_FLY,
    "ULTRA_AGGRESSIVE": StrategyType.SHORT_STRANGLE
}

def get_strategy_type_for_regime(regime_name: str) -> StrategyType:
    """Map regime to default strategy type"""
    return REGIME_STRATEGY_MAP.get(regime_name, StrategyType.WAIT)

# ==========================================
# 📝 LEGACY SUPPORT (For backward compatibility)
# ==========================================

@dataclass
class LegacyStrategyDefinition:
    """
    Legacy delta-based strategy definition
    Kept for backward compatibility during migration
    """
    name: str
    allowed_regimes: List[str]
    structure: str
    risk_type: str
    core_deltas: List[float]
    hedge_deltas: List[float]
    ratios: List[int]
    min_ivp: float
    min_vrp: float
    max_vol_of_vol: float
    priority: int = 1

def convert_legacy_to_hybrid(legacy_strategy: LegacyStrategyDefinition) -> HybridStrategyDefinition:
    """
    Convert legacy delta-based strategy to hybrid strategy
    This is a best-effort conversion for migration
    """
    # Map structure to type
    structure_map = {
        "STRANGLE": StrategyType.SHORT_STRANGLE,
        "CONDOR": StrategyType.IRON_CONDOR,
        "FLY": StrategyType.IRON_FLY,
        "SPREAD": StrategyType.CREDIT_SPREAD,
        "RATIO": StrategyType.RATIO_SPREAD
    }
    
    strategy_type = structure_map.get(legacy_strategy.structure, StrategyType.SHORT_STRANGLE)
    
    # Estimate hybrid parameters from deltas
    avg_core_delta = sum(abs(d) for d in legacy_strategy.core_deltas) / len(legacy_strategy.core_deltas)
    
    # Delta to straddle multiplier mapping (approximate)
    # 0.50 delta = ATM (0.0 multiplier)
    # 0.25 delta = ~0.75 multiplier
    # 0.15 delta = ~1.0 multiplier
    straddle_multiplier = 1.0 / (avg_core_delta * 2) if avg_core_delta > 0 else 1.0
    
    # Wing delta if available
    wing_delta = None
    if legacy_strategy.hedge_deltas:
        avg_hedge_delta = sum(abs(d) for d in legacy_strategy.hedge_deltas) / len(legacy_strategy.hedge_deltas)
        wing_delta = avg_hedge_delta
    
    return HybridStrategyDefinition(
        name=f"{legacy_strategy.name}_CONVERTED",
        type=strategy_type,
        allowed_regimes=legacy_strategy.allowed_regimes,
        straddle_multiplier=straddle_multiplier,
        wing_delta=wing_delta,
        wing_distance_points=None,
        risk_type=legacy_strategy.risk_type,
        max_position_size=3,
        max_capital_allocation=0.3,
        min_ivp=legacy_strategy.min_ivp,
        min_vrp=legacy_strategy.min_vrp,
        max_vov_zscore=legacy_strategy.max_vol_of_vol / 100,  # Approximate conversion
        min_gex_support=None,
        buffer_pct=0.03,
        require_weekly_expiry=True,
        allow_monthly_expiry=False,
        max_loss_pct=0.25,
        max_delta_exposure=0.2,
        priority=legacy_strategy.priority
            )
