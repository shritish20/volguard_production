from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, date
from pydantic import BaseModel

# ======================================================
# SECTION 1: INTERNAL ENGINE METRICS (Dataclasses)
# These act as the "Nerves" passing data between Engines
# ======================================================

@dataclass
class VolMetrics:
    spot: float
    vix: float
    # Realized & Forecast
    rv7: float
    rv28: float
    rv90: float
    garch7: float
    garch28: float
    park7: float
    park28: float
    # VolGuard 4.1 Specifics
    vov: float
    vov_zscore: float      # <--- NEW
    ivp_30d: float
    ivp_90d: float
    ivp_1yr: float
    trend_strength: float
    atr14: float           # <--- NEW (Required for Strike Selection)
    ma20: float            # <--- NEW
    vol_regime: str
    is_fallback: bool

@dataclass
class StructMetrics:
    net_gex: float
    gex_ratio: float       # <--- NEW
    total_oi_value: float  # <--- NEW
    gex_regime: str        # STICKY / SLIPPERY
    pcr: float
    max_pain: float
    skew_25d: float        # <--- NEW
    oi_regime: str
    lot_size: int

@dataclass
class EdgeMetrics:
    # Weekly (Gamma)
    iv_weekly: float
    vrp_weighted_weekly: float  # <--- NEW (70% Garch)
    vrp_garch_weekly: float
    vrp_park_weekly: float
    vrp_rv_weekly: float
    
    # Monthly (Vega)
    iv_monthly: float
    vrp_weighted_monthly: float # <--- NEW
    vrp_garch_monthly: float
    vrp_park_monthly: float
    vrp_rv_monthly: float

    # Term Structure
    term_spread: float
    term_regime: str
    
    # Classification
    primary_edge: str
    edge_score: float

@dataclass
class ParticipantData:
    fut_long: float
    fut_short: float
    fut_net: float
    call_long: float
    call_short: float
    call_net: float
    put_long: float
    put_short: float
    put_net: float
    stock_net: float

@dataclass
class ExtMetrics:
    fii: Optional[ParticipantData]
    dii: Optional[ParticipantData]
    pro: Optional[ParticipantData]
    client: Optional[ParticipantData]
    fii_net_change: float
    flow_regime: str       # STRONG_SHORT / STRONG_LONG
    event_risk: str        # HIGH / LOW
    data_date: str

# ======================================================
# SECTION 2: THE TRADING MANDATE (The "Brain" Output)
# ======================================================

@dataclass
class RegimeScore:
    vol_score: float
    struct_score: float
    edge_score: float
    risk_score: float
    composite: float
    confidence: str

@dataclass
class TradingMandate:
    """
    The Master Instruction set sent from RegimeEngine to Supervisor.
    Includes COMPATIBILITY PROPERTIES for older TradingEngine.
    """
    expiry_type: str
    regime_name: str        # AGGRESSIVE_SHORT / CASH etc.
    strategy_type: str      # STRANGLE / IRON_CONDOR etc.
    allocation_pct: float   # 0 to 100
    max_lots: int
    risk_per_lot: float = 0.0
    score: Optional[RegimeScore] = None
    rationale: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    suggested_structure: str = "NONE"

    # --- COMPATIBILITY LAYER (Crucial for engine.py) ---
    @property
    def name(self) -> str:
        """Alias for regime_name (Expected by old TradingEngine)"""
        return self.regime_name
    
    @property
    def alloc_pct(self) -> float:
        """Returns decimal allocation (0.60) instead of percentage (60.0)"""
        return self.allocation_pct / 100.0

# Legacy wrapper if strictly needed
RegimeResult = TradingMandate 


# ======================================================
# SECTION 3: API DASHBOARD MODELS (Pydantic)
# Matches the JSON structure in dashboard.py
# ======================================================

class MetricValues(BaseModel):
    vov_zscore: float
    ivp_30d: float
    ivp_1yr: float
    rv_7d: float
    garch_7d: float
    parkinson_7d: float

class VolatilitySection(BaseModel):
    spot: float
    vix: float
    trend_strength: float
    metrics: MetricValues
    regime: str
    kill_switch_active: bool

class ParticipantInfo(BaseModel):
    fut_net: float
    call_net: float
    put_net: float
    bias: str

class FlowSection(BaseModel):
    flow_regime: str
    event_risk: str
    fii_net_change: float
    positions: Dict[str, Optional[ParticipantInfo]]

class StructureDetails(BaseModel):
    net_gex_cr: float
    gex_regime: str
    pcr: float
    max_pain: float
    skew: Optional[float] = None

class StructureSection(BaseModel):
    weekly: StructureDetails
    monthly: StructureDetails

class EdgeDetails(BaseModel):
    atm_iv: float
    weighted_vrp: float
    raw_vrp_garch: Optional[float] = None

class EdgeSection(BaseModel):
    term_structure: str
    term_spread: float
    weekly_edge: EdgeDetails
    monthly_edge: EdgeDetails
    primary_opportunity: str

class MandateDetails(BaseModel):
    regime: str
    strategy: str
    allocation: str
    max_lots: int
    rationale: List[str]
    warnings: Optional[List[str]] = None

class DashboardResponse(BaseModel):
    meta: Dict[str, Any]
    time_context: Dict[str, Any]
    volatility: VolatilitySection
    external_flow: FlowSection
    market_structure: StructureSection
    edges: EdgeSection
    mandates: Dict[str, MandateDetails]
    recommendation: str
