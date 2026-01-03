from pydantic import BaseModel
from typing import List, Dict, Union, Any, Optional
from dataclasses import dataclass
from datetime import datetime

# --- Internal Calculation Types (Dataclasses) ---
# [span_0](start_span)Derived from[span_0](end_span)

@dataclass
class VolMetrics:
    spot: float
    vix: float
    vov: float
    rv7: float
    rv28: float
    ga7: float
    ga28: float
    pk7: float
    pk28: float
    ivp30: float
    ivp90: float
    ivp1y: float
    is_fallback: bool

@dataclass
class StructMetrics:
    net_gex: float
    gex_regime: str
    pcr: float
    max_pain: float
    lot: int
    skew: float
    regime: str

@dataclass
class EdgeMetrics:
    iv_w: float
    iv_m: float
    term: float
    vrp_rv_w: float
    vrp_ga_w: float
    vrp_pk_w: float
    vrp_rv_m: float
    vrp_ga_m: float
    vrp_pk_m: float
    primary: str

@dataclass
class ExtMetrics:
    fii: float
    dii: float
    events: int
    event_names: List[str]
    fast_vol: bool

@dataclass
class RegimeResult:
    name: str
    score: float
    primary: str
    v_scr: float
    s_scr: float
    e_scr: float
    r_scr: float
    alloc_pct: float
    max_lots: int

# --- API Response Models (Pydantic) ---
# [span_1](start_span)Derived from[span_1](end_span)

class MetricItem(BaseModel):
    value: Union[float, str, int]
    formatted: str
    tag: str
    color: str

class VolatilityDashboard(BaseModel):
    spot: MetricItem
    vix: MetricItem
    vov: MetricItem
    ivp_30: MetricItem
    ivp_90: MetricItem
    ivp_1y: MetricItem
    rv_7_28: MetricItem
    garch_7_28: MetricItem
    parkinson_7_28: MetricItem
    is_fallback: bool

class EdgeDashboard(BaseModel):
    iv_weekly: MetricItem
    iv_monthly: MetricItem
    vrp_rv_w: MetricItem
    vrp_rv_m: MetricItem
    vrp_ga_w: MetricItem
    vrp_ga_m: MetricItem
    vrp_pk_w: MetricItem
    vrp_pk_m: MetricItem
    term_structure: MetricItem

class StructureDashboard(BaseModel):
    net_gex: MetricItem
    pcr: MetricItem
    max_pain: MetricItem
    skew_25d: MetricItem

class ScoresDashboard(BaseModel):
    vol_score: float
    struct_score: float
    edge_score: float
    risk_score: float
    total_score: float

class CapitalDashboard(BaseModel):
    regime_name: str
    primary_edge: str
    allocation_pct: float
    max_lots: int
    recommendation: str

class FullAnalysisResponse(BaseModel):
    timestamp: datetime
    volatility: VolatilityDashboard
    edges: EdgeDashboard
    structure: StructureDashboard
    scores: ScoresDashboard
    external: Dict[str, Any]
    capital: CapitalDashboard
