# app/schemas/analytics.py

from dataclasses import dataclass
from typing import List


# ======================================================
# VOLATILITY METRICS
# ======================================================
@dataclass
class VolMetrics:
    spot: float
    vix: float
    vov: float

    rv7: float
    rv28: float

    garch7: float
    garch28: float

    pk7: float
    pk28: float

    ivp30: float
    ivp90: float
    ivp1y: float

    is_fallback: bool


# ======================================================
# STRUCTURE METRICS
# ======================================================
@dataclass
class StructMetrics:
    net_gex: float
    gex_regime: str       # STICKY / SLIPPERY / NEUTRAL
    pcr: float
    max_pain: float
    lot: int
    skew: float
    bias: str             # BULLISH / BEARISH / NEUTRAL


# ======================================================
# EDGE METRICS
# ======================================================
@dataclass
class EdgeMetrics:
    iv_weekly: float
    iv_monthly: float
    term_structure: float

    vrp_rv_w: float
    vrp_rv_m: float

    vrp_garch_w: float
    vrp_garch_m: float

    vrp_pk_w: float
    vrp_pk_m: float

    primary: str = "NONE"


# ======================================================
# EXTERNAL / EVENT METRICS
# ======================================================
@dataclass
class ExtMetrics:
    fii: float
    dii: float
    events: int
    event_names: List[str]
    fast_vol: bool


# ======================================================
# REGIME RESULT
# ======================================================
@dataclass
class RegimeResult:
    name: str
    score: float
    primary_edge: str

    v_scr: float
    s_scr: float
    e_scr: float
    r_scr: float

    alloc_pct: float
    max_lots: int
