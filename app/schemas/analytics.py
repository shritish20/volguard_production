from dataclasses import dataclass

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
    primary: str = "NONE"

@dataclass
class ExtMetrics:
    fii: float
    dii: float
    events: int
    event_names: list
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
