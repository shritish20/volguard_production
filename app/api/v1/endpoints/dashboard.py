# app/api/v1/endpoints/dashboard.py

from fastapi import APIRouter, HTTPException, Depends, Request
from datetime import datetime
import asyncio
import logging
import math

from app.dependencies import get_market_client, get_persistence_service
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.services.persistence import PersistenceService
from app.services.cache import cache
from app.config import settings

# 🔑 AUTHORITATIVE REGISTRY
from app.services.instrument_registry import registry

# Core Analytics Engines
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.edge import EdgeEngine
from app.core.analytics.regime import RegimeEngine

# Schemas
from app.schemas.analytics import (
    FullAnalysisResponse, VolatilityDashboard, EdgeDashboard, 
    StructureDashboard, ScoresDashboard, CapitalDashboard, MetricItem, ExtMetrics
)

router = APIRouter()
logger = logging.getLogger(__name__)

# ============================================================
# VALIDATION HELPERS
# ============================================================

def is_valid_float(val):
    if not isinstance(val, (int, float)):
        return True
    return not (math.isinf(val) or math.isnan(val))


def sanitize_float(val, default=0.0, field_name="unknown"):
    if not isinstance(val, (int, float)):
        return val
    if math.isinf(val) or math.isnan(val):
        logger.error(f"Invalid calculation detected for {field_name}: {val}")
        return default
    return val


def get_tag_meta(val, type_):
    if type_ == "IVP":
        if val < 20: return "CHEAP", "green"
        if val > 80: return "RICH", "red"
        return "FAIR", "default"
    if type_ == "VRP":
        if val > 3.0: return "HIGH", "green"
        if val < 0.0: return "LOW", "red"
        return "OK", "default"
    if type_ == "VOV":
        return ("HIGH", "red") if val > 100 else ("STABLE", "default")
    if type_ == "TERM":
        return ("INV", "red") if val < -1.5 else ("NRML", "green")
    if type_ == "SKEW":
        if val > 5: return "HIGH", "orange"
        if val < -5: return "LOW", "blue"
        return "NEUTRAL", "default"
    return "-", "default"


def mk_item(val, type_tag=None, suffix="", field_name="metric"):
    if isinstance(val, (int, float)):
        val = sanitize_float(val, 0.0, field_name)
        formatted = f"{val:.2f}{suffix}"
    else:
        formatted = str(val)

    tag, color = (
        get_tag_meta(val, type_tag)
        if type_tag and isinstance(val, (int, float))
        else ("-", "default")
    )

    return MetricItem(value=val, formatted=formatted, tag=tag, color=color)


def validate_response_data(vol, st, ed, reg):
    error_fields = []

    critical = [
        (vol.spot, "spot"),
        (vol.vix, "vix"),
        (ed.term_structure, "term_structure"),
        (st.net_gex, "net_gex"),
        (reg.score, "total_score"),
        (reg.alloc_pct, "alloc_pct"),
    ]

    for val, name in critical:
        if not is_valid_float(val):
            error_fields.append(name)

    return len(error_fields) == 0, error_fields

# ============================================================
# MAIN ENDPOINT
# ============================================================

@router.post("/analyze", response_model=FullAnalysisResponse)
async def analyze_market_state(
    request: Request,
    market: MarketDataClient = Depends(get_market_client),
    db: PersistenceService = Depends(get_persistence_service),
):
    start_time = asyncio.get_event_loop().time()
    cache_key = f"dashboard:analysis:{datetime.now().strftime('%Y%m%d%H%M')}"

    try:
        cached = await cache.get(cache_key)
        if cached:
            logger.info("Cache hit")
            return FullAnalysisResponse.parse_raw(cached)
    except Exception:
        pass

    # Initialize Engines
    vol_engine = VolatilityEngine()
    struct_engine = StructureEngine()
    edge_engine = EdgeEngine()
    regime_engine = RegimeEngine()

    try:
        async with asyncio.timeout(30):

            # ====================================================
            # 1. FETCH AUTHORITATIVE STRUCTURE (REGISTRY)
            # ====================================================
            weekly_expiry, monthly_expiry = registry.get_nifty_expiries()
            specs = registry.get_nifty_contract_specs(weekly_expiry)
            lot_size = specs["lot_size"]

            # ====================================================
            # 2. PARALLEL DATA FETCH
            # ====================================================
            nh_task = db.load_daily_history(NIFTY_KEY, days=365)
            vh_task = db.load_daily_history(VIX_KEY, days=365)
            intra_task = market.get_intraday_candles(NIFTY_KEY)
            live_task = market.get_live_quote([NIFTY_KEY, VIX_KEY])
            wc_task = market.get_option_chain(weekly_expiry)
            mc_task = market.get_option_chain(monthly_expiry)

            nh, vh, intra, live, wc, mc = await asyncio.gather(
                nh_task, vh_task, intra_task, live_task, wc_task, mc_task
            )

            if nh.empty:
                nh = await market.get_daily_candles(NIFTY_KEY)
            if vh.empty:
                vh = await market.get_daily_candles(VIX_KEY)

            if nh.empty or wc.empty:
                raise HTTPException(status_code=500, detail="Critical data missing")

            spot = live.get(NIFTY_KEY, 0.0)
            vix = live.get(VIX_KEY, 0.0)

            # ====================================================
            # 3. ANALYTICS PIPELINE
            # ====================================================
            vol = await vol_engine.calculate_volatility(nh, intra, spot, vix)
            st = struct_engine.analyze_structure(wc, vol.spot, lot_size)
            ed = edge_engine.detect_edges(wc, mc, vol.spot, vol)

            ext = ExtMetrics(fii=0, dii=0, events=0, event_names=[], fast_vol=False)
            reg = regime_engine.calculate_regime(vol, st, ed, ext)

            is_valid, errors = validate_response_data(vol, st, ed, reg)

            # ====================================================
            # 4. RESPONSE
            # ====================================================
            result = FullAnalysisResponse(
                timestamp=datetime.now(),

                volatility=VolatilityDashboard(
                    spot=mk_item(vol.spot, field_name="spot"),
                    vix=mk_item(vol.vix, "IVP", field_name="vix"),
                    vov=mk_item(vol.vov, "VOV", "%", field_name="vov"),
                    ivp_30=mk_item(vol.ivp30, "IVP", "%", field_name="ivp30"),
                    ivp_90=mk_item(vol.ivp90, "IVP", "%", field_name="ivp90"),
                    ivp_1y=mk_item(vol.ivp1y, "IVP", "%", field_name="ivp1y"),
                    rv_7_28=mk_item(vol.rv7, field_name="rv7"),
                    garch_7_28=mk_item(vol.garch7, field_name="garch7"),
                    parkinson_7_28=mk_item(vol.pk7, field_name="pk7"),
                    is_fallback=vol.is_fallback,
                ),

                edges=EdgeDashboard(
                    iv_weekly=mk_item(ed.iv_weekly, "%", field_name="iv_weekly"),
                    iv_monthly=mk_item(ed.iv_monthly, "%", field_name="iv_monthly"),
                    vrp_rv_w=mk_item(ed.vrp_rv_w, "VRP"),
                    vrp_rv_m=mk_item(ed.vrp_rv_m, "VRP"),
                    vrp_ga_w=mk_item(ed.vrp_garch_w, "VRP"),
                    vrp_ga_m=mk_item(ed.vrp_garch_m, "VRP"),
                    vrp_pk_w=mk_item(ed.vrp_pk_w, "VRP"),
                    vrp_pk_m=mk_item(ed.vrp_pk_m, "VRP"),
                    term_structure=mk_item(ed.term_structure, "TERM"),
                ),

                structure=StructureDashboard(
                    net_gex=mk_item(st.net_gex),
                    pcr=mk_item(st.pcr),
                    max_pain=mk_item(st.max_pain),
                    skew_25d=mk_item(st.skew, "SKEW", "%"),
                ),

                scores=ScoresDashboard(
                    vol_score=sanitize_float(reg.v_scr),
                    struct_score=sanitize_float(reg.s_scr),
                    edge_score=sanitize_float(reg.e_scr),
                    risk_score=sanitize_float(reg.r_scr),
                    total_score=sanitize_float(reg.score),
                ),

                external={"fast_vol": False},

                capital=CapitalDashboard(
                    regime_name=reg.name,
                    primary_edge=reg.primary_edge,
                    allocation_pct=sanitize_float(reg.alloc_pct),
                    max_lots=int(sanitize_float(reg.max_lots)),
                    recommendation=f"Allocate {reg.alloc_pct*100:.0f}% Capital ({int(reg.max_lots)} lots)",
                ),
            )

            if is_valid:
                await cache.set(cache_key, result.json(), ttl=60)

            elapsed = asyncio.get_event_loop().time() - start_time
            logger.info(f"Analysis completed in {elapsed*1000:.0f}ms")

            return result

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Analysis timeout")

    except Exception as e:
        logger.error(f"Dashboard failure: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Market analysis temporarily unavailable"
            if settings.ENVIRONMENT.startswith("production")
            else str(e),
            )
