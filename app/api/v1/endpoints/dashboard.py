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

# --- Validation Helper ---
def is_valid_float(val):
    """Check if a float value is valid (not infinity or NaN)"""
    if not isinstance(val, (int, float)):
        return True
    return not (math.isinf(val) or math.isnan(val))

def sanitize_float(val, default=0.0, field_name="unknown"):
    """
    Sanitize float values, log errors, and return safe defaults.
    This prevents JSON serialization crashes while alerting about calculation issues.
    """
    if not isinstance(val, (int, float)):
        return val
    
    if math.isinf(val) or math.isnan(val):
        logger.error(f"Invalid calculation detected for {field_name}: {val}")
        return default
    
    return val

# --- Helper for Frontend Tags ---
def get_tag_meta(val, type_):
    """Generate UI tags and colors based on metric thresholds"""
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
    """
    Creates a standardized metric item for the dashboard.
    Includes robust protection against invalid float values.
    """
    # Sanitize numeric values
    if isinstance(val, (int, float)):
        val = sanitize_float(val, default=0.0, field_name=field_name)
        v_str = f"{val:.2f}{suffix}"
    else:
        v_str = str(val)
    
    # Generate tags for valid numeric values
    tag, color = get_tag_meta(val, type_tag) if type_tag and isinstance(val, (int, float)) else ("-", "default")
    
    return MetricItem(value=val, formatted=v_str, tag=tag, color=color)

def validate_response_data(vol, st, ed, reg):
    """
    Validate critical metrics before caching.
    Returns (is_valid, error_fields) tuple.
    """
    error_fields = []
    
    # Check volatility metrics
    critical_vol = [
        (vol.spot, "spot"),
        (vol.vix, "vix"),
        (vol.ivp30, "ivp30"),
        (vol.rv7, "rv7")
    ]
    
    # Check edge metrics
    critical_edge = [
        (ed.iv_weekly, "iv_weekly"),
        (ed.vrp_rv_w, "vrp_rv_w"),
        (ed.term_structure, "term_structure")
    ]
    
    # Check structure metrics
    critical_struct = [
        (st.net_gex, "net_gex"),
        (st.pcr, "pcr"),
        (st.max_pain, "max_pain")
    ]
    
    # Check regime scores
    critical_regime = [
        (reg.v_scr, "vol_score"),
        (reg.s_scr, "struct_score"),
        (reg.e_scr, "edge_score"),
        (reg.score, "total_score"),
        (reg.alloc_pct, "alloc_pct")
    ]
    
    all_critical = critical_vol + critical_edge + critical_struct + critical_regime
    
    for val, name in all_critical:
        if not is_valid_float(val):
            error_fields.append(name)
    
    return len(error_fields) == 0, error_fields

@router.post("/analyze", response_model=FullAnalysisResponse)
async def analyze_market_state(
    request: Request,
    market: MarketDataClient = Depends(get_market_client),
    db: PersistenceService = Depends(get_persistence_service)
):
    """
    On-demand full market analysis using the VolGuard 3.0 Hybrid Engine.
    Uses Postgres for History (Tier 1) and API for Live (Tier 3).
    
    ENHANCEMENTS:
    - ✅ Redis caching (60s TTL) with validation
    - ✅ Timeout protection (30s max)
    - ✅ Safe error handling (no info leaks)
    - ✅ Performance monitoring
    - ✅ Comprehensive NaN/Infinity protection
    - ✅ Cache poisoning prevention
    """
    
    start_time = asyncio.get_event_loop().time()
    
    # ========================================
    # 🚀 ENHANCEMENT #1: REDIS CACHING
    # ========================================
    cache_key = f"dashboard:analysis:{datetime.now().strftime('%Y%m%d%H%M')}"
    
    try:
        cached = await cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for {cache_key}")
            return FullAnalysisResponse.parse_raw(cached)
    except Exception as e:
        logger.warning(f"Cache read failed: {e}")
    
    # Initialize Engines
    vol_engine = VolatilityEngine()
    struct_engine = StructureEngine()
    edge_engine = EdgeEngine()
    regime_engine = RegimeEngine()

    try:
        # ========================================
        # 🛡️ ENHANCEMENT #2: TIMEOUT PROTECTION
        # ========================================
        async with asyncio.timeout(30):
            
            # 1. PARALLEL DATA FETCH (Smart Hybrid)
            
            # A. History (Tier 1 - Postgres)
            nh_task = db.load_daily_history(NIFTY_KEY, days=365)
            vh_task = db.load_daily_history(VIX_KEY, days=365)
            
            # B. Intraday & Live (Tier 2 & 3 - API)
            intra_task = market.get_intraday_candles(NIFTY_KEY)
            live_task = market.get_live_quote([NIFTY_KEY, VIX_KEY])
            exp_task = market.get_expiries()

            # Execute Gather
            nh, vh, intra, live_data, (we, me) = await asyncio.gather(
                nh_task, vh_task, intra_task, live_task, exp_task
            )

            # Fallback: If DB is empty, fetch from API (Cold Start)
            if nh.empty:
                logger.warning("DB history empty, fetching from API")
                nh = await market.get_daily_candles(NIFTY_KEY)
            if vh.empty:
                logger.warning("VIX history empty, fetching from API")
                vh = await market.get_daily_candles(VIX_KEY)

            if nh.empty or not we:
                raise HTTPException(
                    status_code=500, 
                    detail="Data Fetch Failed (History or Expiry missing)"
                )

            # C. Fetch Option Chains (Tier 2)
            chain_tasks = [market.get_option_chain(we)]
            if me:
                chain_tasks.append(market.get_option_chain(me))
            
            chains = await asyncio.gather(*chain_tasks)
            wc = chains[0]
            mc = chains[1] if len(chains) > 1 else None

            # 2. LOGIC CALCULATION (The Brain)
            spot_val = live_data.get(NIFTY_KEY, 0)
            vix_val = live_data.get(VIX_KEY, 0)

            # A. Volatility (Hybrid Calculation)
            vol = await vol_engine.calculate_volatility(nh, intra, spot_val, vix_val)
            
            # B. Structure
            contract_details = await market.get_contract_details("NIFTY")
            lot_size = contract_details.get("lot_size", 50)
            st = struct_engine.analyze_structure(wc, vol.spot, lot_size)
            
            # C. Edge
            ed = edge_engine.detect_edges(wc, mc, vol.spot, vol)

            # D. External Metrics
            fast_vol = False
            if not intra.empty:
                recent = intra.tail(15)
                if not recent.empty:
                    high = recent['high'].max()
                    low = recent['low'].min()
                    open_ = recent.iloc[0]['open']
                    if open_ > 0 and ((high - low) / open_ * 100) > 1.5:
                        fast_vol = True

            ext = ExtMetrics(fii=0, dii=0, events=0, event_names=[], fast_vol=fast_vol)

            # E. Regime (The Decision)
            reg = regime_engine.calculate_regime(vol, st, ed, ext)

            # ========================================
            # 🔍 ENHANCEMENT #6: DATA VALIDATION
            # ========================================
            is_valid, error_fields = validate_response_data(vol, st, ed, reg)
            if not is_valid:
                logger.error(f"Invalid calculations detected in fields: {error_fields}")
                # Don't cache invalid data

            # 3. Response Construction with Sanitization
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
                    is_fallback=vol.is_fallback
                ),
                
                edges=EdgeDashboard(
                    iv_weekly=mk_item(ed.iv_weekly, suffix="%", field_name="iv_weekly"),
                    iv_monthly=mk_item(ed.iv_monthly, suffix="%", field_name="iv_monthly"),
                    vrp_rv_w=mk_item(ed.vrp_rv_w, "VRP", field_name="vrp_rv_w"),
                    vrp_rv_m=mk_item(ed.vrp_rv_m, "VRP", field_name="vrp_rv_m"),
                    vrp_ga_w=mk_item(ed.vrp_garch_w, "VRP", field_name="vrp_garch_w"),
                    vrp_ga_m=mk_item(ed.vrp_garch_m, "VRP", field_name="vrp_garch_m"),
                    vrp_pk_w=mk_item(ed.vrp_pk_w, "VRP", field_name="vrp_pk_w"),
                    vrp_pk_m=mk_item(ed.vrp_pk_m, "VRP", field_name="vrp_pk_m"),
                    term_structure=mk_item(ed.term_structure, "TERM", field_name="term_structure")
                ),
                
                structure=StructureDashboard(
                    net_gex=mk_item(st.net_gex, field_name="net_gex"),
                    pcr=mk_item(st.pcr, field_name="pcr"),
                    max_pain=mk_item(st.max_pain, field_name="max_pain"),
                    skew_25d=mk_item(st.skew, "SKEW", "%", field_name="skew")
                ),
                
                scores=ScoresDashboard(
                    vol_score=sanitize_float(reg.v_scr, 0, "vol_score"),
                    struct_score=sanitize_float(reg.s_scr, 0, "struct_score"),
                    edge_score=sanitize_float(reg.e_scr, 0, "edge_score"),
                    risk_score=sanitize_float(reg.r_scr, 0, "risk_score"),
                    total_score=sanitize_float(reg.score, 0, "total_score")
                ),
                
                external={
                    "fii_net": ext.fii, 
                    "dii_net": ext.dii, 
                    "events": ext.events, 
                    "fast_vol": ext.fast_vol
                },
                
                capital=CapitalDashboard(
                    regime_name=reg.name,
                    primary_edge=reg.primary_edge,
                    allocation_pct=sanitize_float(reg.alloc_pct, 0.0, "alloc_pct"),
                    max_lots=int(sanitize_float(reg.max_lots, 0, "max_lots")),
                    recommendation=f"Allocate {sanitize_float(reg.alloc_pct, 0.0, 'alloc_pct')*100:.0f}% Capital ({int(sanitize_float(reg.max_lots, 0, 'max_lots'))} lots)"
                )
            )
            
            # ========================================
            # 🚀 ENHANCEMENT #3: VALIDATED CACHING
            # ========================================
            if is_valid:
                try:
                    await cache.set(cache_key, result.json(), ttl=60)
                    logger.info(f"Cached validated result for {cache_key}")
                except Exception as e:
                    logger.warning(f"Cache write failed: {e}")
            else:
                logger.warning(f"Skipping cache due to invalid data: {error_fields}")
            
            # ========================================
            # 📊 ENHANCEMENT #4: PERFORMANCE MONITORING
            # ========================================
            elapsed = asyncio.get_event_loop().time() - start_time
            logger.info(f"Analysis completed in {elapsed*1000:.0f}ms")
            
            return result
    
    except asyncio.TimeoutError:
        logger.error("Analysis timeout after 30 seconds")
        raise HTTPException(
            status_code=504, 
            detail="Market data processing timeout - please retry"
        )
    
    except HTTPException:
        raise
    
    except Exception as e:
        # ========================================
        # 🛡️ ENHANCEMENT #5: SAFE ERROR HANDLING
        # ========================================
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        
        if settings.ENVIRONMENT in ["production_live", "production_semi"]:
            raise HTTPException(
                status_code=500, 
                detail="Market analysis temporarily unavailable. Please try again."
            )
        else:
            raise HTTPException(
                status_code=500, 
                detail=f"Analysis Error: {str(e)}"
)
