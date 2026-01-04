# app/api/v1/endpoints/dashboard.py

from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime
import asyncio
import logging

from app.dependencies import get_market_client, get_persistence_service
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.services.persistence import PersistenceService

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

# --- Helper for Frontend Tags ---
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
    return "-", "default"

def mk_item(val, type_tag=None, suffix=""):
    v_str = f"{val:.2f}{suffix}" if isinstance(val, (float, int)) else str(val)
    tag, color = get_tag_meta(val, type_tag) if type_tag and isinstance(val, (int, float)) else ("-", "default")
    return MetricItem(value=val, formatted=v_str, tag=tag, color=color)

@router.post("/analyze", response_model=FullAnalysisResponse)
async def analyze_market_state(
    market: MarketDataClient = Depends(get_market_client),
    db: PersistenceService = Depends(get_persistence_service)
):
    """
    On-demand full market analysis using the VolGuard 3.0 Hybrid Engine.
    Uses Postgres for History (Tier 1) and API for Live (Tier 3).
    """
    
    # Initialize Engines
    vol_engine = VolatilityEngine()
    struct_engine = StructureEngine()
    edge_engine = EdgeEngine()
    regime_engine = RegimeEngine()

    try:
        # 1. PARALLEL DATA FETCH (Smart Hybrid)
        # We try to load history from DB first (fast), then refresh intraday
        
        # A. History (Tier 1 - Postgres)
        nh_task = db.load_daily_history(NIFTY_KEY, days=365)
        vh_task = db.load_daily_history(VIX_KEY, days=365)
        
        # B. Intraday & Live (Tier 2 & 3 - API)
        intra_task = market.get_intraday_candles(NIFTY_KEY)
        live_task = market.get_live_quote([NIFTY_KEY, VIX_KEY])
        exp_task = market.get_expiries() # Returns tuple (weekly, monthly)

        # Execute Gather
        nh, vh, intra, live_data, (we, me) = await asyncio.gather(
            nh_task, vh_task, intra_task, live_task, exp_task
        )

        # Fallback: If DB is empty, fetch from API directly (Cold Start)
        if nh.empty:
            nh = await market.get_daily_candles(NIFTY_KEY)
        if vh.empty:
            vh = await market.get_daily_candles(VIX_KEY)

        if nh.empty or not we:
            raise HTTPException(status_code=500, detail="Data Fetch Failed (History or Expiry missing)")

        # C. Fetch Option Chains (Tier 2)
        # We need chains for both Weekly and Monthly for Edge detection
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
        # This merges Daily (DB) + Intraday (API) for real-time Parkinson Vol
        vol = await vol_engine.calculate_volatility(nh, intra, spot_val, vix_val)
        
        # B. Structure
        # Get dynamic lot size
        contract_details = await market.get_contract_details("NIFTY")
        lot_size = contract_details.get("lot_size", 50)
        
        st = struct_engine.analyze_structure(wc, vol.spot, lot_size)
        
        # C. Edge
        ed = edge_engine.detect_edges(wc, mc, vol.spot, vol)

        # D. External (Placeholder / Simple Technicals)
        # In V3, we calculate 'Fast Vol' from Intraday data if available
        fast_vol = False
        if not intra.empty:
            # Check last 15 mins for rapid moves
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

        # 3. Response Construction
        return FullAnalysisResponse(
            timestamp=datetime.now(),
            
            volatility=VolatilityDashboard(
                spot=mk_item(vol.spot),
                vix=mk_item(vol.vix, "IVP"),
                vov=mk_item(vol.vov, "VOV", "%"),
                ivp_30=mk_item(vol.ivp30, "IVP", "%"),
                ivp_90=mk_item(vol.ivp90, "IVP", "%"),
                ivp_1y=mk_item(vol.ivp1y, "IVP", "%"),
                rv_7_28=mk_item(vol.rv7),
                garch_7_28=mk_item(vol.garch7), # Corrected field name from engine
                parkinson_7_28=mk_item(vol.pk7),
                is_fallback=vol.is_fallback
            ),
            
            edges=EdgeDashboard(
                iv_weekly=mk_item(ed.iv_weekly, suffix="%"), # Corrected field name from engine
                iv_monthly=mk_item(ed.iv_monthly, suffix="%"),
                vrp_rv_w=mk_item(ed.vrp_rv_w, "VRP"),
                vrp_rv_m=mk_item(ed.vrp_rv_m, "VRP"),
                vrp_ga_w=mk_item(ed.vrp_garch_w, "VRP"), # Corrected field name
                vrp_ga_m=mk_item(ed.vrp_garch_m, "VRP"),
                vrp_pk_w=mk_item(ed.vrp_pk_w, "VRP"),
                vrp_pk_m=mk_item(ed.vrp_pk_m, "VRP"),
                term_structure=mk_item(ed.term_structure, "TERM")
            ),
            
            structure=StructureDashboard(
                net_gex=mk_item(st.net_gex),
                pcr=mk_item(st.pcr),
                max_pain=mk_item(st.max_pain),
                skew_25d=mk_item(st.skew, "SKEW", "%")
            ),
            
            scores=ScoresDashboard(
                vol_score=reg.v_scr,
                struct_score=reg.s_scr,
                edge_score=reg.e_scr,
                risk_score=reg.r_scr,
                total_score=reg.score
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
                allocation_pct=reg.alloc_pct,
                max_lots=reg.max_lots,
                recommendation=f"Allocate {reg.alloc_pct*100:.0f}% Capital ({reg.max_lots} lots)"
            )
        )

    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        # Don't expose raw stack traces in production, but for debugging it's useful
        raise HTTPException(status_code=500, detail=f"Analysis Error: {str(e)}")
