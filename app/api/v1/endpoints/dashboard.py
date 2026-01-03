from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime
from app.config import settings
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.edge import EdgeEngine
from app.core.analytics.regime import RegimeEngine
from app.schemas.analytics import (
    FullAnalysisResponse, VolatilityDashboard, EdgeDashboard, 
    StructureDashboard, ScoresDashboard, CapitalDashboard, MetricItem, ExtMetrics
)
import asyncio
import logging

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
async def analyze_market_state():
    """
    On-demand full market analysis.
    Returns the exact state of the 'Brain' including Scoring Weights.
    """
    client = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3
    )
    
    # Initialize Engines
    vol_engine = VolatilityEngine()
    struct_engine = StructureEngine()
    edge_engine = EdgeEngine()
    regime_engine = RegimeEngine()

    try:
        # 1. Parallel Data Fetching
        task_nh = client.get_history(NIFTY_KEY)
        task_vh = client.get_history(VIX_KEY)
        task_live = client.get_live_quote([NIFTY_KEY, VIX_KEY])
        task_exp = client.get_expiries_and_lot()

        nh, vh, live_data, (we, me, lot) = await asyncio.gather(
            task_nh, task_vh, task_live, task_exp
        )

        if nh.empty or not we:
            raise HTTPException(status_code=500, detail="Data Fetch Failed")

        # Fetch Chains
        wc, mc = await asyncio.gather(client.get_option_chain(we), client.get_option_chain(me))

        # 2. Logic Calculation (The Brain)
        spot_val = live_data.get(NIFTY_KEY, 0)
        vix_val = live_data.get(VIX_KEY, 0)

        # A. Volatility
        vol = await vol_engine.calculate_volatility(nh, vh, spot_val, vix_val)
        
        # B. Structure
        st = struct_engine.analyze_structure(wc, vol.spot, lot)
        
        # C. Edge
        ed = edge_engine.detect_edges(wc, mc, vol.spot, vol)

        # D. External (Mock for now, replace with real news feed later)
        fast = False
        if len(nh) > 0:
            l = nh.iloc[-1]
            fast = ((l['high']-l['low'])/l['open']*100) > 1.5
        ext = ExtMetrics(1500, 500, 1, ["RBI Policy"], fast)

        # E. Regime (The Decision)
        reg = regime_engine.calculate_regime(vol, st, ed, ext)

        # 3. Response Construction (Exposing the Data)
        return FullAnalysisResponse(
            timestamp=datetime.now(),
            
            volatility=VolatilityDashboard(
                spot=mk_item(vol.spot),
                vix=mk_item(vol.vix, "IVP"),
                vov=mk_item(vol.vov, "VOV", "%"),
                ivp_30=mk_item(vol.ivp30, "IVP", "%"),
                ivp_90=mk_item(vol.ivp90, "IVP", "%"),
                ivp_1y=mk_item(vol.ivp1y, "IVP", "%"),
                rv_7_28=mk_item(vol.rv7), # Simplified for dashboard
                garch_7_28=mk_item(vol.ga7),
                parkinson_7_28=mk_item(vol.pk7),
                is_fallback=vol.is_fallback
            ),
            
            edges=EdgeDashboard(
                iv_weekly=mk_item(ed.iv_w, suffix="%"),
                iv_monthly=mk_item(ed.iv_m, suffix="%"),
                vrp_rv_w=mk_item(ed.vrp_rv_w, "VRP"),
                vrp_rv_m=mk_item(ed.vrp_rv_m, "VRP"),
                vrp_ga_w=mk_item(ed.vrp_ga_w, "VRP"),
                vrp_ga_m=mk_item(ed.vrp_ga_m, "VRP"),
                vrp_pk_w=mk_item(ed.vrp_pk_w, "VRP"),
                vrp_pk_m=mk_item(ed.vrp_pk_m, "VRP"),
                term_structure=mk_item(ed.term, "TERM")
            ),
            
            structure=StructureDashboard(
                net_gex=mk_item(st.net_gex),
                pcr=mk_item(st.pcr),
                max_pain=mk_item(st.max_pain),
                skew_25d=mk_item(st.skew, "SKEW", "%")
            ),
            
            # CRITICAL: This exposes the "Why" (Weights & Scores)
            scores=ScoresDashboard(
                vol_score=reg.v_scr,      # The 40% component
                struct_score=reg.s_scr,   # The 30% component
                edge_score=reg.e_scr,     # The 20% component
                risk_score=reg.r_scr,     # The 10% component
                total_score=reg.score     # The Final Decision (e.g. 7.2)
            ),
            
            external={
                "fii_net": ext.fii, 
                "dii_net": ext.dii, 
                "events": ext.events, 
                "fast_vol": ext.fast_vol
            },
            
            capital=CapitalDashboard(
                regime_name=reg.name,       # e.g. "AGGRESSIVE_SHORT"
                primary_edge=reg.primary,   # e.g. "SHORT_GAMMA"
                allocation_pct=reg.alloc_pct,
                max_lots=reg.max_lots,
                recommendation=f"Allocate {reg.alloc_pct}% Capital ({reg.max_lots} lots)"
            )
        )

    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analysis Error: {str(e)}")
        
    finally:
        await client.close()
