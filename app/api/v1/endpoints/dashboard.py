from fastapi import APIRouter, HTTPException
from datetime import datetime
from app.config import settings
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.edge import EdgeEngine
from app.core.analytics.regime import RegimeEngine
from app.schemas.analytics import FullAnalysisResponse, ExtMetrics, MetricItem
from app.schemas.analytics import (
    VolatilityDashboard, EdgeDashboard, StructureDashboard, ScoresDashboard, CapitalDashboard
)
import asyncio
import logging

# [span_29](start_span)[span_30](start_span)Derived from[span_29](end_span)[span_30](end_span)

router = APIRouter()
logger = logging.getLogger(__name__)

# -[span_31](start_span)-- Helper for Frontend Tags[span_31](end_span) ---
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
        return ("INV", "red") if val < 0 else ("NRML", "green")
    return "-", "default"

def mk_item(val, type_tag=None, suffix=""):
    v_str = f"{val:.2f}{suffix}" if isinstance(val, (float, int)) else str(val)
    tag, color = get_tag_meta(val, type_tag) if type_tag and isinstance(val, (int, float)) else ("-", "default")
    return MetricItem(value=val, formatted=v_str, tag=tag, color=color)

@router.post("/analyze", response_model=FullAnalysisResponse)
async def analyze_market_state():
    [span_32](start_span)"""On-demand full market analysis[span_32](end_span)"""
    client = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN, 
        settings.UPSTOX_BASE_V2, 
        settings.UPSTOX_BASE_V3
    )
    vol_engine = VolatilityEngine()
    struct_engine = StructureEngine()
    edge_engine = EdgeEngine()
    regime_engine = RegimeEngine()
    
    try:
        # 1. [span_33](start_span)Parallel Data Fetching[span_33](end_span)
        task_nh = client.get_history(NIFTY_KEY)
        task_vh = client.get_history(VIX_KEY)
        task_live = client.get_live_quote([NIFTY_KEY, VIX_KEY])
        task_exp = client.get_expiries_and_lot()
        
        nh, vh, live_data, (we, me, lot) = await asyncio.gather(
            task_nh, task_vh, task_live, task_exp
        )
        
        if nh.empty or not we:
            raise HTTPException(status_code=500, detail="Data Fetch Failed")

        wc, mc = await asyncio.gather(client.fetch_chain_data(we), client.fetch_chain_data(me))
        
        # 2. [span_34](start_span)Logic Calculation[span_34](end_span)
        spot_val = live_data.get(NIFTY_KEY, 0)
        vix_val = live_data.get(VIX_KEY, 0)
        
        vol = await vol_engine.calculate_volatility(nh, vh, spot_val, vix_val)
        st = struct_engine.analyze_structure(wc, vol.spot, lot)
        ed = edge_engine.detect_edges(wc, mc, vol.spot, vol)
        
        # External Metrics (Mock)
        fast = False
        if len(nh) > 0:
            l = nh.iloc[-1]
            fast = ((l['high']-l['low'])/l['open']*100) > 1.5
            
        ext = ExtMetrics(1500, -500, 1, ["RBI Policy"], fast)
        reg = regime_engine.calculate_regime(vol, st, ed, ext)
        
        # 3. [span_35](start_span)Response Construction[span_35](end_span)
        return FullAnalysisResponse(
            timestamp=datetime.now(),
            volatility=VolatilityDashboard(
                spot=mk_item(vol.spot),
                vix=mk_item(vol.vix, "IVP"),
                vov=mk_item(vol.vov, "VOV", "%"),
                ivp_30=mk_item(vol.ivp30, "IVP", "%"),
                ivp_90=mk_item(vol.ivp90, "IVP", "%"),
                ivp_1y=mk_item(vol.ivp1y, "IVP", "%"),
                rv_7_28=MetricItem(value=f"{vol.rv7:.2f}/{vol.rv28:.2f}", formatted=f"{vol.rv7:.2f}/{vol.rv28:.2f}", tag="-", color="default"),
                garch_7_28=MetricItem(value=f"{vol.ga7:.2f}/{vol.ga28:.2f}", formatted=f"{vol.ga7:.2f}/{vol.ga28:.2f}", tag="-", color="default"),
                parkinson_7_28=MetricItem(value=f"{vol.pk7:.2f}/{vol.pk28:.2f}", formatted=f"{vol.pk7:.2f}/{vol.pk28:.2f}", tag="-", color="default"),
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
                net_gex=MetricItem(value=st.net_gex, formatted=f"₹{st.net_gex/1e7:.2f} Cr", tag=st.gex_regime, color="default"),
                pcr=MetricItem(value=st.pcr, formatted=f"{st.pcr:.2f}", tag=st.regime, color="default"),
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
            external={"fii_net": ext.fii, "dii_net": ext.dii, "events": ext.events, "fast_vol": ext.fast_vol},
            capital=CapitalDashboard(
                regime_name=reg.name,
                primary_edge=reg.primary,
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
