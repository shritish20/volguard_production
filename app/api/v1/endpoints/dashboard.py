from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any
from datetime import date, datetime
import asyncio
import pandas as pd

# 1. The Brains (Engines)
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.edge import EdgeEngine
from app.core.analytics.regime import RegimeEngine
from app.core.market.participant_client import ParticipantClient

# 2. The Data Sources
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.services.instrument_registry import registry 
from app.dependencies import get_market_client

# 3. Utils
from app.utils.logger import logger

router = APIRouter()

# Initialize Engines (Singletons)
# These hold the logic (Kill Switch, Weighted VRP, GEX Filters)
vol_engine = VolatilityEngine()
struct_engine = StructureEngine()
edge_engine = EdgeEngine()
regime_engine = RegimeEngine()
participant_client = ParticipantClient()

@router.get("/analysis", response_model=Dict[str, Any])
async def get_market_analysis(
    market: MarketDataClient = Depends(get_market_client)
):
    """
    VolGuard 4.1 Master Dashboard.
    Returns the complete 360-degree view (Weekly vs Monthly) matching the v30.1 Script.
    """
    try:
        logger.info("Starting VolGuard 4.1 Market Analysis...")
        start_time = datetime.now()

        # ==================================================================
        # 1. FETCH AUTHORITATIVE EXPIRIES (FROM REGISTRY)
        # ==================================================================
        # This uses the Instrument Master you downloaded from Upstox
        # It ensures the dashboard sees the exact same contracts as the execution engine
        weekly_exp, monthly_exp = registry.get_nifty_expiries()
        
        if not weekly_exp or not monthly_exp:
            # Fallback if registry isn't fully loaded yet (e.g. startup)
            today = date.today()
            logger.warning("Registry not ready, using fallback dates")
            weekly_exp = today
            monthly_exp = today

        # Get Contract Specs (Lot Size) to ensure GEX calc is accurate
        specs = registry.get_nifty_contract_specs(weekly_exp)
        lot_size = specs.get("lot_size", 50)

        # Calculate DTEs
        today_date = date.today()
        dte_w = (weekly_exp - today_date).days
        dte_m = (monthly_exp - today_date).days

        # ==================================================================
        # 2. PARALLEL DATA FETCHING (ASYNC)
        # ==================================================================
        # We fetch NSE flow, Historical Data, and Option Chains simultaneously
        
        # A. Market Data Tasks (Upstox)
        hist_task = market.get_daily_candles(NIFTY_KEY, days=400)
        vix_task = market.get_daily_candles(VIX_KEY, days=400)
        live_task = market.get_live_quote([NIFTY_KEY, VIX_KEY])
        
        # B. Option Chain Tasks (Specific Expiries)
        chain_w_task = market.get_option_chain(weekly_exp.strftime("%Y-%m-%d"))
        chain_m_task = market.get_option_chain(monthly_exp.strftime("%Y-%m-%d"))
        
        # C. Participant Data Task (NSE Scraper - Async Wrapper)
        fii_task = participant_client.fetch_metrics()

        # Execute parallel wait
        nifty_hist, vix_hist, live_data, chain_w, chain_m, ext_metrics = await asyncio.gather(
            hist_task, vix_task, live_task, chain_w_task, chain_m_task, fii_task
        )

        # ==================================================================
        # 3. ANALYTICAL PIPELINE
        # ==================================================================
        
        # A. Volatility (Global)
        # Calculates VoV Z-Score, GARCH, Parkinson, and Kill Switch
        spot = live_data.get(NIFTY_KEY, 0)
        vix = live_data.get(VIX_KEY, 0)
        
        # Note: Vol engine runs heavy math in a thread
        vol_metrics = await vol_engine.analyze(nifty_hist, vix_hist, spot, vix)

        # B. Structure (Weekly vs Monthly)
        # Calculates Net GEX, Max Pain, PCR, and Sticky/Slippery Regimes
        struct_w = struct_engine.calculate_structure(chain_w, spot, lot_size)
        struct_m = struct_engine.calculate_structure(chain_m, spot, lot_size)

        # C. Edges (Global)
        # Calculates Weighted VRP (70/15/15) and Term Structure
        edge_metrics = edge_engine.calculate_edge(vol_metrics, chain_w, chain_m)

        # D. Regime Scoring (The "Brain")
        # We run the regime engine twice to get distinct mandates for different horizons
        
        # Weekly Mandate (Gamma Focus)
        weekly_mandate = regime_engine.analyze_regime(
            vol_metrics, struct_w, edge_metrics, ext_metrics, "WEEKLY", dte_w
        )
        
        # Monthly Mandate (Vega Focus)
        monthly_mandate = regime_engine.analyze_regime(
            vol_metrics, struct_m, edge_metrics, ext_metrics, "MONTHLY", dte_m
        )

        # ==================================================================
        # 4. CONSTRUCT THE "GOD VIEW" RESPONSE
        # ==================================================================
        elapsed = (datetime.now() - start_time).total_seconds()
        
        response = {
            "meta": {
                "version": "VolGuard 4.1",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "analysis_time_sec": round(elapsed, 2),
                "data_source": "Upstox V3 + NSE Live",
                "status": "ONLINE"
            },
            
            # --- 1. TIME CONTEXT ---
            "time_context": {
                "current_date": str(today_date),
                "weekly_expiry": str(weekly_exp),
                "monthly_expiry": str(monthly_exp),
                "dte_weekly": dte_w,
                "dte_monthly": dte_m,
                "is_gamma_danger": dte_w <= 1
            },

            # --- 2. VOLATILITY ANALYSIS (The Kill Switch) ---
            "volatility": {
                "spot": vol_metrics.spot,
                "vix": vol_metrics.vix,
                "trend_strength": round(vol_metrics.trend_strength, 2),
                "metrics": {
                    "vov_zscore": round(vol_metrics.vov_zscore, 2), # <--- Primary Filter
                    "ivp_30d": round(vol_metrics.ivp_30d, 1),
                    "ivp_1yr": round(vol_metrics.ivp_1yr, 1),
                    "rv_7d": round(vol_metrics.rv7, 1),
                    "garch_7d": round(vol_metrics.garch7, 1),
                    "parkinson_7d": round(vol_metrics.park7, 1)
                },
                "regime": vol_metrics.vol_regime, # EXPLODING / RICH / CHEAP
                "kill_switch_active": vol_metrics.vov_zscore > 2.5
            },

            # --- 3. PARTICIPANT DATA (FII FLOW) ---
            "external_flow": {
                "flow_regime": ext_metrics.flow_regime, # STRONG_SHORT / LONG
                "event_risk": ext_metrics.event_risk,   # HIGH / LOW (Manual Calendar)
                "fii_net_change": ext_metrics.fii_net_change,
                "positions": {
                    "FII": _format_participant(ext_metrics.fii),
                    "DII": _format_participant(ext_metrics.dii),
                    "PRO": _format_participant(ext_metrics.pro),
                    "CLIENT": _format_participant(ext_metrics.client)
                }
            },

            # --- 4. MARKET STRUCTURE (WEEKLY vs MONTHLY) ---
            "market_structure": {
                "weekly": {
                    "net_gex_cr": round(struct_w.net_gex / 10000000, 2),
                    "gex_regime": struct_w.gex_regime,
                    "pcr": round(struct_w.pcr, 2),
                    "max_pain": struct_w.max_pain,
                    "skew": round(struct_w.skew_25d, 2)
                },
                "monthly": {
                    "net_gex_cr": round(struct_m.net_gex / 10000000, 2),
                    "gex_regime": struct_m.gex_regime,
                    "pcr": round(struct_m.pcr, 2),
                    "max_pain": struct_m.max_pain
                }
            },

            # --- 5. OPTION EDGES (Weighted VRP) ---
            "edges": {
                "term_structure": edge_metrics.term_regime, # BACKWARDATION / CONTANGO
                "term_spread": round(edge_metrics.term_spread, 2),
                "weekly_edge": {
                    "atm_iv": round(edge_metrics.iv_weekly, 2),
                    "weighted_vrp": round(edge_metrics.vrp_weighted_weekly, 2),
                    "raw_vrp_garch": round(edge_metrics.vrp_garch_weekly, 2)
                },
                "monthly_edge": {
                    "atm_iv": round(edge_metrics.iv_monthly, 2),
                    "weighted_vrp": round(edge_metrics.vrp_weighted_monthly, 2)
                },
                "primary_opportunity": edge_metrics.primary_edge
            },

            # --- 6. TRADING MANDATES (THE OUTPUT) ---
            "mandates": {
                "WEEKLY": {
                    "regime": weekly_mandate.regime_name,
                    "strategy": weekly_mandate.strategy_type,
                    "allocation": f"{weekly_mandate.allocation_pct}%",
                    "max_lots": weekly_mandate.max_lots,
                    "rationale": weekly_mandate.rationale,
                    "warnings": weekly_mandate.warnings
                },
                "MONTHLY": {
                    "regime": monthly_mandate.regime_name,
                    "strategy": monthly_mandate.strategy_type,
                    "allocation": f"{monthly_mandate.allocation_pct}%",
                    "max_lots": monthly_mandate.max_lots,
                    "rationale": monthly_mandate.rationale
                }
            },
            
            # --- 7. COMPARATIVE SUMMARY ---
            "recommendation": _generate_recommendation(weekly_mandate, monthly_mandate)
        }

        return response

    except Exception as e:
        logger.error(f"Dashboard Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================================================================
# HELPER FUNCTIONS (Formatting & logic)
# ==================================================================

def _format_participant(data):
    if not data: return None
    return {
        "fut_net": data.fut_net,
        "call_net": data.call_net,
        "put_net": data.put_net,
        "bias": "BULLISH" if data.fut_net > 0 else "BEARISH"
    }

def _generate_recommendation(weekly, monthly):
    """Simple logic to pick the winner based on allocation Score"""
    # If both are CASH, stay out
    if weekly.regime_name == "CASH" and monthly.regime_name == "CASH":
        return "STAY CASH: No favorable regime found."
    
    # Pick the higher allocation
    if weekly.allocation_pct >= monthly.allocation_pct:
        winner = "WEEKLY"
        score = weekly.allocation_pct
    else:
        winner = "MONTHLY" 
        score = monthly.allocation_pct
        
    return f"FOCUS ON {winner} ({score}% Allocation). {weekly.regime_name} vs {monthly.regime_name}."
