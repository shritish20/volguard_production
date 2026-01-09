# app/api/v1/endpoints/dashboard.py

from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List
from datetime import date, datetime
import asyncio
import pandas as pd
import httpx

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

# 3. NEW: Token Manager for authenticated API calls
from app.core.auth.token_manager import TokenManager

# 4. Utils
from app.utils.logger import logger

router = APIRouter()

# Initialize Engines (Singletons)
vol_engine = VolatilityEngine()
struct_engine = StructureEngine()
edge_engine = EdgeEngine()
regime_engine = RegimeEngine()
participant_client = ParticipantClient()

# NEW: Track hybrid logic status
HYBRID_LOGIC_STATUS = {
    "enabled": True,
    "mode": "STRADDLE_RANGE_DELTA_WINGS",  # New hybrid logic
    "last_update": datetime.now().isoformat(),
    "performance": {
        "trades_completed": 0,
        "avg_straddle_cost": 0.0,
        "avg_wing_delta": 0.10
    }
}

@router.get("/analysis", response_model=Dict[str, Any])
async def get_market_analysis(
    market: MarketDataClient = Depends(get_market_client)
):
    """
    VolGuard 5.0 Master Dashboard with Hybrid Logic Status.
    
    Returns the complete 360-degree view including:
    1. Market Analysis (Volatility, Structure, Edge)
    2. Hybrid Logic Status (Straddle Range + Delta Wings)
    3. Real-time Position Matrix
    4. System Health Metrics
    """
    try:
        logger.info("Starting VolGuard 5.0 Market Analysis...")
        start_time = datetime.now()

        # ==================================================================
        # 1. FETCH AUTHORITATIVE EXPIRIES (FROM REGISTRY)
        # ==================================================================
        weekly_exp, monthly_exp = registry.get_nifty_expiries()
        
        if not weekly_exp or not monthly_exp:
            today = date.today()
            logger.warning("Registry not ready, using fallback dates")
            weekly_exp = today
            monthly_exp = today

        # Get Contract Specs (Lot Size)
        specs = registry.get_nifty_contract_specs(weekly_exp)
        lot_size = specs.get("lot_size", 50)

        # Calculate DTEs
        today_date = date.today()
        dte_w = (weekly_exp - today_date).days
        dte_m = (monthly_exp - today_date).days

        # ==================================================================
        # 2. PARALLEL DATA FETCHING (ASYNC)
        # ==================================================================
        
        # A. Market Data Tasks
        hist_task = market.get_daily_candles(NIFTY_KEY, days=400)
        vix_task = market.get_daily_candles(VIX_KEY, days=400)
        live_task = market.get_live_quote([NIFTY_KEY, VIX_KEY])
        
        # B. Option Chain Tasks
        chain_w_task = market.get_option_chain(weekly_exp.strftime("%Y-%m-%d"))
        chain_m_task = market.get_option_chain(monthly_exp.strftime("%Y-%m-%d"))
        
        # C. Participant Data Task
        fii_task = participant_client.fetch_metrics()
        
        # D. NEW: Real-time positions and margin (parallel)
        positions_task = get_live_positions_matrix()
        margin_task = get_margin_status()

        # Execute parallel wait
        results = await asyncio.gather(
            hist_task, vix_task, live_task, chain_w_task, chain_m_task, 
            fii_task, positions_task, margin_task,
            return_exceptions=True
        )
        
        # Unpack results with error handling
        nifty_hist, vix_hist, live_data, chain_w, chain_m, ext_metrics, positions_data, margin_data = results[:8]
        
        # Check for errors in critical data
        if isinstance(positions_data, Exception):
            logger.warning(f"Positions fetch failed: {positions_data}")
            positions_data = {"status": "ERROR", "positions": []}
        if isinstance(margin_data, Exception):
            logger.warning(f"Margin fetch failed: {margin_data}")
            margin_data = {"status": "ERROR", "available_margin": 0.0}

        # ==================================================================
        # 3. ANALYTICAL PIPELINE
        # ==================================================================
        
        # A. Volatility (Global)
        spot = live_data.get(NIFTY_KEY, 0) if not isinstance(live_data, Exception) else 0
        vix = live_data.get(VIX_KEY, 0) if not isinstance(live_data, Exception) else 0
        
        vol_metrics = await vol_engine.analyze(
            nifty_hist if not isinstance(nifty_hist, Exception) else pd.DataFrame(),
            vix_hist if not isinstance(vix_hist, Exception) else pd.DataFrame(),
            spot, vix
        )

        # B. Structure (Weekly vs Monthly)
        chain_w_df = chain_w if not isinstance(chain_w, Exception) else pd.DataFrame()
        chain_m_df = chain_m if not isinstance(chain_m, Exception) else pd.DataFrame()
        
        struct_w = struct_engine.calculate_structure(chain_w_df, spot, lot_size)
        struct_m = struct_engine.calculate_structure(chain_m_df, spot, lot_size)

        # C. Edges (Global)
        edge_metrics = edge_engine.calculate_edge(vol_metrics, chain_w_df, chain_m_df)

        # D. Regime Scoring
        weekly_mandate = regime_engine.analyze_regime(
            vol_metrics, struct_w, edge_metrics, ext_metrics, "WEEKLY", dte_w
        ) if not isinstance(ext_metrics, Exception) else None
        
        monthly_mandate = regime_engine.analyze_regime(
            vol_metrics, struct_m, edge_metrics, ext_metrics, "MONTHLY", dte_m
        ) if not isinstance(ext_metrics, Exception) else None

        # ==================================================================
        # 4. CONSTRUCT THE "GOD VIEW" RESPONSE WITH HYBRID STATUS
        # ==================================================================
        elapsed = (datetime.now() - start_time).total_seconds()
        
        response = {
            "meta": {
                "version": "VolGuard 5.0",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "analysis_time_sec": round(elapsed, 2),
                "data_source": "Upstox V3 + NSE Live",
                "status": "ONLINE",
                "hybrid_logic": HYBRID_LOGIC_STATUS
            },
            
            # --- 1. TIME CONTEXT ---
            "time_context": {
                "current_date": str(today_date),
                "weekly_expiry": str(weekly_exp),
                "monthly_expiry": str(monthly_exp),
                "dte_weekly": dte_w,
                "dte_monthly": dte_m,
                "is_gamma_danger": dte_w <= 1,
                "market_hours": check_market_hours()
            },

            # --- 2. VOLATILITY ANALYSIS ---
            "volatility": {
                "spot": vol_metrics.spot,
                "vix": vol_metrics.vix,
                "trend_strength": round(vol_metrics.trend_strength, 2),
                "metrics": {
                    "vov_zscore": round(vol_metrics.vov_zscore, 2),
                    "ivp_30d": round(vol_metrics.ivp_30d, 1),
                    "ivp_1yr": round(vol_metrics.ivp_1yr, 1),
                    "rv_7d": round(vol_metrics.rv7, 1),
                    "garch_7d": round(vol_metrics.garch7, 1),
                    "parkinson_7d": round(vol_metrics.park7, 1)
                },
                "regime": vol_metrics.vol_regime,
                "kill_switch_active": vol_metrics.vov_zscore > 2.5
            },

            # --- 3. PARTICIPANT DATA ---
            "external_flow": {
                "flow_regime": ext_metrics.flow_regime if not isinstance(ext_metrics, Exception) else "ERROR",
                "event_risk": ext_metrics.event_risk if not isinstance(ext_metrics, Exception) else "ERROR",
                "fii_net_change": ext_metrics.fii_net_change if not isinstance(ext_metrics, Exception) else 0,
                "positions": _format_participant(ext_metrics) if not isinstance(ext_metrics, Exception) else {}
            },

            # --- 4. MARKET STRUCTURE ---
            "market_structure": {
                "weekly": {
                    "net_gex_cr": round(struct_w.net_gex / 10000000, 2) if struct_w else 0,
                    "gex_regime": struct_w.gex_regime if struct_w else "ERROR",
                    "pcr": round(struct_w.pcr, 2) if struct_w else 0,
                    "max_pain": struct_w.max_pain if struct_w else 0,
                    "skew": round(struct_w.skew_25d, 2) if struct_w else 0
                },
                "monthly": {
                    "net_gex_cr": round(struct_m.net_gex / 10000000, 2) if struct_m else 0,
                    "gex_regime": struct_m.gex_regime if struct_m else "ERROR",
                    "pcr": round(struct_m.pcr, 2) if struct_m else 0,
                    "max_pain": struct_m.max_pain if struct_m else 0
                }
            },

            # --- 5. OPTION EDGES ---
            "edges": {
                "term_structure": edge_metrics.term_regime,
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

            # --- 6. TRADING MANDATES ---
            "mandates": {
                "WEEKLY": {
                    "regime": weekly_mandate.regime_name if weekly_mandate else "ERROR",
                    "strategy": weekly_mandate.strategy_type if weekly_mandate else "ERROR",
                    "allocation": f"{weekly_mandate.allocation_pct}%" if weekly_mandate else "0%",
                    "max_lots": weekly_mandate.max_lots if weekly_mandate else 0,
                    "rationale": weekly_mandate.rationale if weekly_mandate else "Data unavailable",
                    "warnings": weekly_mandate.warnings if weekly_mandate else []
                } if weekly_mandate else {"status": "DATA_UNAVAILABLE"},
                "MONTHLY": {
                    "regime": monthly_mandate.regime_name if monthly_mandate else "ERROR",
                    "strategy": monthly_mandate.strategy_type if monthly_mandate else "ERROR",
                    "allocation": f"{monthly_mandate.allocation_pct}%" if monthly_mandate else "0%",
                    "max_lots": monthly_mandate.max_lots if monthly_mandate else 0,
                    "rationale": monthly_mandate.rationale if monthly_mandate else "Data unavailable"
                } if monthly_mandate else {"status": "DATA_UNAVAILABLE"}
            },
            
            # --- 7. NEW: REAL-TIME POSITIONS MATRIX ---
            "positions": positions_data,
            
            # --- 8. NEW: MARGIN STATUS ---
            "margin": margin_data,
            
            # --- 9. NEW: HYBRID LOGIC CALCULATIONS ---
            "hybrid_calculations": await calculate_hybrid_metrics(spot, chain_w_df, chain_m_df),
            
            # --- 10. SYSTEM HEALTH ---
            "system_health": {
                "data_quality": check_data_quality(
                    nifty_hist, vix_hist, chain_w_df, chain_m_df, ext_metrics
                ),
                "api_status": {
                    "upstox": not isinstance(live_data, Exception),
                    "nse_participants": not isinstance(ext_metrics, Exception),
                    "option_chains": not isinstance(chain_w, Exception) and not isinstance(chain_m, Exception)
                },
                "last_update": datetime.now().isoformat()
            },
            
            # --- 11. COMPARATIVE SUMMARY ---
            "recommendation": _generate_recommendation(weekly_mandate, monthly_mandate) 
            if weekly_mandate and monthly_mandate else "Data incomplete for recommendation"
        }

        # Update hybrid logic status
        update_hybrid_logic_status(response)

        return response

    except Exception as e:
        logger.error(f"Dashboard Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================================================================
# NEW: REAL-TIME POSITIONS MATRIX ENDPOINT
# ==================================================================

@router.get("/live-matrix")
async def get_live_matrix(token_mgr: TokenManager = Depends()):
    """
    Real-time positions matrix with Greeks and P&L
    
    Returns:
        Live view of all positions with delta, gamma, theta, vega, and P&L
    """
    try:
        headers = token_mgr.get_headers()
        
        # Fetch current positions
        positions_url = "https://api.upstox.com/v2/portfolio/short-term-positions"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Get positions
            pos_response = await client.get(positions_url, headers=headers)
            
            if pos_response.status_code != 200:
                logger.error(f"Positions API error: {pos_response.status_code}")
                return {"status": "ERROR", "message": "Failed to fetch positions"}
            
            pos_data = pos_response.json().get('data', [])
            
            # Filter for active options positions
            active_positions = [
                p for p in pos_data 
                if p.get('quantity', 0) != 0 and p.get('product', '') == 'D'
            ]
            
            if not active_positions:
                return {"status": "FLAT", "message": "No active positions", "positions": []}
            
            # Get instrument keys for Greeks
            instrument_keys = [p['instrument_token'] for p in active_positions]
            keys_str = ",".join(instrument_keys)
            
            # Fetch Greeks in parallel
            greeks_url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
            quotes_url = "https://api-v2.upstox.com/v2/market-quote/quotes"
            
            greeks_task = client.get(greeks_url, params={'instrument_key': keys_str}, headers=headers)
            quotes_task = client.get(quotes_url, params={'instrument_key': keys_str}, headers=headers)
            
            greeks_resp, quotes_resp = await asyncio.gather(greeks_task, quotes_task)
            
            greeks_data = greeks_resp.json().get('data', {})
            quotes_data = quotes_resp.json().get('data', {})
            
            # Build positions matrix
            matrix = []
            total_pnl = 0.0
            total_delta = 0.0
            total_vega = 0.0
            total_theta = 0.0
            
            for position in active_positions:
                key = position['instrument_token']
                greek_info = greeks_data.get(key, {})
                quote_info = quotes_data.get(key, {})
                
                # Calculate position Greeks
                quantity = position['quantity']
                side_multiplier = 1 if position.get('side', 'BUY') == 'BUY' else -1
                
                position_delta = greek_info.get('delta', 0) * quantity * side_multiplier
                position_gamma = greek_info.get('gamma', 0) * quantity * side_multiplier
                position_theta = greek_info.get('theta', 0) * quantity * side_multiplier
                position_vega = greek_info.get('vega', 0) * quantity * side_multiplier
                
                # Current P&L
                current_price = quote_info.get('last_price', 0)
                average_price = position.get('average_price', 0)
                unrealized_pnl = (current_price - average_price) * quantity * side_multiplier
                
                position_data = {
                    "symbol": position.get('trading_symbol', 'UNKNOWN'),
                    "instrument_key": key,
                    "quantity": quantity,
                    "side": position.get('side', 'UNKNOWN'),
                    "average_price": round(average_price, 2),
                    "current_price": round(current_price, 2),
                    "greeks": {
                        "delta": round(position_delta, 4),
                        "gamma": round(position_gamma, 6),
                        "theta": round(position_theta, 4),
                        "vega": round(position_vega, 4)
                    },
                    "pnl": {
                        "unrealized": round(unrealized_pnl, 2),
                        "m2m": position.get('m2m', 0)
                    },
                    "risk_metrics": {
                        "iv": round(greek_info.get('iv', 0) * 100, 2),  # Convert to percentage
                        "moneyness": "ITM" if greek_info.get('delta', 0) > 0.7 else 
                                     "ATM" if 0.3 <= greek_info.get('delta', 0) <= 0.7 else 
                                     "OTM"
                    }
                }
                
                matrix.append(position_data)
                
                # Update totals
                total_pnl += unrealized_pnl
                total_delta += position_delta
                total_vega += position_vega
                total_theta += position_theta
            
            # Calculate portfolio metrics
            portfolio_metrics = {
                "total_positions": len(active_positions),
                "total_pnl": round(total_pnl, 2),
                "total_delta": round(total_delta, 4),
                "total_vega": round(total_vega, 4),
                "total_theta": round(total_theta, 4),
                "net_exposure": "LONG" if total_delta > 0.1 else "SHORT" if total_delta < -0.1 else "NEUTRAL",
                "vulnerability": "VEGA" if abs(total_vega) > 100 else 
                               "THETA" if abs(total_theta) > 50 else 
                               "DELTA" if abs(total_delta) > 0.5 else "BALANCED"
            }
            
            return {
                "status": "ACTIVE",
                "timestamp": datetime.now().isoformat(),
                "portfolio_metrics": portfolio_metrics,
                "positions": matrix
            }
            
    except httpx.TimeoutException:
        logger.error("Positions matrix timeout")
        return {"status": "TIMEOUT", "message": "Request timeout"}
    except Exception as e:
        logger.error(f"Live matrix error: {e}")
        return {"status": "ERROR", "message": str(e)}

# ==================================================================
# NEW: HELPER FUNCTIONS FOR HYBRID LOGIC
# ==================================================================

async def get_live_positions_matrix():
    """
    Helper function to get live positions matrix
    Uses the new hybrid logic for position analysis
    """
    try:
        # In a real implementation, this would fetch from the Supervisor
        # For now, return a placeholder structure
        return {
            "status": "SIMULATED",
            "positions": [],
            "message": "Position matrix available via /live-matrix endpoint"
        }
    except Exception as e:
        logger.error(f"Positions matrix helper error: {e}")
        return {"status": "ERROR", "error": str(e)}

async def get_margin_status():
    """
    Get current margin status
    """
    try:
        # This would integrate with CapitalGovernor in real implementation
        return {
            "status": "SIMULATED",
            "available_margin": 1000000.0,
            "utilized_margin": 0.0,
            "utilization_pct": 0.0,
            "message": "Margin data available via CapitalGovernor"
        }
    except Exception as e:
        logger.error(f"Margin status error: {e}")
        return {"status": "ERROR", "error": str(e)}

async def calculate_hybrid_metrics(spot: float, weekly_chain: pd.DataFrame, monthly_chain: pd.DataFrame):
    """
    Calculate hybrid logic metrics (Straddle Range + Delta Wings)
    """
    try:
        if spot <= 0 or weekly_chain.empty:
            return {"status": "INSUFFICIENT_DATA"}
        
        # Calculate ATM strike
        atm_strike = round(spot / 50) * 50
        
        # Find ATM in weekly chain
        atm_row = weekly_chain[weekly_chain['strike'] == atm_strike]
        
        if atm_row.empty:
            return {"status": "ATM_NOT_FOUND"}
        
        # Extract ATM option prices (simplified)
        atm_ce_price = atm_row['ce_ltp'].iloc[0] if 'ce_ltp' in atm_row else 100.0
        atm_pe_price = atm_row['pe_ltp'].iloc[0] if 'pe_ltp' in atm_row else 100.0
        
        # Calculate straddle cost (expected move)
        straddle_cost = atm_ce_price + atm_pe_price
        
        # Calculate theoretical short strike range
        upper_range = atm_strike + straddle_cost
        lower_range = atm_strike - straddle_cost
        
        # Find nearest strikes to range
        weekly_strikes = weekly_chain['strike'].tolist()
        
        def find_nearest_strike(target: float) -> int:
            return min(weekly_strikes, key=lambda x: abs(x - target))
        
        upper_strike = find_nearest_strike(upper_range)
        lower_strike = find_nearest_strike(lower_range)
        
        return {
            "status": "CALCULATED",
            "spot": round(spot, 2),
            "atm_strike": atm_strike,
            "straddle_cost": round(straddle_cost, 2),
            "expected_move_pct": round((straddle_cost / spot) * 100, 2),
            "short_strike_range": {
                "upper": upper_strike,
                "lower": lower_strike,
                "width_points": upper_strike - lower_strike,
                "width_pct": round(((upper_strike - lower_strike) / spot) * 100, 2)
            },
            "delta_wings": {
                "condor": 0.10,  # 10 Delta for Iron Condor
                "fly": 0.15      # 15 Delta for Iron Fly
            },
            "interpretation": _interpret_hybrid_metrics(straddle_cost, spot)
        }
        
    except Exception as e:
        logger.error(f"Hybrid metrics calculation error: {e}")
        return {"status": "ERROR", "error": str(e)}

def _interpret_hybrid_metrics(straddle_cost: float, spot: float) -> str:
    """Interpret the hybrid metrics for display"""
    move_pct = (straddle_cost / spot) * 100
    
    if move_pct > 3.0:
        return "HIGH Expected Move - Consider wider strikes or reduced size"
    elif move_pct > 2.0:
        return "MODERATE Expected Move - Normal trading conditions"
    else:
        return "LOW Expected Move - Good opportunity for premium selling"

def update_hybrid_logic_status(dashboard_response: Dict):
    """Update hybrid logic status based on dashboard analysis"""
    global HYBRID_LOGIC_STATUS
    
    try:
        # Extract relevant metrics
        volatility = dashboard_response.get('volatility', {})
        structure = dashboard_response.get('market_structure', {})
        
        # Update status
        HYBRID_LOGIC_STATUS.update({
            "last_update": datetime.now().isoformat(),
            "volatility_regime": volatility.get('regime', 'UNKNOWN'),
            "kill_switch_active": volatility.get('kill_switch_active', False),
            "weekly_gex_regime": structure.get('weekly', {}).get('gex_regime', 'UNKNOWN'),
            "monthly_gex_regime": structure.get('monthly', {}).get('gex_regime', 'UNKNOWN')
        })
        
    except Exception as e:
        logger.error(f"Failed to update hybrid logic status: {e}")

def check_market_hours() -> Dict:
    """Check if market is currently open"""
    now = datetime.now()
    current_time = now.time()
    
    # NSE equity market hours (9:15 AM to 3:30 PM)
    market_open = datetime.strptime("09:15", "%H:%M").time()
    market_close = datetime.strptime("15:30", "%H:%M").time()
    
    is_open = market_open <= current_time <= market_close
    minutes_to_close = 0
    
    if is_open:
        close_dt = datetime.combine(now.date(), market_close)
        minutes_to_close = max(0, int((close_dt - now).total_seconds() / 60))
    
    return {
        "is_open": is_open,
        "current_time": current_time.strftime("%H:%M"),
        "open_time": "09:15",
        "close_time": "15:30",
        "minutes_to_close": minutes_to_close,
        "day_of_week": now.strftime("%A"),
        "is_weekend": now.weekday() >= 5
    }

def check_data_quality(*data_sources) -> Dict:
    """Check quality of all data sources"""
    quality_report = {
        "overall": "GOOD",
        "details": {},
        "issues": []
    }
    
    source_names = ["NIFTY_HIST", "VIX_HIST", "WEEKLY_CHAIN", "MONTHLY_CHAIN", "PARTICIPANT_DATA"]
    
    for name, data in zip(source_names, data_sources):
        if isinstance(data, Exception):
            quality_report["details"][name] = "ERROR"
            quality_report["issues"].append(f"{name}: {str(data)}")
            quality_report["overall"] = "DEGRADED"
        elif hasattr(data, 'empty') and data.empty:
            quality_report["details"][name] = "EMPTY"
            quality_report["issues"].append(f"{name}: Empty dataset")
            quality_report["overall"] = "DEGRADED"
        elif data is None:
            quality_report["details"][name] = "MISSING"
            quality_report["issues"].append(f"{name}: Missing data")
            quality_report["overall"] = "DEGRADED"
        else:
            quality_report["details"][name] = "GOOD"
    
    return quality_report

def _format_participant(ext_metrics):
    """Format participant data"""
    if not ext_metrics or isinstance(ext_metrics, Exception):
        return {}
    
    return {
        "FII": {
            "fut_net": ext_metrics.fii.fut_net if hasattr(ext_metrics.fii, 'fut_net') else 0,
            "call_net": ext_metrics.fii.call_net if hasattr(ext_metrics.fii, 'call_net') else 0,
            "put_net": ext_metrics.fii.put_net if hasattr(ext_metrics.fii, 'put_net') else 0,
            "bias": "BULLISH" if (ext_metrics.fii.fut_net if hasattr(ext_metrics.fii, 'fut_net') else 0) > 0 else "BEARISH"
        } if hasattr(ext_metrics, 'fii') else {},
        "DII": {
            "fut_net": ext_metrics.dii.fut_net if hasattr(ext_metrics.dii, 'fut_net') else 0,
            "call_net": ext_metrics.dii.call_net if hasattr(ext_metrics.dii, 'call_net') else 0,
            "put_net": ext_metrics.dii.put_net if hasattr(ext_metrics.dii, 'put_net') else 0,
            "bias": "BULLISH" if (ext_metrics.dii.fut_net if hasattr(ext_metrics.dii, 'fut_net') else 0) > 0 else "BEARISH"
        } if hasattr(ext_metrics, 'dii') else {},
        "PRO": {
            "fut_net": ext_metrics.pro.fut_net if hasattr(ext_metrics.pro, 'fut_net') else 0,
            "call_net": ext_metrics.pro.call_net if hasattr(ext_metrics.pro, 'call_net') else 0,
            "put_net": ext_metrics.pro.put_net if hasattr(ext_metrics.pro, 'put_net') else 0,
            "bias": "BULLISH" if (ext_metrics.pro.fut_net if hasattr(ext_metrics.pro, 'fut_net') else 0) > 0 else "BEARISH"
        } if hasattr(ext_metrics, 'pro') else {},
        "CLIENT": {
            "fut_net": ext_metrics.client.fut_net if hasattr(ext_metrics.client, 'fut_net') else 0,
            "call_net": ext_metrics.client.call_net if hasattr(ext_metrics.client, 'call_net') else 0,
            "put_net": ext_metrics.client.put_net if hasattr(ext_metrics.client, 'put_net') else 0,
            "bias": "BULLISH" if (ext_metrics.client.fut_net if hasattr(ext_metrics.client, 'fut_net') else 0) > 0 else "BEARISH"
        } if hasattr(ext_metrics, 'client') else {}
    }

def _generate_recommendation(weekly, monthly):
    """Generate trading recommendation"""
    if not weekly or not monthly:
        return "Data incomplete for recommendation"
    
    # If both are CASH, stay out
    if weekly.regime_name == "CASH" and monthly.regime_name == "CASH":
        return "STAY CASH: No favorable regime found."
    
    # Pick the higher allocation
    if weekly.allocation_pct >= monthly.allocation_pct:
        winner = "WEEKLY"
        score = weekly.allocation_pct
        regime = weekly.regime_name
    else:
        winner = "MONTHLY" 
        score = monthly.allocation_pct
        regime = monthly.regime_name
        
    # Add hybrid logic context
    hybrid_context = " (Using Straddle Range + Delta Wings)" if HYBRID_LOGIC_STATUS["enabled"] else ""
    
    return f"FOCUS ON {winner}{hybrid_context} ({score}% Allocation). Regime: {regime}."
