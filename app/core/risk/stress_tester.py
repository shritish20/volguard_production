import numpy as np
import pandas as pd
from typing import Dict, List
from datetime import datetime
from py_vollib_vectorized import vectorized_implied_volatility, vectorized_greeks, get_all_greeks

class StressTester:
    """
    Production Grade Stress Tester.
    Simulates portfolio performance under various Spot (%) and Volatility (%) shocks.
    """

    def __init__(self):
        # Standard stress scenarios for institutional compliance
        # Format: (Spot Change %, Volatility Increase %)
        self.scenarios = [
            (-0.10, 0.25),  # CRASH: Market drops 10%, Volatility spikes 25% (WORST CASE)
            (-0.05, 0.15),  # CORRECTION: Market drops 5%, Volatility spikes 15%
            (-0.02, 0.05),  # DIP: Market drops 2%, Volatility up 5%
            (0.00, 0.00),   # FLAT: No change
            (0.02, -0.05),  # RALLY: Market up 2%, Volatility drops 5%
            (0.05, -0.10),  # BULL RUN: Market up 5%, Volatility drops 10%
        ]
        self.risk_free_rate = 0.06  # 6% India Risk Free Rate

    def simulate_scenarios(self, positions: Dict[str, Dict], current_spot: float, current_vix: float) -> Dict:
        """
        Runs the simulation matrix on the current portfolio.
        """
        if not positions or current_spot == 0:
            return {
                "WORST_CASE": {"impact": 0.0, "scenario": "NO_POSITIONS"},
                "matrix": []
            }

        # 1. Prepare Data Vectors for Vectorized Calculation
        scenario_results = []
        
        # Process each scenario
        for spot_shock, vol_shock in self.scenarios:
            sim_spot = current_spot * (1 + spot_shock)
            
            # Aggregate PnL for this specific scenario
            scenario_pnl = 0.0
            
            for pos_id, pos in positions.items():
                qty = pos.get('quantity', 0)
                if qty == 0: continue

                # Instrument details
                strike = pos.get('strike')
                expiry_date = pos.get('expiry') # datetime object
                otype = pos.get('option_type', 'CE').lower()
                # 1 for BUY, -1 for SELL
                # Note: 'quantity' in Upstox is usually positive, 'side' determines sign.
                # However, commonly for PnL logic: Long = +Qty, Short = -Qty.
                # Here we trust the input 'quantity' sign from Executor or adjust via side.
                raw_qty = abs(qty)
                pos_sign = 1 if pos.get('side') == 'BUY' else -1
                net_qty = raw_qty * pos_sign
                
                lot_size = pos.get('lot_size', 50) # Fallback to 50 if missing
                
                # Handle Futures (Linear Instrument)
                # If strike is 0 or key indicates Future
                if pos.get('instrument_key', '').endswith('FUT') or not strike or strike == 0:
                    entry_price = pos.get('average_price', current_spot)
                    # Linear PnL: (New Price - Old Price) * Qty
                    pnl = (sim_spot - entry_price) * net_qty * lot_size
                    scenario_pnl += pnl
                    continue

                # Handle Options (Non-Linear)
                # Calculate Time to Expiry (T)
                if expiry_date:
                    if isinstance(expiry_date, str):
                        try:
                            expiry_date = datetime.strptime(expiry_date, "%Y-%m-%d")
                        except:
                            expiry_date = datetime.now()
                            
                    t_days = (expiry_date - datetime.now()).days
                    t_years = max(t_days / 365.0, 0.001) # Avoid div by zero
                else:
                    t_years = 0.05 # Default fallback

                # Estimate current IV
                # In simulation, we assume the Option's IV expands by the vol_shock
                base_iv = pos.get('greeks', {}).get('iv', current_vix / 100.0)
                if not base_iv or base_iv == 0: base_iv = current_vix / 100.0
                
                sim_iv = base_iv * (1 + vol_shock)

                # Calculate Theoretical Price using Black-Scholes
                flag = 'c' if otype in ['ce', 'call'] else 'p'
                
                try:
                    # Using py_vollib_vectorized for robustness
                    greeks = get_all_greeks(flag, sim_spot, strike, t_years, self.risk_free_rate, sim_iv, return_as='dict')
                    theoretical_price = greeks.get('call_price' if flag=='c' else 'put_price', 0)
                    
                    # Mark-to-Market PnL: (New Theo Price - Current Market Price) * NetQty
                    # Note: We compare against 'current_price' (LTP) for immediate risk, 
                    # or 'average_price' for total PnL. For stress testing "risk from now", use LTP.
                    current_price = pos.get('current_price', 0)
                    pnl = (theoretical_price - current_price) * net_qty * lot_size
                    scenario_pnl += pnl
                    
                except Exception as e:
                    # Log but continue to avoid crashing the whole cycle
                    # print(f"Stress Calc Error {pos_id}: {e}")
                    continue

            scenario_results.append({
                "spot_shock": f"{spot_shock*100:+.0f}%",
                "vol_shock": f"{vol_shock*100:+.0f}%",
                "projected_pnl": round(scenario_pnl, 2)
            })

        # 2. Identify Worst Case
        # Sort by PnL ascending (lowest/most negative first)
        if not scenario_results:
             return {"WORST_CASE": {"impact": 0.0}, "matrix": []}

        sorted_scenarios = sorted(scenario_results, key=lambda x: x['projected_pnl'])
        worst_case = sorted_scenarios[0]

        return {
            "WORST_CASE": {
                "impact": worst_case['projected_pnl'],
                "scenario": f"Spot {worst_case['spot_shock']} / Vol {worst_case['vol_shock']}"
            },
            "matrix": scenario_results
        }
