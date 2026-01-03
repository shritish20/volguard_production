import numpy as np
from typing import Dict, List
from datetime import datetime
# FIX: Correct import to match Engine
from py_vollib_vectorized import get_all_greeks

class StressTester:
    def __init__(self):
        # Format: (Spot Shock %, Vol Shock %)
        self.scenarios = [
            (-0.10, 0.25),  # Crash
            (-0.05, 0.15),  # Correction
            (-0.02, 0.05),  # Dip
            (0.00, 0.00),   # Flat
            (0.02, -0.05),  # Rally
            (0.05, -0.10),  # Bull Run
        ]
        self.r_rate = 0.06 # Risk Free Rate

    def simulate_scenarios(self, positions: Dict[str, Dict], spot: float, vix: float) -> Dict:
        if not positions or spot == 0: 
            return {"WORST_CASE": {"impact": 0.0}, "matrix": []}
            
        results = []
        for s_shock, v_shock in self.scenarios:
            sim_spot = spot * (1 + s_shock)
            sim_pnl = 0.0
            
            for _, pos in positions.items():
                qty = abs(pos.get('quantity', 0)) * (1 if pos.get('side') == 'BUY' else -1)
                if qty == 0: continue
                
                strike = pos.get('strike')
                
                # Handle Futures
                if not strike or strike == 0: 
                    sim_pnl += (sim_spot - pos.get('average_price', spot)) * qty * pos.get('lot_size', 50)
                    continue
                
                # Handle Options
                exp = pos.get('expiry')
                if isinstance(exp, str): 
                    try:
                        exp = datetime.strptime(exp, "%Y-%m-%d") if len(exp) == 10 else datetime.now()
                    except:
                        exp = datetime.now()
                        
                t = max((exp - datetime.now()).days / 365.0, 0.001) if exp else 0.05
                
                # Shock the Volatility
                base_iv = pos.get('greeks', {}).get('iv') or (vix/100)
                sim_iv = base_iv * (1 + v_shock)
                
                flag = 'c' if pos.get('option_type', 'CE').lower() in ['ce','call'] else 'p'
                
                try:
                    # Calculate new theoretical price
                    greeks = get_all_greeks(flag, sim_spot, strike, t, self.r_rate, sim_iv, return_as='dict')
                    price = greeks.get('call_price' if flag=='c' else 'put_price', 0)
                    
                    # PnL = (New Price - Entry Price) * Quantity * Lot Size
                    sim_pnl += (price - pos.get('current_price', 0)) * qty * pos.get('lot_size', 50)
                except: 
                    pass
                
            results.append({
                "spot_shock": s_shock, 
                "vol_shock": v_shock, 
                "projected_pnl": round(sim_pnl, 2)
            })
            
        # Sort to find the biggest loss
        worst = sorted(results, key=lambda x: x['projected_pnl'])[0]
        
        return {
            "WORST_CASE": {
                "impact": worst['projected_pnl'], 
                "scenario": f"{worst['spot_shock']}/{worst['vol_shock']}"
            }, 
            "matrix": results
        }
