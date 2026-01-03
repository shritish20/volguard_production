import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
import asyncio
from py_vollib_vectorized import vectorized_implied_volatility, get_all_greeks 
from app.core.risk.stress_tester import StressTester

class RiskEngine:
    def __init__(self, config: Dict):
        self.max_gamma = config.get("MAX_GAMMA", 0.15)
        self.max_vega = config.get("MAX_VEGA", 1000.0)
        self.stress_tester = StressTester()

    def check_breaches(self, metrics: Dict) -> List[Dict]:
        breaches = []
        if abs(metrics.get("gamma", 0)) > self.max_gamma:
            breaches.append({
                "limit": "GAMMA", "val": metrics["gamma"], 
                "action": "REDUCE_EXPOSURE_IMMEDIATELY"
            })
        if metrics.get("vega", 0) > self.max_vega:
            breaches.append({
                "limit": "VEGA", "val": metrics["vega"], 
                "action": "WARN_AND_REDUCE"
            })
        return breaches

    def calculate_leg_greeks(self, price, spot, strike, time_years, rate, option_type) -> Dict:
        try:
            flag = 'c' if option_type.lower() in ['ce', 'call'] else 'p'
            
            implied_iv = vectorized_implied_volatility(
                price, spot, strike, time_years, rate, flag, return_as='numpy'
            )
            
            if np.isnan(implied_iv) or implied_iv == 0:
                implied_iv = 0.20 
                
            greeks = get_all_greeks(flag, spot, strike, time_years, rate, implied_iv, return_as='dict')
            
            return {
                "delta": float(greeks.get('delta', 0)),
                "gamma": float(greeks.get('gamma', 0)),
                "theta": float(greeks.get('theta', 0)),
                "vega": float(greeks.get('vega', 0)),
                "iv": float(implied_iv)
            }
        except Exception:
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0}

    async def run_stress_tests(self, portfolio_greeks: Dict, market_snapshot: Dict, positions: Dict) -> Dict:
        spot = market_snapshot.get("spot", 0)
        vix = market_snapshot.get("vix", 0)
        if spot == 0: return {}
        return await asyncio.to_thread(self.stress_tester.simulate_scenarios, positions, spot, vix)
