import numpy as np
from typing import Dict, List
import logging
from py_vollib_vectorized import vectorized_implied_volatility, vectorized_greeks
from app.core.risk.stress_tester import StressTester

logger = logging.getLogger(__name__)

class RiskEngine:
    def __init__(self, config: Dict):
        self.max_gamma = config.get("MAX_GAMMA", 0.15)
        self.max_vega = config.get("MAX_VEGA", 1000)
        self.stress_tester = StressTester()

    async def run_stress_tests(self, portfolio_greeks: Dict, market_snapshot: Dict, positions: Dict) -> Dict:
        spot = market_snapshot.get("spot", 0)
        vix = market_snapshot.get("vix", 0)
        if spot == 0: return {}
        return self.stress_tester.simulate_scenarios(positions, spot, vix)

    def check_breaches(self, metrics: Dict) -> List[Dict]:
        breaches = []
        if abs(metrics.get("gamma", 0)) > self.max_gamma:
            breaches.append({"limit": "GAMMA", "val": metrics["gamma"], "action": "REDUCE_EXPOSURE_IMMEDIATELY"})
        if abs(metrics.get("vega", 0)) > self.max_vega:
            breaches.append({"limit": "VEGA", "val": metrics["vega"], "action": "WARN_AND_REDUCE"})
        return breaches

    @staticmethod
    def calculate_leg_greeks(price, spot, strike, time_years, rate, option_type) -> Dict:
        """Black-Scholes Calculator"""
        try:
            flag = 'c' if option_type.lower() in ['ce', 'call', 'c'] else 'p'
            iv = vectorized_implied_volatility(price, spot, strike, time_years, rate, flag)
            greeks = vectorized_greeks(spot, strike, time_years, rate, iv, flag, return_as='dict')
            greeks['iv'] = iv
            return greeks
        except:
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0}
