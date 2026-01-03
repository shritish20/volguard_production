# app/core/risk/risk_engine.py

import numpy as np
from typing import Dict, List
import asyncio
from py_vollib_vectorized import vectorized_implied_volatility, get_all_greeks
from app.core.risk.stress_tester import StressTester
import logging

logger = logging.getLogger(__name__)

class RiskEngine:
    def __init__(self, config: Dict):
        self.max_gamma = config.get("MAX_GAMMA", 0.15)
        self.max_vega = config.get("MAX_VEGA", 1000.0)
        self.stress_tester = StressTester()

    # --------------------------------------------------
    # GREEKS
    # --------------------------------------------------
    def calculate_leg_greeks(self, price, spot, strike, time_years, rate, option_type) -> Dict:
        try:
            flag = 'c' if option_type.lower() in ['ce', 'call'] else 'p'

            implied_iv = vectorized_implied_volatility(
                price, spot, strike, time_years, rate, flag, return_as='numpy'
            )

            # Conservative fallback (protective bias)
            if not np.isfinite(implied_iv) or implied_iv <= 0:
                implied_iv = 0.40  # Stress-biased IV

            greeks = get_all_greeks(
                flag, spot, strike, time_years, rate, implied_iv, return_as='dict'
            )

            return {
                "delta": float(greeks.get("delta", 0)),
                "gamma": float(greeks.get("gamma", 0)),
                "theta": float(greeks.get("theta", 0)),
                "vega": float(greeks.get("vega", 0)),
                "iv": float(implied_iv)
            }

        except Exception as e:
            logger.error(f"Greek calc failed: {e}")
            # Fail-closed: non-zero gamma/vega is safer than zero
            return {"delta": 0, "gamma": 0.01, "theta": 0, "vega": 10, "iv": 0.40}

    # --------------------------------------------------
    # PORTFOLIO AGGREGATION
    # --------------------------------------------------
    def aggregate_portfolio_greeks(self, positions: Dict) -> Dict:
        agg = {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0}

        for p in positions.values():
            g = p.get("greeks", {})
            qty = p.get("quantity", 0)
            side = 1 if p.get("side") == "BUY" else -1

            agg["delta"] += g.get("delta", 0) * qty * side
            agg["gamma"] += g.get("gamma", 0) * qty
            agg["vega"] += g.get("vega", 0) * qty
            agg["theta"] += g.get("theta", 0) * qty

        return agg

    # --------------------------------------------------
    # LIMIT CHECKS
    # --------------------------------------------------
    def check_breaches(self, metrics: Dict) -> List[Dict]:
        breaches = []

        if abs(metrics.get("gamma", 0)) > self.max_gamma:
            breaches.append({
                "limit": "GAMMA",
                "value": metrics["gamma"],
                "action": "REDUCE_EXPOSURE_IMMEDIATELY"
            })

        if metrics.get("vega", 0) > self.max_vega:
            breaches.append({
                "limit": "VEGA",
                "value": metrics["vega"],
                "action": "WARN_AND_REDUCE"
            })

        return breaches

    # --------------------------------------------------
    # STRESS TESTS
    # --------------------------------------------------
    async def run_stress_tests(self, portfolio_greeks: Dict, market_snapshot: Dict, positions: Dict) -> Dict:
        spot = market_snapshot.get("spot", 0)
        vix = market_snapshot.get("vix", 0)

        if spot <= 0:
            return {}

        try:
            return await asyncio.to_thread(
                self.stress_tester.simulate_scenarios,
                positions, spot, vix
            )
        except Exception as e:
            logger.critical(f"Stress test failed: {e}")
            # Fail-closed: assume worst
            return {"WORST_CASE": {"impact": -0.05 * spot}}
