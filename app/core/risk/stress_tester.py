# app/core/risk/stress_tester.py

import numpy as np
from typing import Dict
from datetime import datetime
from py_vollib_vectorized import get_all_greeks
import logging

logger = logging.getLogger(__name__)

class StressTester:
    def __init__(self):
        # (spot shock, vol shock)
        self.scenarios = [
            (-0.10, 0.25),
            (-0.05, 0.15),
            (-0.02, 0.05),
            (0.00, 0.00),
            (0.02, -0.05),
            (0.05, -0.10),
        ]
        self.r_rate = 0.06
        self.min_iv = 0.15  # Conservative floor

    def simulate_scenarios(self, positions: Dict[str, Dict], spot: float, vix: float) -> Dict:
        if not positions or spot <= 0:
            return {"WORST_CASE": {"impact": 0.0}, "matrix": []}

        results = []

        for s_shock, v_shock in self.scenarios:
            sim_spot = spot * (1 + s_shock)
            sim_pnl = 0.0

            for pos in positions.values():
                qty = abs(pos.get("quantity", 0))
                if qty == 0:
                    continue

                side = 1 if pos.get("side") == "BUY" else -1
                lot = pos.get("lot_size", 50)
                qty_signed = qty * side

                strike = pos.get("strike", 0)

                # ---------------------------
                # FUTURES LOGIC
                # ---------------------------
                if not strike:
                    entry = pos.get("average_price", spot)
                    sim_pnl += (sim_spot - entry) * qty_signed * lot
                    continue

                # ---------------------------
                # OPTIONS LOGIC
                # ---------------------------
                expiry = pos.get("expiry")
                if isinstance(expiry, str):
                    try:
                        expiry = datetime.strptime(expiry, "%Y-%m-%d")
                    except Exception:
                        expiry = datetime.now()

                t_seconds = max((expiry - datetime.now()).total_seconds(), 3600)
                t = t_seconds / (365 * 24 * 3600)

                base_iv = pos.get("greeks", {}).get("iv") or (vix / 100)
                sim_iv = max(base_iv * (1 + v_shock), self.min_iv)

                flag = "c" if pos.get("option_type", "CE").lower() in ["ce", "call"] else "p"

                try:
                    greeks = get_all_greeks(
                        flag, sim_spot, strike, t, self.r_rate, sim_iv, return_as="dict"
                    )
                    price = greeks.get("call_price" if flag == "c" else "put_price", 0.0)
                    sim_pnl += (price - pos.get("current_price", 0.0)) * qty_signed * lot
                except Exception as e:
                    logger.error(f"Stress calc failed for {pos.get('instrument_key')}: {e}")
                    # Fail conservative
                    sim_pnl -= abs(sim_spot * 0.01 * qty_signed)

            results.append({
                "spot_shock": s_shock,
                "vol_shock": v_shock,
                "projected_pnl": round(sim_pnl, 2)
            })

        worst = min(results, key=lambda x: x["projected_pnl"])
        return {
            "WORST_CASE": {
                "impact": worst["projected_pnl"],
                "scenario": f"{worst['spot_shock']}/{worst['vol_shock']}"
            },
            "matrix": results
        }
