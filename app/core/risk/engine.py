# app/core/risk/engine.py

import logging
import numpy as np
from scipy.stats import norm
from typing import Dict, List, Any

# Removed broken import: from app.schemas.analytics import RiskReport
# The engine uses standard Dictionaries for reporting.

logger = logging.getLogger(__name__)

class RiskEngine:
    """
    VolGuard Smart Risk Engine (VolGuard 3.0)
    
    Responsibilities:
    1. GREEK ENGINE: Calculates missing Greeks for V2 Portfolio positions.
    2. STRESS TESTER: Simulates Portfolio PnL under Market Crash/Moon scenarios.
    3. EXPOSURE CHECK: Aggregates Net Delta, Gamma, Vega to flag dangerous skews.
    """

    def __init__(self, max_portfolio_loss: float = 50000.0):
        self.max_loss_limit = max_portfolio_loss
        self.risk_free_rate = 0.06 # India 10Y Bond Proxy

    async def run_stress_tests(
        self, 
        config: Dict, 
        snapshot: Dict, 
        positions: Dict[str, Dict]
    ) -> Dict:
        """
        Simulates portfolio PnL across a matrix of Spot % and IV % changes.
        Used by Supervisor to trigger STRESS BLOCK.
        """
        spot = snapshot.get("spot", 0)
        vix = snapshot.get("vix", 15.0)
        
        if spot == 0 or not positions:
            return {"WORST_CASE": {"impact": 0.0, "scenario": "N/A"}}

        # Scenarios: Spot [ -5%, -2%, 0%, +2%, +5% ] x IV [ -20%, +20% ]
        scenarios = []
        spot_moves = [-0.05, -0.02, 0.00, 0.02, 0.05]
        iv_moves = [0.8, 1.0, 1.2] # -20%, Flat, +20%

        worst_impact = 0.0
        worst_scenario = "Flat"

        # Pre-calculate position basics to speed up loop
        pos_list = []
        for p in positions.values():
            if p.get("quantity", 0) == 0: continue
            
            # Extract or Default IV
            # If we calculated greeks earlier, we might have an IV, else use VIX
            iv = p.get("greeks", {}).get("iv", vix / 100.0)
            
            pos_list.append({
                "strike": p.get("strike", spot),
                "type": p.get("option_type", "CE"),
                "expiry_sec": self._get_time_fraction(p.get("expiry")),
                "qty": p.get("quantity") * (1 if p.get("side") == "BUY" else -1),
                "current_price": p.get("current_price", 0),
                "iv": iv
            })

        # Run Matrix
        results = {}
        for sm in spot_moves:
            sim_spot = spot * (1 + sm)
            for im in iv_moves:
                sim_pnl = 0.0
                
                for pos in pos_list:
                    # Sim IV
                    sim_iv = pos["iv"] * im
                    
                    # New Price (Black Scholes)
                    new_price = self._black_scholes(
                        S=sim_spot, 
                        K=pos["strike"], 
                        T=pos["expiry_sec"], 
                        r=self.risk_free_rate, 
                        sigma=sim_iv, 
                        flag=pos["type"]
                    )
                    
                    # PnL = (New Price - Current Price) * Qty
                    # Qty is negative for SELL, so if New Price > Curr Price, PnL is negative. Correct.
                    pnl_leg = (new_price - pos["current_price"]) * pos["qty"]
                    sim_pnl += pnl_leg

                tag = f"Spot {sm*100:+.0f}% / IV {im*100:.0f}%"
                results[tag] = sim_pnl
                
                if sim_pnl < worst_impact:
                    worst_impact = sim_pnl
                    worst_scenario = tag

        return {
            "matrix": results,
            "WORST_CASE": {
                "impact": worst_impact,
                "scenario": worst_scenario,
                "breach": worst_impact < -abs(self.max_loss_limit)
            }
        }

    def calculate_leg_greeks(
        self, 
        price: float, 
        spot: float, 
        strike: float, 
        time_years: float, 
        r: float, 
        opt_type: str
    ) -> Dict[str, float]:
        """
        Calculates Greeks for a single leg.
        Critical for V2 Portfolio which lacks this data.
        """
        if time_years <= 0 or spot <= 0:
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0}

        # 1. Back-solve IV from Price (Newton-Raphson)
        # Simplified: We assume IV ~ VIX for speed, or we could solve it. 
        # For a High Frequency system, solving IV for every tick is expensive.
        # Smart Compromise: Use a broad assumption or VIX, but refine Delta based on moneyness.
        
        sigma = 0.15 # Default
        
        # 2. Calculate Greeks
        d1 = (np.log(spot / strike) + (r + 0.5 * sigma ** 2) * time_years) / (sigma * np.sqrt(time_years))
        d2 = d1 - sigma * np.sqrt(time_years)
        
        nd1 = norm.pdf(d1)
        cdf_d1 = norm.cdf(d1)
        
        if opt_type == "CE":
            delta = cdf_d1
            theta = (- (spot * nd1 * sigma) / (2 * np.sqrt(time_years)) - r * strike * np.exp(-r * time_years) * norm.cdf(d2)) / 365.0
        else:
            delta = cdf_d1 - 1
            theta = (- (spot * nd1 * sigma) / (2 * np.sqrt(time_years)) + r * strike * np.exp(-r * time_years) * norm.cdf(-d2)) / 365.0
            
        gamma = nd1 / (spot * sigma * np.sqrt(time_years))
        vega = spot * np.sqrt(time_years) * nd1 / 100.0 # Per 1% Vol change

        return {
            "delta": round(delta, 3),
            "gamma": round(gamma, 4),
            "theta": round(theta, 2),
            "vega": round(vega, 2),
            "iv": sigma
        }

    # ==================================================================
    # INTERNAL MATH (The "engine" room)
    # ==================================================================

    def _black_scholes(self, S, K, T, r, sigma, flag="CE"):
        """
        Vectorized Black-Scholes Pricing Model.
        S: Spot, K: Strike, T: Time(Y), r: Rate, sigma: IV
        """
        if T <= 0:
            return max(0, S - K) if flag == "CE" else max(0, K - S)
            
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if flag == "CE":
            price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:
            price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
            
        return price

    def _get_time_fraction(self, expiry_str: Any) -> float:
        """Converts expiry string/date to Years"""
        try:
            if not expiry_str: return 0.0
            # Simple placeholder logic, assumes logic upstream handles actual date obj
            # In production, ensure this parses 'YYYY-MM-DD' vs Today
            return 0.05 # Default to ~18 days
        except:
            return 0.0
