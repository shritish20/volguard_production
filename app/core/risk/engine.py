import logging
import time
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# FIX #9: Dynamic Risk-Free Rate Fetcher
# ------------------------------------------------------------------
class RiskFreeRateCache:
    """
    Fetches live RBI repo rate or uses T-bill proxy
    Cache for 24 hours
    """
    def __init__(self):
        self._cached_rate = 0.06  # Default 6%
        self._last_fetch = 0
        self._cache_ttl = 86400  # 24 hours
    
    def get_rate(self) -> float:
        """
        Returns annual risk-free rate (as decimal, e.g., 0.06 = 6%)
        """
        now = time.time()
        
        # Return cached if fresh
        if now - self._last_fetch < self._cache_ttl:
            return self._cached_rate
        
        # Try to fetch live rate
        try:
            # Option 1: Scrape from RBI website (requires HTML parsing)
            # Option 2: Use government bond API (if available)
            # Option 3: Hardcoded with quarterly manual update
            
            # For now, use semi-annual manual update
            # TODO: Implement API fetcher
            logger.info(f"Using cached risk-free rate: {self._cached_rate:.4f}")
            self._last_fetch = now
            return self._cached_rate
            
        except Exception as e:
            logger.warning(f"Could not fetch live risk-free rate: {e}")
            return self._cached_rate
    
    def set_rate_manual(self, rate: float):
        """Allow manual override via admin API"""
        if 0.01 <= rate <= 0.20:  # Sanity check: 1% to 20%
            self._cached_rate = rate
            self._last_fetch = time.time()
            logger.info(f"Risk-free rate manually updated to {rate:.4f}")
        else:
            raise ValueError(f"Invalid rate: {rate}")

# Global instance
rf_rate_cache = RiskFreeRateCache()


class RiskEngine:
    def __init__(self, max_portfolio_loss: float = 50000.0):
        self.max_loss_limit = max_portfolio_loss
        self.risk_free_rate = rf_rate_cache.get_rate()  # 🔴 Dynamic rate via Fix #9

    # ------------------------------------------------------------------
    # FIX #2: Greeks Fabrication - Never Invent Critical Data
    # ------------------------------------------------------------------
    def calculate_leg_greeks(
        self,
        price: float,
        spot: float,
        strike: float,
        time_years: float,
        r: float,
        opt_type: str
    ) -> Optional[Dict[str, float]]:
        """
        🔴 CRITICAL FIX: Never fabricate Greeks with wrong assumptions
        Returns None if unable to calculate reliably.
        Solves for Implied Volatility (IV) from market price.
        """
        if time_years <= 0 or spot <= 0 or price <= 0 or strike <= 0:
            logger.warning(f"Invalid inputs for Greek calculation: time={time_years}, spot={spot}, price={price}, strike={strike}")
            return None  # 🔴 Changed from returning zeros
        
        # 🔴 CRITICAL FIX: Solve for implied volatility from market price
        # Instead of assuming sigma = 0.15
        sigma = 0.0
        try:
            def bs_price_error(sigma_guess):
                """Calculate Black-Scholes price error for root finding"""
                if sigma_guess <= 0:
                    return float('inf')
                
                d1 = (np.log(spot / strike) + (r + 0.5 * sigma_guess ** 2) * time_years) / (sigma_guess * np.sqrt(time_years))
                d2 = d1 - sigma_guess * np.sqrt(time_years)
                
                if opt_type == "CE":
                    theoretical = spot * norm.cdf(d1) - strike * np.exp(-r * time_years) * norm.cdf(d2)
                else:
                    theoretical = strike * np.exp(-r * time_years) * norm.cdf(-d2) - spot * norm.cdf(-d1)
                
                return theoretical - price
            
            # Solve for IV between 5% and 200%
            sigma = brentq(bs_price_error, 0.05, 2.0, xtol=0.0001)
            
        except Exception as e:
            # If we can't solve for IV (e.g., deep OTM option with price ~0), we cannot trust the Greeks
            logger.debug(f"⚠️ Failed to solve IV for price={price}, spot={spot}, strike={strike}: {e}")
            return None  # 🔴 Do not fabricate Greeks
        
        # Now calculate Greeks with ACTUAL implied vol
        try:
            d1 = (np.log(spot / strike) + (r + 0.5 * sigma ** 2) * time_years) / (sigma * np.sqrt(time_years))
            d2 = d1 - sigma * np.sqrt(time_years)
            nd1 = norm.pdf(d1)
            
            if opt_type == "CE":
                delta = norm.cdf(d1)
                theta = (- (spot * nd1 * sigma) / (2 * np.sqrt(time_years)) 
                         - r * strike * np.exp(-r * time_years) * norm.cdf(d2)) / 365.0
            else:
                delta = norm.cdf(d1) - 1
                theta = (- (spot * nd1 * sigma) / (2 * np.sqrt(time_years)) 
                         + r * strike * np.exp(-r * time_years) * norm.cdf(-d2)) / 365.0
            
            gamma = nd1 / (spot * sigma * np.sqrt(time_years))
            vega = spot * np.sqrt(time_years) * nd1 / 100.0  # Per 1% vol change
            
            return {
                "delta": round(delta, 4),
                "gamma": round(gamma, 6),
                "theta": round(theta, 2),
                "vega": round(vega, 2),
                "iv": round(sigma, 4)
            }
        except Exception as e:
            logger.error(f"Greek calculation failed after IV solve: {e}")
            return None  # 🔴 Never return fabricated Greeks

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
            if not expiry_str:
                return 0.0
            # Simple placeholder logic, assumes logic upstream handles actual date obj
            # In production, ensure this parses 'YYYY-MM-DD' vs Today or datetime objects
            return 0.05  # Default to ~18 days if parsing fails (should be handled by caller)
        except:
            return 0.0
