# app/core/analytics/volatility.py

import numpy as np
import pandas as pd
import logging
import time
import asyncio
import math
from arch import arch_model
from typing import Tuple, Dict, Optional, List
from dataclasses import dataclass

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
logger = logging.getLogger(__name__)

# =============================================================================
# DATA STRUCTURES (Aligned with Dashboard)
# =============================================================================
@dataclass
class VolMetrics:
    """
    Standardized Output for Volatility Metrics.
    Now includes explicit fields for Dashboard compatibility.
    """
    # Core Fields
    spot: float
    iv: float            # Current VIX (Dashboard: 'vix')
    vov: float           # Volatility of Volatility
    regime: str          # Derived Regime
    
    # Dashboard Specific Fields
    ivp30: float         # 30-Day VIX Percentile
    ivp90: float         # 90-Day VIX Percentile
    ivp1y: float         # 1-Year VIX Percentile
    
    rv7: float           # 7-Day Realized Vol
    rv28: float          # 28-Day Realized Vol
    
    garch7: float        # 7-Day GARCH Forecast
    garch28: float       # 28-Day GARCH Forecast
    
    pk7: float           # 7-Day Parkinson Vol
    pk28: float          # 28-Day Parkinson Vol
    
    is_fallback: bool = False

    # Aliases for backward compatibility (if other engines use them)
    @property
    def rv_daily(self): return self.rv7
    
    @property
    def garch_vol(self): return self.garch7
    
    @property
    def parkinson_vol(self): return self.pk7
    
    @property
    def iv_percentile(self): return self.ivp1y

# =============================================================================
# CORE ENGINE
# =============================================================================
class VolatilityEngine:
    """
    VolGuard Smart Volatility Engine (VolGuard 3.0 - Dashboard Optimized)
    
    MERGED ARCHITECTURE:
    1. SAFETY: Inherits all _safe_divide, _safe_log, NaN protection.
    2. LOGIC: Implements the correct VIX-based IVP and VoV.
    3. COMPATIBILITY: Outputs fields exactly as Dashboard.py expects.
    """

    def __init__(self, garch_interval_seconds: int = 1800):
        self.garch_interval = garch_interval_seconds
        
        # State Cache
        self._last_garch_time = 0
        self._cached_garch7 = np.nan
        self._cached_garch28 = np.nan

    # =========================================================================
    # SAFETY HELPERS
    # =========================================================================
    
    def _safe_divide(self, numerator, denominator, default=0.0):
        try:
            if denominator == 0 or pd.isna(denominator): return default
            result = numerator / denominator
            return result if math.isfinite(result) else default
        except: return default

    def _safe_sqrt(self, value):
        try:
            if value < 0 or pd.isna(value) or not math.isfinite(value): return 0.0
            return np.sqrt(value)
        except: return 0.0

    def _safe_percentile_rank(self, series: pd.Series, current_val: float) -> float:
        """Calculate rank of a scalar against historical series"""
        try:
            if series.empty or len(series) < 5: return 50.0
            clean = series.replace([np.inf, -np.inf], np.nan).dropna()
            if len(clean) < 5: return 50.0
            
            rank_pct = (clean < current_val).mean() * 100.0
            if not math.isfinite(rank_pct): return 50.0
            return float(rank_pct)
        except: return 50.0

    # =========================================================================
    # MAIN CALCULATION
    # =========================================================================
    
    async def calculate_volatility(
        self,
        history_candles: pd.DataFrame,
        intraday_candles: pd.DataFrame,
        spot_price: float,
        vix_current: float,
        vix_history: pd.DataFrame = None
    ) -> VolMetrics:
        """
        Main computation entry point.
        """
        try:
            # 1. Validation
            if not math.isfinite(spot_price): spot_price = 0.0
            if not math.isfinite(vix_current): vix_current = 0.0
            
            # Fallback for spot
            if spot_price <= 0 and not history_candles.empty:
                spot_price = history_candles.iloc[-1]["close"]
            
            is_fallback = (spot_price <= 0)

            # 2. Merge Data
            full_series = self._merge_data(history_candles, intraday_candles, spot_price)
            if len(full_series) < 30:
                return self._get_default_metrics(spot_price, vix_current)

            # 3. Calculate Returns
            close_series = full_series["close"].replace([0, np.inf, -np.inf], np.nan).dropna()
            returns = np.log(close_series / close_series.shift(1)).dropna()
            
            if len(returns) < 7:
                return self._get_default_metrics(spot_price, vix_current)

            # 4. METRICS CALCULATION
            
            # A. Realized Volatility
            rv7 = self._calculate_realized_vol(returns, 7)
            rv28 = self._calculate_realized_vol(returns, 28)

            # B. Parkinson Volatility
            pk7, pk28 = self._calculate_parkinson_vol(full_series)

            # C. Volatility of Volatility (VoV)
            vov = 0.0
            if vix_history is not None and not vix_history.empty:
                vov = self._calculate_vov(vix_history)

            # D. IV Percentile (IVP) - The Fix
            ivp1y = 50.0
            ivp90 = 50.0
            ivp30 = 50.0
            
            if vix_history is not None and not vix_history.empty:
                v_hist = vix_history['close'].dropna()
                ivp1y = self._safe_percentile_rank(v_hist.tail(252), vix_current)
                ivp90 = self._safe_percentile_rank(v_hist.tail(90), vix_current)
                ivp30 = self._safe_percentile_rank(v_hist.tail(30), vix_current)

            # E. GARCH (Async)
            if time.time() - self._last_garch_time > self.garch_interval:
                ga7, ga28 = await asyncio.to_thread(self._run_garch_models, returns)
                self._cached_garch7 = ga7 if math.isfinite(ga7) else np.nan
                self._cached_garch28 = ga28 if math.isfinite(ga28) else np.nan
                self._last_garch_time = time.time()
            else:
                ga7 = self._cached_garch7
                ga28 = self._cached_garch28

            # Fallbacks
            if not math.isfinite(ga7): ga7 = rv7
            if not math.isfinite(ga28): ga28 = rv28

            # 5. Regime
            regime = "NORMAL"
            if ivp1y < 20: regime = "LOW_VOL"
            elif ivp1y > 80: regime = "HIGH_VOL"

            return VolMetrics(
                spot=self._clamp(spot_price, 0, 100000),
                iv=self._clamp(vix_current, 0, 100),
                vov=self._clamp(vov, 0, 500),
                regime=regime,
                
                # Explicit Fields for Dashboard
                ivp30=self._clamp(ivp30, 0, 100),
                ivp90=self._clamp(ivp90, 0, 100),
                ivp1y=self._clamp(ivp1y, 0, 100),
                rv7=self._clamp(rv7, 0, 200),
                rv28=self._clamp(rv28, 0, 200),
                garch7=self._clamp(ga7, 0, 200),
                garch28=self._clamp(ga28, 0, 200),
                pk7=self._clamp(pk7, 0, 200),
                pk28=self._clamp(pk28, 0, 200),
                
                is_fallback=is_fallback
            )
            
        except Exception as e:
            logger.error(f"Vol Calc Failed: {e}", exc_info=True)
            return self._get_default_metrics(spot_price, vix_current)

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _calculate_realized_vol(self, returns, window):
        try:
            if len(returns) < window: return 0.0
            std = returns.tail(window).std()
            return float(std * np.sqrt(252) * 100)
        except: return 0.0

    def _calculate_vov(self, vix_history):
        try:
            closes = vix_history['close'].replace([0, np.inf], np.nan).dropna()
            if len(closes) < 20: return 0.0
            rets = np.log(closes / closes.shift(1)).dropna()
            return float(rets.tail(20).std() * np.sqrt(252) * 100)
        except: return 0.0

    def _calculate_parkinson_vol(self, data):
        try:
            h = data["high"].replace([0, np.inf], np.nan).dropna()
            l = data["low"].replace([0, np.inf], np.nan).dropna()
            idx = h.index.intersection(l.index)
            if len(idx) < 7: return 0.0, 0.0
            
            sq = (np.log(h.loc[idx] / l.loc[idx]) ** 2)
            const = 1.0 / (4.0 * np.log(2.0))
            
            pk7 = np.sqrt(const * sq.tail(7).mean()) * np.sqrt(252) * 100
            pk28 = np.sqrt(const * sq.tail(28).mean()) * np.sqrt(252) * 100
            
            return (pk7 if math.isfinite(pk7) else 0.0, 
                    pk28 if math.isfinite(pk28) else 0.0)
        except: return 0.0, 0.0

    def _run_garch_models(self, returns):
        try:
            clean = returns.replace([np.inf, -np.inf], np.nan).dropna()
            if len(clean) < 50 or clean.std() == 0: return np.nan, np.nan
            
            model = arch_model(clean * 100, vol="Garch", p=1, q=1)
            res = model.fit(disp="off", show_warning=False)
            var = res.forecast(horizon=28, reindex=False).variance.iloc[-1]
            
            g7 = np.sqrt(var.iloc[:7].mean()) * np.sqrt(252)
            g28 = np.sqrt(var.iloc[:28].mean()) * np.sqrt(252)
            return float(g7), float(g28)
        except: return np.nan, np.nan

    def _merge_data(self, daily, intraday, spot):
        try:
            df = daily.copy() if not daily.empty else pd.DataFrame()
            if not intraday.empty:
                high = max(intraday["high"].max(), spot)
                low = min(intraday["low"].min(), spot)
                open_ = intraday.iloc[0]["open"]
            else:
                high = low = open_ = spot
            
            today = {
                "timestamp": pd.Timestamp.now().normalize(),
                "open": open_, "high": high, "low": low, "close": spot
            }
            
            if not df.empty and df.iloc[-1]["timestamp"].date() == today["timestamp"].date():
                idx = df.index[-1]
                df.loc[idx, ["high", "low", "close"]] = [
                    max(df.loc[idx, "high"], high),
                    min(df.loc[idx, "low"], low),
                    spot
                ]
            else:
                df = pd.concat([df, pd.DataFrame([today])], ignore_index=True)
            return df
        except: return daily

    def _clamp(self, val, min_v, max_v):
        if not isinstance(val, (int, float)) or not math.isfinite(val): return min_v
        return max(min_v, min(val, max_v))

    def _get_default_metrics(self, spot, vix):
        return VolMetrics(
            spot=spot, iv=vix, vov=0, regime="NORMAL",
            ivp30=50, ivp90=50, ivp1y=50,
            rv7=0, rv28=0, garch7=0, garch28=0, pk7=0, pk28=0,
            is_fallback=True
        )
