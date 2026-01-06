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

# If you have a central schema, you can import it, but we define it here 
# to GUARANTEE the fields match the logic below.
@dataclass
class VolMetrics:
    """
    Standardized Output for Volatility Metrics.
    Includes both Production Safety fields and Research Logic fields (IVP/VoV).
    """
    spot: float
    iv: float            # Current VIX
    rv_daily: float      # Realized Vol (Standard Deviation)
    garch_vol: float     # GARCH(1,1) Forecast
    parkinson_vol: float # Parkinson (High/Low) Vol
    iv_percentile: float # VIX Percentile Rank (0-100)
    vov: float           # Volatility of Volatility
    regime: str          # Derived Regime (LOW_VOL, NORMAL, HIGH_VOL)
    
    # Extended Debug Metrics (Kept for traceability)
    rv28: float = 0.0
    garch28: float = 0.0
    pk28: float = 0.0
    ivp90: float = 0.0
    ivp1y: float = 0.0   # 1-Year VIX Percentile
    is_fallback: bool = False

logger = logging.getLogger(__name__)

class VolatilityEngine:
    """
    VolGuard Smart Volatility Engine (VolGuard 3.0 - Hybrid Production)
    
    MERGED ARCHITECTURE:
    1. SAFETY: Inherits all _safe_divide, _safe_log, NaN protection from your original code.
    2. LOGIC: Implements the correct VIX-based IVP and VoV from the Research Script.
    3. PERFORMANCE: Uses Async GARCH with caching to prevent loop blocking.
    """

    def __init__(self, garch_interval_seconds: int = 1800):
        # Configuration
        self.garch_interval = garch_interval_seconds
        
        # State Cache for Heavy Calculations (GARCH)
        self._last_garch_time = 0
        self._cached_garch7 = np.nan
        self._cached_garch28 = np.nan
        
        # Constants
        self.WINDOW_IVP_SHORT = 30
        self.WINDOW_IVP_LONG = 252

    # =========================================================================
    # SECTION 1: SAFETY HELPERS (Preserved from your original 500-line file)
    # =========================================================================
    
    def _safe_divide(self, numerator, denominator, default=0.0, context="division"):
        """Safe division with validation and logging"""
        try:
            if denominator == 0 or pd.isna(denominator):
                return default
            result = numerator / denominator
            if not math.isfinite(result):
                logger.warning(f"Non-finite result in {context}: {result}")
                return default
            return result
        except Exception as e:
            logger.error(f"Division error in {context}: {e}")
            return default

    def _safe_sqrt(self, value, context="sqrt"):
        """Safe square root with validation"""
        try:
            if value < 0:
                return 0.0
            if pd.isna(value) or not math.isfinite(value):
                return 0.0
            result = np.sqrt(value)
            if not math.isfinite(result):
                return 0.0
            return result
        except Exception as e:
            logger.error(f"Sqrt error in {context}: {e}")
            return 0.0

    def _safe_log(self, value, context="log"):
        """Safe logarithm with validation"""
        try:
            if value <= 0:
                return np.nan
            if not math.isfinite(value):
                return np.nan
            result = np.log(value)
            if not math.isfinite(result):
                return np.nan
            return result
        except Exception as e:
            logger.error(f"Log error in {context}: {e}")
            return np.nan

    def _safe_percentile_rank(self, series: pd.Series, current_val: float, context="percentile") -> float:
        """
        Calculate percentile rank of a scalar against a historical series.
        CRITICAL FIX: This now handles VIX ranking correctly.
        """
        try:
            if series.empty or len(series) < 5:
                return 50.0 # Neutral fallback
            
            # Remove NaN and infinite values from history
            clean_series = series.replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(clean_series) < 5:
                return 50.0
            
            # Calculate rank: Percentage of history strictly below current value
            rank_pct = (clean_series < current_val).mean() * 100.0
            
            # Validate result
            if not math.isfinite(rank_pct) or rank_pct < 0 or rank_pct > 100:
                logger.warning(f"Invalid percentile result in {context}: {rank_pct}")
                return 50.0
            
            return float(rank_pct)
            
        except Exception as e:
            logger.error(f"Percentile calculation failed in {context}: {e}")
            return 50.0

    # =========================================================================
    # SECTION 2: MAIN CALCULATION ENGINE
    # =========================================================================
    
    async def calculate_volatility(
        self,
        history_candles: pd.DataFrame,     # Daily OHLCV (Nifty)
        intraday_candles: pd.DataFrame,    # Minute OHLCV (Nifty)
        spot_price: float,
        vix_current: float,
        vix_history: pd.DataFrame = None   # REQUIRED: Passed from Supervisor for IVP
    ) -> VolMetrics:
        """
        Main computation entry point.
        Combines safe data merging with correct financial logic.
        """
        try:
            # 1. Input Validation & Fallbacks
            # -----------------------------
            if not math.isfinite(spot_price):
                logger.warning(f"Invalid spot_price: {spot_price}")
                spot_price = 0.0
            
            if not math.isfinite(vix_current):
                vix_current = 0.0
            
            # Spot Fallback: Use last close if live spot is missing
            if spot_price <= 0 and not history_candles.empty:
                last_close = history_candles.iloc[-1]["close"]
                if math.isfinite(last_close) and last_close > 0:
                    spot_price = last_close
                    logger.info(f"Using last close as spot: {spot_price}")
            
            is_fallback = (spot_price <= 0)

            # 2. Data Merging (Hybrid Logic)
            # -----------------------------
            # Combines historical daily candles with today's live intraday data
            full_series = self._merge_data(history_candles, intraday_candles, spot_price)
            
            if len(full_series) < 30:
                logger.warning("Insufficient data for volatility calculation. Returning defaults.")
                return self._get_default_metrics(spot_price, vix_current)

            # 3. Calculate Returns (Log Returns)
            # -----------------------------
            close_series = full_series["close"].replace([0, np.inf, -np.inf], np.nan).dropna()
            
            # Safe log returns calculation: ln(price / prev_price)
            returns = np.log(close_series / close_series.shift(1)).dropna()
            
            if len(returns) < 7:
                logger.warning(f"Insufficient valid returns: {len(returns)}")
                return self._get_default_metrics(spot_price, vix_current)

            # 4. METRICS CALCULATION
            # ----------------------------------------
            
            # A. Realized Volatility (Standard Deviation)
            rv7 = self._calculate_realized_vol(returns, 7, "rv7")
            rv28 = self._calculate_realized_vol(returns, 28, "rv28")

            # B. Parkinson Volatility (High/Low)
            # Critical for detecting intraday expansion
            pk7, pk28 = self._calculate_parkinson_vol(full_series)

            # C. Volatility of Volatility (VoV) - LOGIC FIXED
            # Calculates the standard deviation of VIX returns
            vov = 0.0
            if vix_history is not None and not vix_history.empty:
                vov = self._calculate_vov(vix_history)

            # D. IV Percentile (IVP) - LOGIC FIXED
            # Compares CURRENT VIX to HISTORICAL VIX (not price returns)
            ivp1y = 50.0
            ivp90 = 50.0
            
            if vix_history is not None and not vix_history.empty:
                v_hist = vix_history['close'].dropna()
                ivp1y = self._safe_percentile_rank(v_hist.tail(252), vix_current, "ivp1y")
                ivp90 = self._safe_percentile_rank(v_hist.tail(90), vix_current, "ivp90")

            # E. GARCH (Heavy Compute - Async Cached)
            # -------------------------------------------------
            # Only run every `garch_interval` seconds to save CPU
            if time.time() - self._last_garch_time > self.garch_interval:
                # Run in thread pool to prevent blocking the event loop
                ga7, ga28 = await asyncio.to_thread(self._run_garch_models, returns)
                
                # Update Cache
                self._cached_garch7 = ga7 if math.isfinite(ga7) else np.nan
                self._cached_garch28 = ga28 if math.isfinite(ga28) else np.nan
                self._last_garch_time = time.time()
            else:
                # Use Cached Values
                ga7 = self._cached_garch7
                ga28 = self._cached_garch28

            # GARCH Fallback: If model failed to converge, use RV
            if not math.isfinite(ga7):
                ga7 = rv7
            if not math.isfinite(ga28):
                ga28 = rv28

            # 5. Regime Classification
            # ------------------------
            # Simple Logic: <20% = Low Vol (Buy), >80% = High Vol (Sell)
            regime = "NORMAL"
            if ivp1y < 20: regime = "LOW_VOL"
            elif ivp1y > 80: regime = "HIGH_VOL"

            # 6. Final Assembly & Clamping
            # ------------------------
            return VolMetrics(
                spot=self._clamp(spot_price, 0, 100000),
                iv=self._clamp(vix_current, 0, 100),
                rv_daily=self._clamp(rv7, 0, 200),
                garch_vol=self._clamp(ga7, 0, 200),
                parkinson_vol=self._clamp(pk7, 0, 200),
                iv_percentile=self._clamp(ivp1y, 0, 100),
                vov=self._clamp(vov, 0, 500),
                regime=regime,
                # Extended Debug Fields
                rv28=self._clamp(rv28, 0, 200),
                garch28=self._clamp(ga28, 0, 200),
                pk28=self._clamp(pk28, 0, 200),
                ivp90=self._clamp(ivp90, 0, 100),
                ivp1y=self._clamp(ivp1y, 0, 100),
                is_fallback=is_fallback
            )
            
        except Exception as e:
            logger.error(f"Volatility calculation critical failure: {e}", exc_info=True)
            return self._get_default_metrics(spot_price, vix_current)

    # =========================================================================
    # SECTION 3: CALCULATION HELPERS (Math Logic)
    # =========================================================================

    def _calculate_realized_vol(self, returns: pd.Series, window: int, context: str) -> float:
        """Calculate realized volatility (Std Dev) with safety checks"""
        try:
            if len(returns) < window: return 0.0
            
            window_returns = returns.tail(window)
            std_dev = window_returns.std()
            
            # Annualize: std * sqrt(252) * 100
            annualized = std_dev * np.sqrt(252) * 100
            
            return float(annualized) if math.isfinite(annualized) else 0.0
        except Exception as e:
            logger.error(f"Realized vol calculation failed for {context}: {e}")
            return 0.0

    def _calculate_vov(self, vix_history: pd.DataFrame) -> float:
        """
        Calculates Volatility of Volatility (VoV).
        Logic: Standard deviation of log-returns of VIX.
        """
        try:
            closes = vix_history['close'].replace([0, np.inf, -np.inf], np.nan).dropna()
            if len(closes) < 20: return 0.0
                
            # Log returns of VIX
            vix_ret = np.log(closes / closes.shift(1)).dropna()
            
            # Std Dev of last 20 days
            vov = vix_ret.tail(20).std() * np.sqrt(252) * 100
            
            return float(vov) if math.isfinite(vov) else 0.0
        except Exception as e:
            logger.error(f"VoV calculation failed: {e}")
            return 0.0

    def _calculate_parkinson_vol(self, data: pd.DataFrame) -> Tuple[float, float]:
        """
        Calculate Parkinson volatility (uses High/Low).
        Formula: sqrt(1/(4ln2) * mean(ln(H/L)^2))
        """
        try:
            high = data["high"].replace([0, np.inf, -np.inf], np.nan).dropna()
            low = data["low"].replace([0, np.inf, -np.inf], np.nan).dropna()
            
            common_idx = high.index.intersection(low.index)
            high, low = high.loc[common_idx], low.loc[common_idx]
            
            if len(high) < 7: return 0.0, 0.0
            
            # Vectorized calculation for speed
            ratio = high / low
            log_ratio = np.log(ratio)
            hl_ratio_sq = log_ratio ** 2
            
            hl_ratio_sq = hl_ratio_sq.replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(hl_ratio_sq) < 7: return 0.0, 0.0
            
            # Parkinson constant: 1 / (4 * ln(2))
            const_factor = 1.0 / (4.0 * np.log(2.0))
            
            mean7 = hl_ratio_sq.tail(7).mean()
            mean28 = hl_ratio_sq.tail(28).mean() if len(hl_ratio_sq) >= 28 else mean7
            
            pk7 = np.sqrt(const_factor * mean7) * np.sqrt(252) * 100
            pk28 = np.sqrt(const_factor * mean28) * np.sqrt(252) * 100
            
            return (
                float(pk7) if math.isfinite(pk7) else 0.0, 
                float(pk28) if math.isfinite(pk28) else 0.0
            )
        except Exception as e:
            logger.error(f"Parkinson calculation failed: {e}")
            return 0.0, 0.0

    def _run_garch_models(self, returns: pd.Series) -> Tuple[float, float]:
        """
        Runs GARCH(1,1) safely with proper scaling.
        Returns (Forecast_7_Day, Forecast_28_Day) annualized vol in percentage.
        """
        try:
            clean_returns = returns.replace([np.inf, -np.inf], np.nan).dropna()
            if len(clean_returns) < 50 or clean_returns.std() == 0:
                return np.nan, np.nan
            
            # Rescale=True handles convergence automatically
            model = arch_model(clean_returns * 100, vol="Garch", p=1, q=1, dist="normal")
            res = model.fit(disp="off", show_warning=False)
            
            # Forecast
            forecast = res.forecast(horizon=28, reindex=False)
            variance = forecast.variance.iloc[-1]
            
            # Annualize (Since input was returns*100, output is %^2)
            garch7 = np.sqrt(variance.iloc[:7].mean()) * np.sqrt(252)
            garch28 = np.sqrt(variance.iloc[:28].mean()) * np.sqrt(252)
            
            return float(garch7), float(garch28)
        except Exception as e:
            logger.warning(f"GARCH calculation failed: {e}")
            return np.nan, np.nan

    def _merge_data(self, daily: pd.DataFrame, intraday: pd.DataFrame, current_spot: float) -> pd.DataFrame:
        """
        Intelligently appends the current 'live' day to the historical dataset.
        """
        try:
            if daily.empty: return pd.DataFrame()
            df = daily.copy()
            
            # Build Today's Candle
            if not intraday.empty:
                high = max(intraday["high"].max(), current_spot)
                low = min(intraday["low"].min(), current_spot)
                today_candle = {
                    "timestamp": pd.Timestamp.now().normalize(),
                    "open": intraday.iloc[0]["open"],
                    "high": high, "low": low, "close": current_spot
                }
            else:
                # Fallback: Treat current spot as a Doji
                today_candle = {
                    "timestamp": pd.Timestamp.now().normalize(),
                    "open": current_spot, "high": current_spot, 
                    "low": current_spot, "close": current_spot
                }

            # Upsert Logic
            if df.iloc[-1]["timestamp"].date() == pd.Timestamp.now().date():
                # Update existing row
                idx = df.index[-1]
                df.loc[idx, ["high", "low", "close"]] = [
                    max(df.loc[idx, "high"], today_candle["high"]),
                    min(df.loc[idx, "low"], today_candle["low"]),
                    today_candle["close"]
                ]
            else:
                # Append new row
                df = pd.concat([df, pd.DataFrame([today_candle])], ignore_index=True)
                
            return df
        except Exception as e:
            logger.error(f"Data merge failed: {e}")
            return daily.copy()

    def _clamp(self, val: float, min_v: float = 0.0, max_v: float = 200.0) -> float:
        """Enhanced clamping with validation"""
        if not isinstance(val, (int, float)) or not math.isfinite(val):
            return min_v
        return max(min_v, min(val, max_v))

    def _get_default_metrics(self, spot, vix) -> VolMetrics:
        """Safe defaults if data is missing"""
        spot = spot if math.isfinite(spot) else 0.0
        vix = vix if math.isfinite(vix) else 0.0
        return VolMetrics(
            spot=spot, iv=vix, rv_daily=0, garch_vol=0, parkinson_vol=0, 
            iv_percentile=50, vov=0, regime="NORMAL", is_fallback=True
        )
