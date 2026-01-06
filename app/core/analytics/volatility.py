# app/core/analytics/volatility.py

import numpy as np
import pandas as pd
import logging
import time
import asyncio
import math
from arch import arch_model
from typing import Tuple, Dict, Optional
from app.schemas.analytics import VolMetrics

logger = logging.getLogger(__name__)

class VolatilityEngine:
    """
    VolGuard Smart Volatility Engine (VolGuard 3.0)
    
    Architecture:
    - Hybrid Data: Merges Long-term History (PG) + Real-time Intraday (Redis/API).
    - CPU Optimization: Runs GARCH every 30 mins, light metrics every cycle.
    - Crash Detection: Uses Intraday High/Low for immediate Parkinson Vol spikes.
    
    ENHANCEMENTS:
    - ✅ Comprehensive NaN/Inf protection at every calculation step
    - ✅ Safe division with zero checks
    - ✅ Robust percentile calculations
    - ✅ Enhanced GARCH validation
    - ✅ Better error context logging
    """

    def __init__(self, garch_interval_seconds: int = 1800):
        # Configuration
        self.garch_interval = garch_interval_seconds
        
        # State Cache for Heavy Calculations
        self._last_garch_time = 0
        self._cached_garch7 = np.nan
        self._cached_garch28 = np.nan

    def _safe_divide(self, numerator, denominator, default=0.0, context="division"):
        """Safe division with validation and logging"""
        try:
            if denominator == 0 or pd.isna(denominator):
                logger.warning(f"Division by zero in {context}")
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
                logger.warning(f"Negative value for sqrt in {context}: {value}")
                return 0.0
            if pd.isna(value) or not math.isfinite(value):
                logger.warning(f"Invalid value for sqrt in {context}: {value}")
                return 0.0
            result = np.sqrt(value)
            if not math.isfinite(result):
                logger.warning(f"Non-finite sqrt result in {context}")
                return 0.0
            return result
        except Exception as e:
            logger.error(f"Sqrt error in {context}: {e}")
            return 0.0

    def _safe_log(self, value, context="log"):
        """Safe logarithm with validation"""
        try:
            if value <= 0:
                logger.warning(f"Non-positive value for log in {context}: {value}")
                return np.nan
            if not math.isfinite(value):
                logger.warning(f"Non-finite value for log in {context}: {value}")
                return np.nan
            result = np.log(value)
            if not math.isfinite(result):
                logger.warning(f"Non-finite log result in {context}")
                return np.nan
            return result
        except Exception as e:
            logger.error(f"Log error in {context}: {e}")
            return np.nan

    def _safe_percentile_rank(self, series: pd.Series, context="percentile") -> float:
        """
        Calculate percentile rank safely.
        Returns value between 0-100, or 0.0 if calculation fails.
        """
        try:
            if series.empty or len(series) < 2:
                logger.warning(f"Insufficient data for {context} percentile")
                return 0.0
            
            # Remove NaN and infinite values
            clean_series = series.replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(clean_series) < 2:
                logger.warning(f"Insufficient valid data for {context} percentile after cleaning")
                return 0.0
            
            # Get the last value
            last_val = clean_series.iloc[-1]
            
            if not math.isfinite(last_val):
                logger.warning(f"Non-finite last value in {context} percentile")
                return 0.0
            
            # Calculate rank
            rank_pct = (clean_series < last_val).sum() / len(clean_series) * 100
            
            # Validate result
            if not math.isfinite(rank_pct) or rank_pct < 0 or rank_pct > 100:
                logger.warning(f"Invalid percentile result in {context}: {rank_pct}")
                return 0.0
            
            return float(rank_pct)
            
        except Exception as e:
            logger.error(f"Percentile calculation failed in {context}: {e}")
            return 0.0

    async def calculate_volatility(
        self,
        daily_data: pd.DataFrame,
        intraday_data: pd.DataFrame,
        spot_live: float,
        vix_live: float
    ) -> VolMetrics:
        """
        Main computation entry point with comprehensive error handling.
        """
        try:
            # 1. Validate inputs
            if not math.isfinite(spot_live):
                logger.warning(f"Invalid spot_live: {spot_live}")
                spot_live = 0.0
            
            if not math.isfinite(vix_live):
                logger.warning(f"Invalid vix_live: {vix_live}")
                vix_live = 0.0
            
            # 2. Fallback Checks
            if spot_live <= 0 and not daily_data.empty:
                last_close = daily_data.iloc[-1]["close"]
                if math.isfinite(last_close) and last_close > 0:
                    spot_live = last_close
                    logger.info(f"Using last close as spot: {spot_live}")
            
            is_fallback = (spot_live <= 0)

            # 3. Merge Data (The Hybrid Logic)
            full_series = self._merge_data(daily_data, intraday_data, spot_live)
            
            if len(full_series) < 30:
                logger.warning("Insufficient data for volatility calculation. Returning defaults.")
                return self._get_default_metrics(spot_live, vix_live)

            # 4. Calculate Returns (Log Returns) with safety
            close_series = full_series["close"].replace([0, np.inf, -np.inf], np.nan).dropna()
            
            if len(close_series) < 2:
                logger.warning("Insufficient price data for returns calculation")
                return self._get_default_metrics(spot_live, vix_live)
            
            # Safe log returns calculation
            returns = pd.Series(index=close_series.index[1:], dtype=float)
            for i in range(1, len(close_series)):
                curr_price = close_series.iloc[i]
                prev_price = close_series.iloc[i-1]
                log_ret = self._safe_log(
                    self._safe_divide(curr_price, prev_price, 1.0, f"returns[{i}]"),
                    f"returns[{i}]"
                )
                returns.iloc[i-1] = log_ret
            
            returns = returns.dropna()
            
            if len(returns) < 7:
                logger.warning(f"Insufficient valid returns: {len(returns)}")
                return self._get_default_metrics(spot_live, vix_live)

            # 5. LIGHT Calculations (Run every cycle)
            # ----------------------------------------
            
            # A. Realized Volatility (Standard Deviation)
            rv7 = self._calculate_realized_vol(returns, 7, "rv7")
            rv28 = self._calculate_realized_vol(returns, 28, "rv28")

            # B. Parkinson Volatility (Uses High/Low)
            pk7, pk28 = self._calculate_parkinson_vol(full_series)

            # C. Vol of Vol (Placeholder - could implement rolling std of volatility)
            vov = 0.0

            # 6. HEAVY Calculations (GARCH - Run conditionally)
            # -------------------------------------------------
            if time.time() - self._last_garch_time > self.garch_interval:
                # Offload to thread to avoid blocking loop
                ga7, ga28 = await asyncio.to_thread(self._run_garch_models, returns)
                
                # Update Cache
                self._cached_garch7 = ga7 if math.isfinite(ga7) else np.nan
                self._cached_garch28 = ga28 if math.isfinite(ga28) else np.nan
                self._last_garch_time = time.time()
            else:
                # Use Cache
                ga7 = self._cached_garch7
                ga28 = self._cached_garch28

            # If GARCH failed or is NaN, fallback to RV
            if not math.isfinite(ga7):
                ga7 = rv7
                logger.info("Using RV7 as GARCH7 fallback")
            if not math.isfinite(ga28):
                ga28 = rv28
                logger.info("Using RV28 as GARCH28 fallback")

            # 7. IV Percentile (Rank) with safe calculation
            ivp30 = self._calculate_ivp(returns, 30)
            ivp90 = self._calculate_ivp(returns, 90)
            ivp1y = self._calculate_ivp(returns, 252)

            # 8. Final validation and return
            return VolMetrics(
                spot=self._clamp(spot_live, 0, 100000),
                vix=self._clamp(vix_live, 0, 100),
                vov=self._clamp(vov, 0, 500),
                rv7=self._clamp(rv7, 0, 200),
                rv28=self._clamp(rv28, 0, 200),
                garch7=self._clamp(ga7, 0, 200),
                garch28=self._clamp(ga28, 0, 200),
                pk7=self._clamp(pk7, 0, 200),
                pk28=self._clamp(pk28, 0, 200),
                ivp30=self._clamp(ivp30, 0, 100),
                ivp90=self._clamp(ivp90, 0, 100),
                ivp1y=self._clamp(ivp1y, 0, 100),
                is_fallback=is_fallback
            )
            
        except Exception as e:
            logger.error(f"Volatility calculation failed: {e}", exc_info=True)
            return self._get_default_metrics(spot_live, vix_live)

    def _calculate_realized_vol(self, returns: pd.Series, window: int, context: str) -> float:
        """Calculate realized volatility with safety checks"""
        try:
            if len(returns) < window:
                logger.warning(f"Insufficient data for {context}: need {window}, have {len(returns)}")
                return 0.0
            
            window_returns = returns.tail(window)
            std_dev = window_returns.std()
            
            if not math.isfinite(std_dev) or std_dev < 0:
                logger.warning(f"Invalid std dev for {context}: {std_dev}")
                return 0.0
            
            # Annualize: std * sqrt(252) * 100
            annualized = std_dev * self._safe_sqrt(252, f"{context}_annualize") * 100
            
            if not math.isfinite(annualized):
                logger.warning(f"Non-finite annualized vol for {context}")
                return 0.0
            
            return float(annualized)
            
        except Exception as e:
            logger.error(f"Realized vol calculation failed for {context}: {e}")
            return 0.0

    def _calculate_parkinson_vol(self, data: pd.DataFrame) -> Tuple[float, float]:
        """Calculate Parkinson volatility with comprehensive safety"""
        try:
            high = data["high"].replace([0, np.inf, -np.inf], np.nan).dropna()
            low = data["low"].replace([0, np.inf, -np.inf], np.nan).dropna()
            
            # Align indices
            common_idx = high.index.intersection(low.index)
            high, low = high.loc[common_idx], low.loc[common_idx]
            
            if len(high) < 7:
                logger.warning(f"Insufficient H/L data for Parkinson: {len(high)}")
                return 0.0, 0.0
            
            # Calculate log(high/low)^2 safely
            hl_ratio_sq = pd.Series(index=common_idx, dtype=float)
            for idx in common_idx:
                h_val = high.loc[idx]
                l_val = low.loc[idx]
                
                if l_val <= 0:
                    hl_ratio_sq.loc[idx] = np.nan
                    continue
                
                ratio = self._safe_divide(h_val, l_val, 1.0, f"hl_ratio[{idx}]")
                log_ratio = self._safe_log(ratio, f"hl_log[{idx}]")
                
                if math.isfinite(log_ratio):
                    hl_ratio_sq.loc[idx] = log_ratio ** 2
                else:
                    hl_ratio_sq.loc[idx] = np.nan
            
            hl_ratio_sq = hl_ratio_sq.dropna()
            
            if len(hl_ratio_sq) < 7:
                logger.warning("Insufficient valid H/L ratios for Parkinson")
                return 0.0, 0.0
            
            # Parkinson constant: 1 / (4 * ln(2))
            const_factor = self._safe_divide(1.0, 4.0 * np.log(2.0), 0.0, "parkinson_const")
            
            # Calculate 7-day and 28-day
            mean7 = hl_ratio_sq.tail(7).mean()
            mean28 = hl_ratio_sq.tail(28).mean() if len(hl_ratio_sq) >= 28 else mean7
            
            if not math.isfinite(mean7):
                mean7 = 0.0
            if not math.isfinite(mean28):
                mean28 = 0.0
            
            pk7 = self._safe_sqrt(const_factor * mean7, "pk7") * self._safe_sqrt(252, "pk7_ann") * 100
            pk28 = self._safe_sqrt(const_factor * mean28, "pk28") * self._safe_sqrt(252, "pk28_ann") * 100
            
            return float(pk7), float(pk28)
            
        except Exception as e:
            logger.error(f"Parkinson calculation failed: {e}")
            return 0.0, 0.0

    def _calculate_ivp(self, returns: pd.Series, window: int) -> float:
        """Calculate IV percentile safely"""
        try:
            if len(returns) < window + 10:  # Need extra for meaningful percentile
                logger.warning(f"Insufficient data for IVP{window}")
                return 0.0
            
            # Calculate rolling volatility
            rolling_vol = returns.rolling(window).std() * self._safe_sqrt(252, f"ivp{window}") * 100
            rolling_vol = rolling_vol.replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(rolling_vol) < 2:
                logger.warning(f"Insufficient rolling vol data for IVP{window}")
                return 0.0
            
            return self._safe_percentile_rank(rolling_vol, f"IVP{window}")
            
        except Exception as e:
            logger.error(f"IVP{window} calculation failed: {e}")
            return 0.0

    def _merge_data(self, daily: pd.DataFrame, intraday: pd.DataFrame, current_spot: float) -> pd.DataFrame:
        """
        Intelligently appends the current 'live' day to the historical dataset.
        Enhanced with validation.
        """
        try:
            if daily.empty:
                return pd.DataFrame()

            df = daily.copy()
            
            # Validate current_spot
            if not math.isfinite(current_spot) or current_spot <= 0:
                logger.warning(f"Invalid current_spot for merge: {current_spot}")
                return df
            
            # If we have intraday data, aggregate it into a 'Today' candle
            if not intraday.empty:
                intra_high = intraday["high"].max()
                intra_low = intraday["low"].min()
                
                # Validate intraday values
                if not math.isfinite(intra_high):
                    intra_high = current_spot
                if not math.isfinite(intra_low):
                    intra_low = current_spot
                
                today_candle = {
                    "timestamp": intraday.iloc[-1]["timestamp"].normalize(),
                    "open": intraday.iloc[0]["open"] if math.isfinite(intraday.iloc[0]["open"]) else current_spot,
                    "high": max(intra_high, current_spot),
                    "low": min(intra_low, current_spot),
                    "close": current_spot,
                    "volume": intraday["volume"].sum(),
                    "oi": intraday["oi"].iloc[-1] if math.isfinite(intraday["oi"].iloc[-1]) else 0
                }
            else:
                # Minimal fallback if no intraday data
                last_date = df.iloc[-1]["timestamp"]
                if last_date.date() < pd.Timestamp.now().date():
                    today_candle = {
                        "timestamp": pd.Timestamp.now().normalize(),
                        "open": current_spot,
                        "high": current_spot,
                        "low": current_spot,
                        "close": current_spot,
                        "volume": 0,
                        "oi": 0
                    }
                else:
                    return df

            # Append or Update
            if df.iloc[-1]["timestamp"].date() == pd.Timestamp.now().date():
                # Update the last row
                idx = df.index[-1]
                df.loc[idx, "high"] = max(df.loc[idx, "high"], today_candle["high"])
                df.loc[idx, "low"] = min(df.loc[idx, "low"], today_candle["low"])
                df.loc[idx, "close"] = today_candle["close"]
            else:
                # Append new row
                new_row = pd.DataFrame([today_candle])
                df = pd.concat([df, new_row], ignore_index=True)
                
            return df
            
        except Exception as e:
            logger.error(f"Data merge failed: {e}")
            return daily.copy() if not daily.empty else pd.DataFrame()

    def _run_garch_models(self, returns: pd.Series) -> Tuple[float, float]:
        """
        Runs GARCH(1,1) safely with proper scaling and comprehensive validation.
        Returns (Forecast_7_Day, Forecast_28_Day) annualized vol in percentage.
        """
        try:
            # Ensure returns are clean
            clean_returns = returns.replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(clean_returns) < 50:
                logger.warning(f"Insufficient data for GARCH: {len(clean_returns)} < 50")
                return np.nan, np.nan
            
            # Additional validation - check for constant returns
            if clean_returns.std() == 0:
                logger.warning("Returns have zero variance, cannot fit GARCH")
                return np.nan, np.nan
            
            # Fit GARCH(1,1) model with rescaling enabled
            model = arch_model(
                clean_returns, 
                vol="Garch",
                p=1,
                q=1,
                dist="normal",
                rescale=True
            )
            
            res = model.fit(disp="off", show_warning=False)
            
            # Capture the scale factor ARCH used
            scale = res.scale
            
            if not math.isfinite(scale) or scale <= 0:
                logger.warning(f"Invalid GARCH scale factor: {scale}")
                return np.nan, np.nan
            
            # Forecast variance
            forecast = res.forecast(horizon=28, reindex=False)
            variance = forecast.variance.iloc[-1]
            
            if variance is None or len(variance) < 28:
                logger.warning("GARCH forecast failed to produce sufficient horizons")
                return np.nan, np.nan
            
            # Calculate Daily Volatility (still scaled)
            mean7 = variance.iloc[:7].mean()
            mean28 = variance.iloc[:28].mean()
            
            if not math.isfinite(mean7) or not math.isfinite(mean28):
                logger.warning(f"Non-finite GARCH variance means: {mean7}, {mean28}")
                return np.nan, np.nan
            
            if mean7 < 0 or mean28 < 0:
                logger.warning(f"Negative GARCH variance: {mean7}, {mean28}")
                return np.nan, np.nan
            
            daily_vol_7 = self._safe_sqrt(mean7, "garch7_daily")
            daily_vol_28 = self._safe_sqrt(mean28, "garch28_daily")
            
            # DE-SCALE to get back to original units
            if scale > 1.0:
                daily_vol_7 = self._safe_divide(daily_vol_7, scale, 0.0, "garch7_descale")
                daily_vol_28 = self._safe_divide(daily_vol_28, scale, 0.0, "garch28_descale")
            
            # Annualize: Daily Vol * sqrt(252) * 100
            garch7 = daily_vol_7 * self._safe_sqrt(252, "garch7_ann") * 100
            garch28 = daily_vol_28 * self._safe_sqrt(252, "garch28_ann") * 100
            
            # Sanity check results
            if not (1 < garch7 < 200) or not (1 < garch28 < 200):
                logger.warning(f"GARCH produced unrealistic values: garch7={garch7:.2f}, garch28={garch28:.2f}")
                return np.nan, np.nan
            
            return float(garch7), float(garch28)
            
        except Exception as e:
            logger.warning(f"GARCH calculation failed: {e}")
            return np.nan, np.nan

    def _clamp(self, val: float, min_v: float = 0.0, max_v: float = 200.0) -> float:
        """Enhanced clamping with validation"""
        if not isinstance(val, (int, float)):
            return min_v
        if math.isnan(val) or math.isinf(val):
            return min_v
        return max(min_v, min(val, max_v))

    def _get_default_metrics(self, spot, vix) -> VolMetrics:
        """Safe defaults if data is missing"""
        return VolMetrics(
            spot=self._clamp(spot, 0, 100000) if math.isfinite(spot) else 0,
            vix=self._clamp(vix, 0, 100) if math.isfinite(vix) else 0,
            vov=0, rv7=0, rv28=0, 
            garch7=0, garch28=0, pk7=0, pk28=0, 
            ivp30=0, ivp90=0, ivp1y=0, is_fallback=True
                )
