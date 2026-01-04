# app/core/analytics/volatility.py

import numpy as np
import pandas as pd
import logging
import time
import asyncio
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
    """

    def __init__(self, garch_interval_seconds: int = 1800):
        # Configuration
        self.garch_interval = garch_interval_seconds
        
        # State Cache for Heavy Calculations
        self._last_garch_time = 0
        self._cached_garch7 = np.nan
        self._cached_garch28 = np.nan

    async def calculate_volatility(
        self,
        daily_data: pd.DataFrame,
        intraday_data: pd.DataFrame,
        spot_live: float,
        vix_live: float
    ) -> VolMetrics:
        """
        Main computation entry point.
        """
        # 1. Fallback Checks
        # If live spot/vix are missing, try to use the last close from history
        if spot_live <= 0 and not daily_data.empty:
            spot_live = daily_data.iloc[-1]["close"]
        
        if vix_live <= 0 and not daily_data.empty:
            # Look for a VIX column if it was passed, otherwise default logic
            # Assuming daily_data passed here is NIFTY. 
            # If VIX history is needed for IVP, it should be passed separately or handled upstream.
            # For this engine, we focus on Realized Vol of the Index.
            pass 

        is_fallback = (spot_live <= 0)

        # 2. Merge Data (The Hybrid Logic)
        full_series = self._merge_data(daily_data, intraday_data, spot_live)
        
        if len(full_series) < 30:
            logger.warning("Insufficient data for volatility calculation. Returning defaults.")
            return self._get_default_metrics(spot_live, vix_live)

        # 3. Calculate Returns (Log Returns)
        # Handle zeros safely
        close_series = full_series["close"].replace(0, np.nan).dropna()
        returns = np.log(close_series / close_series.shift(1)).dropna()

        # 4. LIGHT Calculations (Run every cycle)
        # ----------------------------------------
        
        # A. Realized Volatility (Standard Deviation)
        rv7 = returns.tail(7).std() * np.sqrt(252) * 100
        rv28 = returns.tail(28).std() * np.sqrt(252) * 100

        # B. Parkinson Volatility (Uses High/Low - Critical for Intraday detection)
        # Formula: sqrt(1 / (4 * ln(2)) * mean(ln(high/low)^2))
        high = full_series["high"].replace(0, np.nan).dropna()
        low = full_series["low"].replace(0, np.nan).dropna()
        
        # Align indices
        common_idx = high.index.intersection(low.index)
        high, low = high.loc[common_idx], low.loc[common_idx]
        
        hl_ratio_sq = (np.log(high / low) ** 2)
        const_factor = 1.0 / (4.0 * np.log(2.0))
        
        pk7 = np.sqrt(const_factor * hl_ratio_sq.tail(7).mean()) * np.sqrt(252) * 100
        pk28 = np.sqrt(const_factor * hl_ratio_sq.tail(28).mean()) * np.sqrt(252) * 100

        # C. Vol of Vol (Based on VIX moves if available, or RV variance)
        # For now, we use a placeholder or derived metric
        vov = 0.0 # Requires VIX history series passed explicitly, simplified here

        # 5. HEAVY Calculations (GARCH - Run conditionally)
        # -------------------------------------------------
        if time.time() - self._last_garch_time > self.garch_interval:
            # Offload to thread to avoid blocking loop
            ga7, ga28 = await asyncio.to_thread(self._run_garch_models, returns)
            
            # Update Cache
            self._cached_garch7 = ga7
            self._cached_garch28 = ga28
            self._last_garch_time = time.time()
            # logger.info(f"Recalculated GARCH: {ga7:.2f} / {ga28:.2f}")
        else:
            # Use Cache
            ga7 = self._cached_garch7
            ga28 = self._cached_garch28

        # If GARCH failed or is NaN, fallback to RV
        if np.isnan(ga7): ga7 = rv7
        if np.isnan(ga28): ga28 = rv28

        # 6. IV Percentile (Rank)
        # We need VIX history for this. If not passed, we can't calc accurately.
        # Assuming upstream handles IVP or we calculate "Realized Vol Percentile" here.
        # Let's calc RV Percentile as a proxy for internal regime
        ivp30 = (returns.rolling(30).std() * np.sqrt(252) * 100).rank(pct=True).iloc[-1] * 100
        ivp90 = (returns.rolling(90).std() * np.sqrt(252) * 100).rank(pct=True).iloc[-1] * 100
        ivp1y = (returns.rolling(252).std() * np.sqrt(252) * 100).rank(pct=True).iloc[-1] * 100

        # 7. Clamp & Return
        return VolMetrics(
            spot=spot_live,
            vix=vix_live,
            vov=self._clamp(vov),
            rv7=self._clamp(rv7),
            rv28=self._clamp(rv28),
            garch7=self._clamp(ga7),
            garch28=self._clamp(ga28),
            pk7=self._clamp(pk7),
            pk28=self._clamp(pk28),
            ivp30=self._clamp(ivp30, 0, 100),
            ivp90=self._clamp(ivp90, 0, 100),
            ivp1y=self._clamp(ivp1y, 0, 100),
            is_fallback=is_fallback
        )

    def _merge_data(self, daily: pd.DataFrame, intraday: pd.DataFrame, current_spot: float) -> pd.DataFrame:
        """
        Intelligently appends the current 'live' day to the historical dataset.
        """
        if daily.empty:
            return pd.DataFrame()

        # Create a copy to avoid mutating source
        df = daily.copy()
        
        # If we have intraday data, aggregate it into a 'Today' candle
        if not intraday.empty:
            today_candle = {
                "timestamp": intraday.iloc[-1]["timestamp"].normalize(), # Midnight timestamp
                "open": intraday.iloc[0]["open"],
                "high": max(intraday["high"].max(), current_spot),
                "low": min(intraday["low"].min(), current_spot),
                "close": current_spot, # Live price
                "volume": intraday["volume"].sum(),
                "oi": intraday["oi"].iloc[-1]
            }
        else:
            # Minimal fallback if no intraday data (just use spot)
            last_date = df.iloc[-1]["timestamp"]
            # If last date is not today, append a dummy candle
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
                return df # Data is already up to date? (Unlikely for live trading)

        # Append
        # Check if the last row in Daily is actually Today (some APIs update EOD data real-time)
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

    def _run_garch_models(self, returns: pd.Series) -> Tuple[float, float]:
        """
        Runs GARCH(1,1) safely. 
        Returns (Forecast_7_Day, Forecast_28_Day) annualized vol.
        """
        try:
            # Scale returns for numerical stability (GARCH prefers pct * 100)
            scaled_returns = returns * 100
            
            # Basic GARCH(1,1)
            model = arch_model(scaled_returns, vol="Garch", p=1, q=1, dist="normal")
            res = model.fit(disp="off", show_warning=False)
            
            # Forecast
            forecast = res.forecast(horizon=28, reindex=False)
            variance = forecast.variance.iloc[-1] # Series of variances
            
            # Convert to Annualized Vol
            # Variance is for daily returns. 
            # Vol = sqrt(variance) * sqrt(252) / 100 (rescale back)
            
            # 7-day forecast (approx by taking first 7 days avg var)
            var7 = variance.iloc[:7].mean()
            garch7 = np.sqrt(var7) * np.sqrt(252) # Already scaled *100 implicitly by input
            
            # 28-day forecast
            var28 = variance.iloc[:28].mean()
            garch28 = np.sqrt(var28) * np.sqrt(252)
            
            return float(garch7), float(garch28)
            
        except Exception as e:
            logger.warning(f"GARCH calculation failed: {e}")
            return np.nan, np.nan

    def _clamp(self, val: float, min_v: float = 0.1, max_v: float = 200.0) -> float:
        if np.isnan(val) or np.isinf(val):
            return 0.0
        return max(min_v, min(val, max_v))

    def _get_default_metrics(self, spot, vix) -> VolMetrics:
        """Safe defaults if data is missing"""
        return VolMetrics(
            spot=spot, vix=vix, vov=0, rv7=0, rv28=0, 
            garch7=0, garch28=0, pk7=0, pk28=0, 
            ivp30=0, ivp90=0, ivp1y=0, is_fallback=True
        )
