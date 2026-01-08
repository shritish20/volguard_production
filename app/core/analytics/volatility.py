import numpy as np
import pandas as pd
from arch import arch_model
from scipy import stats
import asyncio
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

# We use the internal logger to keep production monitoring happy
from app.utils.logger import logger 

@dataclass
class VolMetrics:
    spot: float
    vix: float
    # Realized Volatility
    rv7: float
    rv28: float
    rv90: float
    # Forecast & Intraday
    garch7: float
    garch28: float
    park7: float
    park28: float
    # The "Holy Grail" Filters
    vov: float
    vov_zscore: float  # <--- The Kill Switch Metric
    # Context
    ivp_30d: float
    ivp_90d: float
    ivp_1yr: float
    trend_strength: float
    # CRITICAL ADDITIONS FOR TRADING ENGINE
    atr14: float          # <--- Used for strike width
    ma20: float           # <--- Used for trend bias
    vol_regime: str
    is_fallback: bool

class VolatilityEngine:
    """
    VolGuard 4.1 Volatility Engine.
    Powered by v30.1 Logic: VoV Z-Score & Weighted VRP Inputs.
    """

    def __init__(self):
        self.risk_free_rate = 0.07  # India 10Y ~7%

    async def analyze(self, 
                      nifty_hist: pd.DataFrame, 
                      vix_hist: pd.DataFrame, 
                      spot_live: float, 
                      vix_live: float) -> VolMetrics:
        """
        Main entry point. Runs heavy math in a thread to prevent blocking the event loop.
        """
        try:
            # Offload the heavy math to a separate thread
            return await asyncio.to_thread(
                self._compute_sync, 
                nifty_hist, 
                vix_hist, 
                spot_live, 
                vix_live
            )
        except Exception as e:
            logger.error(f"Volatility Analysis Failed: {str(e)}")
            # Return a "Safety" Fallback object if math fails
            return self._get_fallback_metrics(spot_live, vix_live)

    def _compute_sync(self, df_spot: pd.DataFrame, df_vix: pd.DataFrame, spot_now: float, vix_now: float) -> VolMetrics:
        """
        Synchronous core logic - identical to v30.1 script but structured for production.
        """
        # 1. Data Prep & Fallback Handling
        is_fallback = False
        if spot_now <= 0 or vix_now <= 0:
            spot_now = df_spot.iloc[-1]['close'] if not df_spot.empty else 0
            vix_now = df_vix.iloc[-1]['close'] if not df_vix.empty else 0
            is_fallback = True

        # Log Returns
        df_spot['returns'] = np.log(df_spot['close'] / df_spot['close'].shift(1))
        returns = df_spot['returns'].dropna()
        
        # 2. Realized Volatility (RV) - 7, 28, 90 Days
        # Annualized: std * sqrt(252) * 100
        rv7 = returns.rolling(7).std().iloc[-1] * np.sqrt(252) * 100
        rv28 = returns.rolling(28).std().iloc[-1] * np.sqrt(252) * 100
        rv90 = returns.rolling(90).std().iloc[-1] * np.sqrt(252) * 100

        # 3. Parkinson Volatility (Intraday Range) - Crucial for "Weighted VRP"
        # Formula: 1/(4ln2) * ln(H/L)^2
        const = 1.0 / (4.0 * np.log(2.0))
        high_low_log = np.log(df_spot['high'] / df_spot['low']) ** 2
        
        park7 = np.sqrt(high_low_log.tail(7).mean() * const) * np.sqrt(252) * 100
        park28 = np.sqrt(high_low_log.tail(28).mean() * const) * np.sqrt(252) * 100

        # 4. GARCH(1,1) Forecasting
        # We catch errors here because arch_model can be finicky with small data
        garch7 = self._fit_garch(returns, horizon=7) or rv7
        garch28 = self._fit_garch(returns, horizon=28) or rv28

        # 5. The "Holy Grail": Vol-of-Vol (VoV) Z-Score
        # Logic: 30D Rolling Std of VIX Returns, Normalized by 60D History
        vix_ret = np.log(df_vix['close'] / df_vix['close'].shift(1)).dropna()
        vov_rolling = vix_ret.rolling(30).std() * np.sqrt(252) * 100
        
        vov_current = vov_rolling.iloc[-1] if not vov_rolling.empty else 0
        vov_mean = vov_rolling.rolling(60).mean().iloc[-1] if not vov_rolling.empty else 0
        vov_std = vov_rolling.rolling(60).std().iloc[-1] if not vov_rolling.empty else 1
        
        # Avoid division by zero
        if vov_std > 0:
            vov_zscore = (vov_current - vov_mean) / vov_std
        else:
            vov_zscore = 0.0

        # 6. IV Percentile (Context)
        ivp_30 = self._calc_ivp(df_vix['close'], vix_now, 30)
        ivp_90 = self._calc_ivp(df_vix['close'], vix_now, 90)
        ivp_1yr = self._calc_ivp(df_vix['close'], vix_now, 252)

        # 7. Trend Strength (ATR Based)
        # Using ATR14 to determine if we are trending or ranging
        high_low = df_spot['high'] - df_spot['low']
        high_close = (df_spot['high'] - df_spot['close'].shift(1)).abs()
        low_close = (df_spot['low'] - df_spot['close'].shift(1)).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        atr14 = tr.rolling(14).mean().iloc[-1] if not tr.empty else 0
        ma20 = df_spot['close'].rolling(20).mean().iloc[-1] if not df_spot.empty else 0
        
        trend_strength = abs(spot_now - ma20) / atr14 if atr14 > 0 else 0

        # 8. Preliminary Regime Tag (Detailed scoring happens in RegimeEngine)
        regime_tag = "FAIR"
        if vov_zscore > 2.5: 
            regime_tag = "EXPLODING"  # Kill Switch
        elif ivp_1yr > 75:
            regime_tag = "RICH"
        elif ivp_1yr < 25:
            regime_tag = "CHEAP"

        return VolMetrics(
            spot=spot_now,
            vix=vix_now,
            rv7=rv7, rv28=rv28, rv90=rv90,
            garch7=garch7, garch28=garch28,
            park7=park7, park28=park28,
            vov=vov_current,
            vov_zscore=vov_zscore,
            ivp_30d=ivp_30, ivp_90d=ivp_90, ivp_1yr=ivp_1yr,
            trend_strength=trend_strength,
            atr14=atr14,  # <--- CRITICAL: Passed to object
            ma20=ma20,    # <--- CRITICAL: Passed to object
            vol_regime=regime_tag,
            is_fallback=is_fallback
        )

    def _fit_garch(self, returns: pd.Series, horizon: int) -> float:
        """Helper to fit GARCH(1,1). Returns annualized vol forecast."""
        try:
            # Rescale for optimizer stability
            scaled_returns = returns * 100
            model = arch_model(scaled_returns, vol='Garch', p=1, q=1, dist='normal')
            res = model.fit(disp='off', show_warning=False)
            forecast = res.forecast(horizon=horizon, reindex=False)
            # Extract variance, sqrt to get vol, sqrt(252) to annualize
            return np.sqrt(forecast.variance.values[-1, -1]) * np.sqrt(252)
        except:
            return 0.0

    def _calc_ivp(self, history: pd.Series, current: float, window: int) -> float:
        """Calculate IV Percentile over a lookback window."""
        if len(history) < window: 
            return 0.0
        relevant_hist = history.tail(window)
        return (relevant_hist < current).mean() * 100

    def _get_fallback_metrics(self, spot: float, vix: float) -> VolMetrics:
        """Returns safe default values if analysis crashes."""
        return VolMetrics(
            spot=spot, vix=vix,
            rv7=0, rv28=0, rv90=0,
            garch7=0, garch28=0,
            park7=0, park28=0,
            vov=0, vov_zscore=0,
            ivp_30d=0, ivp_90d=0, ivp_1yr=0,
            trend_strength=0,
            atr14=0, ma20=0, # <--- Added to fallback
            vol_regime="ERROR",
            is_fallback=True
        )
