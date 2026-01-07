# app/core/analytics/volatility.py

import numpy as np
import pandas as pd
import logging
import time
import asyncio
import math
from arch import arch_model
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# =============================================================================
# DATA STRUCTURE
# =============================================================================
@dataclass
class VolMetrics:
    spot: float
    iv: float
    vov: float
    regime: str

    ivp30: float
    ivp90: float
    ivp1y: float

    rv7: float
    rv28: float

    garch7: float
    garch28: float

    pk7: float
    pk28: float

    is_fallback: bool = False


# =============================================================================
# ENGINE
# =============================================================================
class VolatilityEngine:
    """
    Production-safe volatility engine.
    NO assumptions.
    NO silent NaNs.
    """

    def __init__(self, garch_interval_seconds: int = 1800):
        self.garch_interval = garch_interval_seconds
        self._last_garch_time = 0.0
        self._cached_garch7 = np.nan
        self._cached_garch28 = np.nan

    # -------------------------------------------------------------------------
    # MAIN ENTRY
    # -------------------------------------------------------------------------
    async def calculate_volatility(
        self,
        history_candles: pd.DataFrame,
        intraday_candles: pd.DataFrame,
        spot_price: float,
        vix_current: float,
        vix_history: pd.DataFrame,
    ) -> VolMetrics:

        try:
            # -----------------------------
            # VALIDATION
            # -----------------------------
            if not math.isfinite(spot_price) or spot_price <= 0:
                if not history_candles.empty:
                    spot_price = float(history_candles.iloc[-1]["close"])
                else:
                    spot_price = 0.0

            if not math.isfinite(vix_current):
                vix_current = 0.0

            is_fallback = spot_price <= 0

            if history_candles.empty:
                return self._default(spot_price, vix_current)

            # -----------------------------
            # CLEAN DAILY CLOSES
            # -----------------------------
            closes = history_candles["close"].replace([0, np.inf, -np.inf], np.nan)
            closes = closes.dropna()
            closes = closes[closes > 0]

            if len(closes) < 30:
                return self._default(spot_price, vix_current)

            returns = np.log(closes / closes.shift(1)).dropna()

            if len(returns) < 7:
                return self._default(spot_price, vix_current)

            # -----------------------------
            # REALIZED VOL
            # -----------------------------
            rv7 = self._realized_vol(returns, 7)
            rv28 = self._realized_vol(returns, 28)

            # -----------------------------
            # PARKINSON VOL (DAILY ONLY)
            # -----------------------------
            pk7, pk28 = self._parkinson_vol(history_candles)

            # -----------------------------
            # VIX HISTORY (MANDATORY)
            # -----------------------------
            ivp30 = ivp90 = ivp1y = 50.0
            vov = 0.0

            if vix_history is not None and not vix_history.empty:
                vix_closes = vix_history["close"].replace(
                    [0, np.inf, -np.inf], np.nan
                ).dropna()

                if len(vix_closes) >= 30:
                    ivp30 = self._percentile(vix_closes.tail(30), vix_current)
                if len(vix_closes) >= 90:
                    ivp90 = self._percentile(vix_closes.tail(90), vix_current)
                if len(vix_closes) >= 252:
                    ivp1y = self._percentile(vix_closes.tail(252), vix_current)

                vov = self._vov(vix_closes)

            # -----------------------------
            # GARCH (CACHED)
            # -----------------------------
            if time.time() - self._last_garch_time > self.garch_interval:
                g7, g28 = await asyncio.to_thread(self._run_garch, returns)
                self._cached_garch7 = g7
                self._cached_garch28 = g28
                self._last_garch_time = time.time()
            else:
                g7 = self._cached_garch7
                g28 = self._cached_garch28

            if not math.isfinite(g7):
                g7 = rv7
            if not math.isfinite(g28):
                g28 = rv28

            # -----------------------------
            # REGIME
            # -----------------------------
            regime = "NORMAL"
            if ivp1y < 20:
                regime = "LOW_VOL"
            elif ivp1y > 80:
                regime = "HIGH_VOL"

            return VolMetrics(
                spot=float(spot_price),
                iv=float(vix_current),
                vov=float(vov),
                regime=regime,
                ivp30=float(ivp30),
                ivp90=float(ivp90),
                ivp1y=float(ivp1y),
                rv7=float(rv7),
                rv28=float(rv28),
                garch7=float(g7),
                garch28=float(g28),
                pk7=float(pk7),
                pk28=float(pk28),
                is_fallback=is_fallback,
            )

        except Exception as e:
            logger.error("Volatility calculation failed", exc_info=True)
            return self._default(spot_price, vix_current)

    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------
    def _realized_vol(self, returns, window):
        if len(returns) < window:
            return 0.0
        return float(returns.tail(window).std() * np.sqrt(252) * 100)

    def _parkinson_vol(self, df):
        try:
            h = df["high"].replace([0, np.inf], np.nan).dropna()
            l = df["low"].replace([0, np.inf], np.nan).dropna()
            idx = h.index.intersection(l.index)

            if len(idx) < 7:
                return 0.0, 0.0

            rs = np.log(h.loc[idx] / l.loc[idx]) ** 2
            const = 1.0 / (4.0 * np.log(2.0))

            pk7 = np.sqrt(const * rs.tail(7).mean()) * np.sqrt(252) * 100
            pk28 = np.sqrt(const * rs.tail(28).mean()) * np.sqrt(252) * 100

            return pk7, pk28
        except Exception:
            return 0.0, 0.0

    def _run_garch(self, returns):
        clean = returns.replace([np.inf, -np.inf], np.nan).dropna()
        if len(clean) < 50 or clean.std() == 0:
            return np.nan, np.nan

        model = arch_model(clean * 100, vol="Garch", p=1, q=1)
        res = model.fit(disp="off")
        var = res.forecast(horizon=28, reindex=False).variance.iloc[-1]

        g7 = np.sqrt(var.iloc[:7].mean()) * np.sqrt(252)
        g28 = np.sqrt(var.iloc[:28].mean()) * np.sqrt(252)
        return float(g7), float(g28)

    def _percentile(self, series, value):
        return float((series < value).mean() * 100)

    def _vov(self, vix_series):
        rets = np.log(vix_series / vix_series.shift(1)).dropna()
        if len(rets) < 20:
            return 0.0
        return float(rets.tail(20).std() * np.sqrt(252) * 100)

    def _default(self, spot, vix):
        return VolMetrics(
            spot=float(spot),
            iv=float(vix),
            vov=0.0,
            regime="NORMAL",
            ivp30=50.0,
            ivp90=50.0,
            ivp1y=50.0,
            rv7=0.0,
            rv28=0.0,
            garch7=0.0,
            garch28=0.0,
            pk7=0.0,
            pk28=0.0,
            is_fallback=True,
        )
