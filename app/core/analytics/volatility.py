# app/core/analytics/volatility.py

import numpy as np
import asyncio
import pandas as pd
import logging
from arch import arch_model

from app.schemas.analytics import VolMetrics

logger = logging.getLogger(__name__)


class VolatilityEngine:
    """
    Computes volatility metrics in a production-safe manner.
    All failures degrade gracefully with explicit fallbacks.
    """

    # ----------------------------------------------------------
    # INTERNAL: Safe GARCH execution
    # ----------------------------------------------------------
    @staticmethod
    def _run_garch_safe(returns: np.ndarray, horizon: int) -> float:
        """
        Runs GARCH(1,1) safely.
        Returns np.nan on failure (never zero).
        """
        try:
            if returns is None or len(returns) < 120:
                return np.nan

            if np.allclose(np.std(returns), 0):
                return np.nan

            model = arch_model(
                returns * 100,
                vol="Garch",
                p=1,
                q=1,
                dist="normal"
            )

            res = model.fit(disp="off", show_warning=False)

            var = res.forecast(horizon=horizon, reindex=False).variance.values
            garch_vol = np.sqrt(var[-1, -1]) * np.sqrt(252)

            if not np.isfinite(garch_vol) or garch_vol <= 0:
                return np.nan

            return float(garch_vol)

        except Exception as e:
            logger.warning(f"GARCH failed: {e}")
            return np.nan

    # ----------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------
    async def calculate_volatility(
        self,
        nh: pd.DataFrame,
        vh: pd.DataFrame,
        spot_live: float,
        vix_live: float
    ) -> VolMetrics:

        # ------------------------------------------------------
        # 1️⃣ LIVE VALUE FALLBACKS
        # ------------------------------------------------------
        spot = spot_live if spot_live > 0 else nh["close"].iloc[-1]
        vix = vix_live if vix_live > 0 else vh["close"].iloc[-1]
        is_fallback = (spot_live <= 0) or (vix_live <= 0)

        # ------------------------------------------------------
        # 2️⃣ RETURNS
        # ------------------------------------------------------
        close = nh["close"].replace(0, np.nan).dropna()
        ret = np.log(close / close.shift(1)).dropna()

        if len(ret) < 30:
            raise ValueError("Insufficient data for volatility calculation")

        # ------------------------------------------------------
        # 3️⃣ REALIZED VOLATILITY
        # ------------------------------------------------------
        rv7 = ret.rolling(7).std().iloc[-1] * np.sqrt(252) * 100
        rv28 = ret.rolling(28).std().iloc[-1] * np.sqrt(252) * 100

        # ------------------------------------------------------
        # 4️⃣ PARKINSON VOLATILITY
        # ------------------------------------------------------
        high = nh["high"].replace(0, np.nan).dropna()
        low = nh["low"].replace(0, np.nan).dropna()

        const = 1.0 / (4.0 * np.log(2.0))

        pk7 = np.sqrt(
            (np.log(high / low) ** 2).tail(7).mean() * const
        ) * np.sqrt(252) * 100

        pk28 = np.sqrt(
            (np.log(high / low) ** 2).tail(28).mean() * const
        ) * np.sqrt(252) * 100

        # ------------------------------------------------------
        # 5️⃣ GARCH (ASYNC SAFE)
        # ------------------------------------------------------
        ga7, ga28 = await asyncio.gather(
            asyncio.to_thread(self._run_garch_safe, ret.values, 7),
            asyncio.to_thread(self._run_garch_safe, ret.values, 28),
        )

        if np.isnan(ga7):
            ga7 = rv7
        if np.isnan(ga28):
            ga28 = rv28

        # ------------------------------------------------------
        # 6️⃣ VOL OF VOL (VIX RETURNS)
        # ------------------------------------------------------
        vix_ret = np.log(vh["close"].replace(0, np.nan)).diff().dropna()
        vov = vix_ret.tail(30).std() * np.sqrt(252) * 100

        # ------------------------------------------------------
        # 7️⃣ IV PERCENTILES
        # ------------------------------------------------------
        def calc_ivp(window: int) -> float:
            hist = vh["close"].replace(0, np.nan).dropna().tail(window)
            if len(hist) < window:
                return 0.0
            return float((hist < vix).mean() * 100)

        ivp30 = calc_ivp(30)
        ivp90 = calc_ivp(90)
        ivp252 = calc_ivp(252)

        # ------------------------------------------------------
        # 8️⃣ SANITY BOUNDS (CRITICAL)
        # ------------------------------------------------------
        def clamp(x, lo=0.1, hi=200.0):
            return float(min(max(x, lo), hi))

        rv7 = clamp(rv7)
        rv28 = clamp(rv28)
        pk7 = clamp(pk7)
        pk28 = clamp(pk28)
        ga7 = clamp(ga7)
        ga28 = clamp(ga28)
        vov = clamp(vov)

        return VolMetrics(
            spot=spot,
            vix=vix,
            vov=vov,
            rv7=rv7,
            rv28=rv28,
            garch7=ga7,
            garch28=ga28,
            pk7=pk7,
            pk28=pk28,
            ivp30=ivp30,
            ivp90=ivp90,
            ivp1y=ivp252,
            is_fallback=is_fallback,
        )
