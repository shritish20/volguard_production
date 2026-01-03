# app/core/analytics/edge.py

import pandas as pd
import numpy as np
from app.schemas.analytics import EdgeMetrics, VolMetrics


class EdgeEngine:
    """
    Detects volatility and term-structure based trading edges.
    """

    def detect_edges(
        self,
        wc: pd.DataFrame,
        mc: pd.DataFrame,
        spot: float,
        vol: VolMetrics
    ) -> EdgeMetrics:

        # --------------------------------------------------
        # ATM IV helper (avg CE & PE)
        # --------------------------------------------------
        def get_atm_iv(chain: pd.DataFrame) -> float:
            if chain is None or chain.empty or spot <= 0:
                return np.nan

            chain = chain.dropna(subset=["strike", "ce_iv", "pe_iv"])
            if chain.empty:
                return np.nan

            idx = (chain["strike"] - spot).abs().idxmin()
            ce_iv = chain.loc[idx, "ce_iv"]
            pe_iv = chain.loc[idx, "pe_iv"]

            if ce_iv <= 0 or pe_iv <= 0:
                return np.nan

            return (ce_iv + pe_iv) / 2.0

        iw = get_atm_iv(wc)
        im = get_atm_iv(mc)

        term = im - iw if np.isfinite(iw) and np.isfinite(im) else np.nan

        # --------------------------------------------------
        # Vol Risk Premia
        # --------------------------------------------------
        vrp_rv_w = iw - vol.rv7 if np.isfinite(iw) else np.nan
        vrp_rv_m = im - vol.rv28 if np.isfinite(im) else np.nan

        vga_w = iw - vol.garch7 if np.isfinite(iw) else np.nan
        vga_m = im - vol.garch28 if np.isfinite(im) else np.nan

        vpk_w = iw - vol.pk7 if np.isfinite(iw) else np.nan
        vpk_m = im - vol.pk28 if np.isfinite(im) else np.nan

        # --------------------------------------------------
        # Primary Edge Classification (Machine-Friendly)
        # --------------------------------------------------
        primary = "NONE"

        if vol.ivp1y < 20:
            primary = "LONG_VOL"
        elif np.isfinite(vpk_w) and vpk_w > 3.0:
            primary = "SHORT_VOL"
        elif np.isfinite(term) and term < -1.5:
            primary = "CALENDAR"

        return EdgeMetrics(
            iv_weekly=iw,
            iv_monthly=im,
            term_structure=term,
            vrp_rv_w=vrp_rv_w,
            vrp_rv_m=vrp_rv_m,
            vrp_garch_w=vga_w,
            vrp_garch_m=vga_m,
            vrp_pk_w=vpk_w,
            vrp_pk_m=vpk_m,
            primary=primary
        )
