# app/core/analytics/structure.py

import numpy as np
import pandas as pd
from app.schemas.analytics import StructMetrics
from app.config import settings


class StructureEngine:
    def analyze_structure(self, wc: pd.DataFrame, spot: float, lot: int) -> StructMetrics:

        if wc is None or wc.empty or spot <= 0:
            return StructMetrics(0.0, "NEUTRAL", np.nan, 0.0, lot, 0.0, "NEUTRAL")

        # --------------------------------------------------
        # GEX Calculation (Spot-Centered Window)
        # --------------------------------------------------
        sub = wc[(wc["strike"] > spot * 0.9) & (wc["strike"] < spot * 1.1)]
        if sub.empty:
            net_gex = 0.0
        else:
            net_gex = (
                (sub["ce_gamma"] * sub["ce_oi"])
                - (sub["pe_gamma"] * sub["pe_oi"])
            ).sum() * spot * lot

        gex_th = getattr(settings, "GEX_STICKY_THRESHOLD", 2e8)

        if net_gex > gex_th:
            gex_regime = "STICKY"
        elif net_gex < -gex_th:
            gex_regime = "SLIPPERY"
        else:
            gex_regime = "NEUTRAL"

        # --------------------------------------------------
        # PCR
        # --------------------------------------------------
        ce_oi = wc["ce_oi"].sum()
        pe_oi = wc["pe_oi"].sum()
        pcr = pe_oi / ce_oi if ce_oi > 0 else np.nan

        # --------------------------------------------------
        # Max Pain
        # --------------------------------------------------
        strikes = wc["strike"].values
        losses = []
        for s in strikes:
            call_loss = np.sum(np.maximum(0, s - strikes) * wc["ce_oi"].values)
            put_loss = np.sum(np.maximum(0, strikes - s) * wc["pe_oi"].values)
            losses.append(call_loss + put_loss)

        max_pain = strikes[np.argmin(losses)] if losses else 0.0

        # --------------------------------------------------
        # Skew (25-delta)
        # --------------------------------------------------
        skew = 0.0
        try:
            valid = wc.dropna(subset=["ce_delta", "pe_delta", "ce_iv", "pe_iv"])
            c25 = valid.iloc[(valid["ce_delta"].abs() - 0.25).abs().argsort()[:1]]["ce_iv"].values[0]
            p25 = valid.iloc[(valid["pe_delta"].abs() - 0.25).abs().argsort()[:1]]["pe_iv"].values[0]
            skew = p25 - c25
        except Exception:
            skew = 0.0

        # --------------------------------------------------
        # Structural Bias
        # --------------------------------------------------
        if pcr > 1.2:
            sreg = "BULLISH"
        elif pcr < 0.7:
            sreg = "BEARISH"
        else:
            sreg = "NEUTRAL"

        return StructMetrics(net_gex, gex_regime, pcr, max_pain, lot, skew, sreg)
