# app/core/analytics/regime.py

from app.schemas.analytics import (
    RegimeResult,
    VolMetrics,
    StructMetrics,
    EdgeMetrics,
    ExtMetrics,
)
from app.config import settings


class RegimeEngine:
    """
    RegimeEngine classifies the market environment.
    It DOES NOT allocate capital or size trades.
    It only defines risk permission envelopes.
    """

    def calculate_regime(
        self,
        vol: VolMetrics,
        st: StructMetrics,
        ed: EdgeMetrics,
        ex: ExtMetrics,
    ) -> RegimeResult:

        # ======================================================
        # 1️⃣ VOLATILITY SCORE
        # ======================================================
        vs = 5.0
        if vol.ivp1y < 20:
            vs = 2.0
        elif vol.ivp1y > 80:
            vs = 8.0

        # ======================================================
        # 2️⃣ STRUCTURE SCORE (GEX / PINNING)
        # ======================================================
        ss = 5.0
        if st.gex_regime == "STICKY":
            ss += 2.0
        elif st.gex_regime == "SLIPPERY":
            ss -= 2.0

        # ======================================================
        # 3️⃣ EDGE SCORE (VRP)
        # ======================================================
        es = 5.0
        if ed.vrp_pk_w > 3:
            es += 2.0

        # ======================================================
        # 4️⃣ RISK / EVENT SCORE
        # ======================================================
        rs = 10.0
        if ex.fast_vol:
            rs -= 4.0
        if ex.events > 0:
            rs -= 2.0

        # ======================================================
        # 5️⃣ COMPOSITE SCORE
        # ======================================================
        comp = (vs * 0.4) + (ss * 0.3) + (es * 0.2) + (rs * 0.1)

        # ======================================================
        # 6️⃣ REGIME CLASSIFICATION
        # ======================================================
        # Default = CASH (no trade)
        name = "CASH"
        alloc_pct = 0.0
        max_lots = 0

        # Thresholds (configurable)
        aggressive_th = 7.0
        defensive_th = 4.0

        if comp >= aggressive_th:
            name = "AGGRESSIVE_SHORT"
            alloc_pct = 0.60
        elif comp <= defensive_th:
            name = "LONG_VOL"
            alloc_pct = 0.20
        else:
            name = "MODERATE_SHORT"
            alloc_pct = 0.40

        # ======================================================
        # 7️⃣ SAFETY OVERRIDES
        # ======================================================
        if ex.fast_vol:
            name = "CASH"
            alloc_pct = 0.0

        # ======================================================
        # 8️⃣ MAX LOT CEILING (FROM CONFIG)
        # ======================================================
        max_lots = settings.REGIME_MAX_LOTS.get(name, 0)

        return RegimeResult(
            name=name,
            score=comp,
            primary_edge=ed.primary,
            v_scr=vs,
            s_scr=ss,
            e_scr=es,
            r_scr=rs,
            alloc_pct=alloc_pct,
            max_lots=max_lots,
        )
