import pandas as pd
from app.schemas.analytics import EdgeMetrics, VolMetrics

# [span_20](start_span)Derived from[span_20](end_span)

class EdgeEngine:
    
    def detect_edges(self, wc: pd.DataFrame, mc: pd.DataFrame, spot: float, vol: VolMetrics) -> EdgeMetrics:
        """Identify statistical edges (VRP, Term Structure)"""
        def get_iv(c):
            if c.empty or spot == 0: return 0
            idx = (c['strike']-spot).abs().argsort()[:1]
            return c.iloc[idx]['ce_iv'].values[0]
        
        iw, im = get_iv(wc), get_iv(mc)
        term = im - iw  # Term structure
        
        # [span_21](start_span)Calculate VRP (IV - RV/GARCH/Parkinson)[span_21](end_span)
        vrv_w, vrv_m = iw - vol.rv7, im - vol.rv28
        vga_w, vga_m = iw - vol.ga7, im - vol.ga28
        vpk_w, vpk_m = iw - vol.pk7, im - vol.pk28
        
        # [span_22](start_span)Determine Primary Edge[span_22](end_span)
        p = "NONE"
        if vol.ivp1y < 20: p = "LONG_VEGA (Cheap Vol)"
        elif vpk_w > 3.0: p = "SHORT_GAMMA (High VRP)"
        elif term < -1.5: p = "CALENDAR (Backwardation)"
        
        return EdgeMetrics(iw, im, term, vrv_w, vga_w, vpk_w, vrv_m, vga_m, vpk_m, p)
