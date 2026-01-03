import pandas as pd
from app.schemas.analytics import EdgeMetrics, VolMetrics

class EdgeEngine:
    def detect_edges(self, wc: pd.DataFrame, mc: pd.DataFrame, spot: float, vol: VolMetrics) -> EdgeMetrics:
        # [Source 590]
        def get_iv(c):
            if c.empty or spot == 0: return 0
            idx = (c['strike']-spot).abs().argsort()[:1]
            return c.iloc[idx]['ce_iv'].values[0]
        
        iw, im = get_iv(wc), get_iv(mc)
        term = im - iw
        
        # [Source 591] Calculate VRPs
        vrp_rv_w, vrp_rv_m = iw - vol.rv7, im - vol.rv28
        vga_w, vga_m = iw - vol.ga7, im - vol.ga28
        vpk_w, vpk_m = iw - vol.pk7, im - vol.pk28
        
        # CRITICAL ALIGNMENT: Primary Edge Logic [Source 591]
        p = "NONE"
        if vol.ivp1y < 20: p = "LONG_VEGA (Cheap Vol)"
        elif vpk_w > 3.0: p = "SHORT_GAMMA (High VRP)" # Uses Parkinson, not RV
        elif term < -1.5: p = "CALENDAR (Backwardation)"
        
        return EdgeMetrics(iw, im, term, vrp_rv_w, vga_w, vpk_w, vrv_m, vga_m, vpk_m, p)
