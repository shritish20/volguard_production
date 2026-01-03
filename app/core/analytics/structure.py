import numpy as np
import pandas as pd
from app.schemas.analytics import StructMetrics

class StructureEngine:
    def analyze_structure(self, wc: pd.DataFrame, spot: float, lot: int) -> StructMetrics:
        # Exact match to Source 586
        if wc.empty or spot == 0: 
            return StructMetrics(0, "NEUTRAL", 0, 0, lot, 0, "NEUTRAL")
        
        # GEX Calculation [Source 587]
        sub = wc[(wc['strike'] > spot*0.9) & (wc['strike'] < spot*1.1)]
        net_gex = ((sub['ce_gamma']*sub['ce_oi']) - (sub['pe_gamma']*sub['pe_oi'])).sum() * spot * lot
        
        # CRITICAL ALIGNMENT: Threshold 2e8 (not 1e7)
        greg = "STICKY" if net_gex > 2e8 else "SLIPPERY" if net_gex < -2e8 else "NEUTRAL"
        
        pcr = wc['pe_oi'].sum()/wc['ce_oi'].sum() if wc['ce_oi'].sum() else 0
        
        # Max Pain [Source 588]
        strikes = wc['strike'].values
        losses = []
        for s in strikes:
            cl = np.sum(np.maximum(0, s - strikes) * wc['ce_oi'].values)
            pl = np.sum(np.maximum(0, strikes - s) * wc['pe_oi'].values)
            losses.append(cl + pl)
        max_pain = strikes[np.argmin(losses)] if losses else 0
        
        # Skew [Source 589]
        try:
            c25 = wc.iloc[(wc['ce_delta'].abs()-0.25).abs().argsort()[:1]]['ce_iv'].values[0]
            p25 = wc.iloc[(wc['pe_delta'].abs()-0.25).abs().argsort()[:1]]['pe_iv'].values[0]
            skew = p25 - c25
        except: skew = 0
        
        sreg = "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.7 else "NEUTRAL"
        return StructMetrics(net_gex, greg, pcr, max_pain, lot, skew, sreg)
