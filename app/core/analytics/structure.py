import numpy as np
import pandas as pd
from app.schemas.analytics import StructMetrics

# [span_16](start_span)Derived from[span_16](end_span)

class StructureEngine:
    
    def analyze_structure(self, wc: pd.DataFrame, spot: float, lot: int) -> StructMetrics:
        """Analyze market structure including GEX and Pain"""
        if wc.empty or spot == 0: 
            return StructMetrics(0, "NEUTRAL", 0, 0, lot, 0, "NEUTRAL")
        
        # [span_17](start_span)GEX Calculation[span_17](end_span)
        # Filter near-the-money strikes (10% range)
        sub = wc[(wc['strike'] > spot*0.9) & (wc['strike'] < spot*1.1)]
        
        # Net GEX = (Call Gamma * OI) - (Put Gamma * OI) * Spot * Lot
        net_gex = ((sub['ce_gamma']*sub['ce_oi']) - (sub['pe_gamma']*sub['pe_oi'])).sum() * spot * lot
        greg = "STICKY" if net_gex > 2e8 else "SLIPPERY" if net_gex < -2e8 else "NEUTRAL"
        
        # PCR
        pcr = wc['pe_oi'].sum()/wc['ce_oi'].sum() if wc['ce_oi'].sum() else 0
        
        # [span_18](start_span)Max Pain[span_18](end_span)
        strikes = wc['strike'].values
        losses = []
        for s in strikes:
            cl = np.sum(np.maximum(0, s - strikes) * wc['ce_oi'].values)
            pl = np.sum(np.maximum(0, strikes - s) * wc['pe_oi'].values)
            losses.append(cl + pl)
        max_pain = strikes[np.argmin(losses)] if losses else 0
        
        # [span_19](start_span)Skew (25 Delta Put IV - 25 Delta Call IV)[span_19](end_span)
        try:
            c25 = wc.iloc[(wc['ce_delta'].abs()-0.25).abs().argsort()[:1]]['ce_iv'].values[0]
            p25 = wc.iloc[(wc['pe_delta'].abs()-0.25).abs().argsort()[:1]]['pe_iv'].values[0]
            skew = p25 - c25
        except: skew = 0
        
        sreg = "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.7 else "NEUTRAL"
        return StructMetrics(net_gex, greg, pcr, max_pain, lot, skew, sreg)
