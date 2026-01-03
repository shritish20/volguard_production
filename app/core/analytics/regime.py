from app.schemas.analytics import RegimeResult, VolMetrics, StructMetrics, EdgeMetrics, ExtMetrics
from app.config import settings

# [span_23](start_span)Derived from[span_23](end_span)

class RegimeEngine:
    
    def calculate_regime(self, vol: VolMetrics, st: StructMetrics, ed: EdgeMetrics, ex: ExtMetrics) -> RegimeResult:
        """Synthesize metrics into a trading decision and score"""
        # 1. [span_24](start_span)Volatility Score[span_24](end_span)
        vs = 5.0
        if vol.ivp1y < 20: vs = 2.0 
        elif vol.ivp1y > 80: vs = 8.0
        
        # 2. [span_25](start_span)Structure Score[span_25](end_span)
        ss = 5.0 + (2 if st.gex_regime == "STICKY" else -2 if st.gex_regime == "SLIPPERY" else 0)
        
        # 3. Edge Score
        es = 5.0 + (2 if ed.vrp_pk_w > 3 else 0)
        
        # 4. [span_26](start_span)Risk Score[span_26](end_span)
        rs = 10.0 - (4 if ex.fast_vol else 0) - (2 if ex.events > 0 else 0)
        
        # Composite Score
        comp = (vs*0.4) + (ss*0.3) + (es*0.2) + (rs*0.1)
        
        # [span_27](start_span)Decision Logic[span_27](end_span)
        name = "NEUTRAL"
        alloc = 0.0
        lots = 0
        
        if comp >= 7.0: 
            name = "AGGRESSIVE_SHORT"
            alloc = 60.0
            lots = int((settings.BASE_CAPITAL * 0.60) / settings.MARGIN_SELL)
        elif comp <= 4.0: 
            name = "LONG_VOL / DEFENSIVE"
            alloc = 20.0
            lots = int((settings.BASE_CAPITAL * 0.20) / settings.MARGIN_BUY)
        elif comp < 7.0: 
            name = "MODERATE_SHORT"
            alloc = 40.0
            lots = int((settings.BASE_CAPITAL * 0.40) / settings.MARGIN_SELL)
        
        # [span_28](start_span)Safety Override[span_28](end_span)
        if ex.fast_vol:
            name = "CASH / STAY AWAY"
            alloc = 0.0
            lots = 0
            
        return RegimeResult(name, comp, ed.primary, vs, ss, es, rs, alloc, lots)
