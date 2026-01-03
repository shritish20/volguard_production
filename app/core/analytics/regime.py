from app.schemas.analytics import RegimeResult, VolMetrics, StructMetrics, EdgeMetrics, ExtMetrics
from app.config import settings

class RegimeEngine:
    def calculate_regime(self, vol: VolMetrics, st: StructMetrics, ed: EdgeMetrics, ex: ExtMetrics) -> RegimeResult:
        # Vol Score [Source 592]
        vs = 5.0
        if vol.ivp1y < 20: vs = 2.0 
        elif vol.ivp1y > 80: vs = 8.0
        
        # Struct Score [Source 593]
        ss = 5.0 + (2 if st.gex_regime == "STICKY" else -2 if st.gex_regime == "SLIPPERY" else 0)
        
        # Edge Score
        es = 5.0 + (2 if ed.vrp_pk_w > 3 else 0) # Uses Parkinson VRP
        
        # Risk Score [Source 594]
        rs = 10.0 - (4 if ex.fast_vol else 0) - (2 if ex.events > 0 else 0)
        
        # Composite Score Formula
        comp = (vs*0.4) + (ss*0.3) + (es*0.2) + (rs*0.1)
        
        # Decision Logic [Source 595]
        name = "NEUTRAL"
        alloc = 0.0
        lots = 0
        
        # Use settings from config to match source logic
        BASE_CAPITAL = settings.BASE_CAPITAL
        MARGIN_SELL = settings.MARGIN_SELL
        MARGIN_BUY = settings.MARGIN_BUY
        
        if comp >= 7.0: 
            name = "AGGRESSIVE_SHORT"
            alloc = 60.0
            lots = int((BASE_CAPITAL * 0.60) / MARGIN_SELL)
        elif comp <= 4.0: 
            name = "LONG_VOL / DEFENSIVE"
            alloc = 20.0
            lots = int((BASE_CAPITAL * 0.20) / MARGIN_BUY)
        elif comp < 7.0: 
            name = "MODERATE_SHORT"
            alloc = 40.0
            lots = int((BASE_CAPITAL * 0.40) / MARGIN_SELL)
        
        # Safety Override [Source 596]
        if ex.fast_vol:
            name = "CASH / STAY AWAY"
            alloc = 0.0
            lots = 0
            
        return RegimeResult(name, comp, ed.primary, vs, ss, es, rs, alloc, lots)

