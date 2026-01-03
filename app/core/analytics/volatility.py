import numpy as np
import asyncio
from arch import arch_model
import pandas as pd
from app.schemas.analytics import VolMetrics

class VolatilityEngine:
    @staticmethod
    def _run_garch_sync(ret, w):
        """CPU bound GARCH calculation matching Logic.run_garch_sync"""
        try:
            if len(ret) < 100: return 0
            # Exact match to Source 581
            res = arch_model(ret*100, vol='Garch', p=1, q=1, dist='normal').fit(disp='off', show_warning=False)
            return np.sqrt(res.forecast(horizon=w, reindex=False).variance.values[-1,-1])*np.sqrt(252)
        except: return 0

    async def calculate_volatility(self, nh: pd.DataFrame, vh: pd.DataFrame, spot_live: float, vix_live: float) -> VolMetrics:
        # Fallback Logic [Source 582]
        spot = spot_live
        vix = vix_live
        is_fallback = False
        
        if spot <= 0 and not nh.empty:
            spot = nh.iloc[-1]['close']
            is_fallback = True
        if vix <= 0 and not vh.empty:
            vix = vh.iloc[-1]['close']
            is_fallback = True

        # Returns & Realized Vol [Source 583]
        ret = np.log(nh['close']/nh['close'].shift(1)).dropna()
        rv7 = ret.rolling(7).std().iloc[-1]*np.sqrt(252)*100
        rv28 = ret.rolling(28).std().iloc[-1]*np.sqrt(252)*100

        # Parkinson Vol [Source 584] - Exact Formula Match
        const = 1.0/(4.0*np.log(2.0))
        pk7 = np.sqrt((np.log(nh['high']/nh['low'])**2).tail(7).mean()*const)*np.sqrt(252)*100
        pk28 = np.sqrt((np.log(nh['high']/nh['low'])**2).tail(28).mean()*const)*np.sqrt(252)*100

        # Async GARCH [Source 584]
        ga7 = await asyncio.to_thread(self._run_garch_sync, ret, 7)
        ga28 = await asyncio.to_thread(self._run_garch_sync, ret, 28)
        
        if ga7 == 0: ga7 = rv7
        if ga28 == 0: ga28 = rv28

        # Vol of Vol [Source 585]
        vov = np.log(vh['close']/vh['close'].shift(1)).dropna().tail(30).std()*np.sqrt(252)*100
        
        def calc_ivp(window):
            if len(vh) < window: return 0.0
            history = vh['close'].tail(window)
            return (history < vix).mean() * 100

        return VolMetrics(spot, vix, vov, rv7, rv28, ga7, ga28, pk7, pk28, 
                          calc_ivp(30), calc_ivp(90), calc_ivp(252), is_fallback)
