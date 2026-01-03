import numpy as np
import asyncio
from arch import arch_model
import pandas as pd
from app.schemas.analytics import VolMetrics

# [span_8](start_span)[span_9](start_span)Derived from[span_8](end_span)[span_9](end_span)

class VolatilityEngine:
    
    @staticmethod
    def _run_garch_sync(ret, w):
        [span_10](start_span)"""CPU bound GARCH calculation[span_10](end_span)"""
        try:
            if len(ret) < 100: return 0
            # Arch model fit
            res = arch_model(ret*100, vol='Garch', p=1, q=1, dist='normal').fit(disp='off', show_warning=False)
            return np.sqrt(res.forecast(horizon=w, reindex=False).variance.values[-1,-1])*np.sqrt(252)
        except: return 0

    async def calculate_volatility(self, nh: pd.DataFrame, vh: pd.DataFrame, spot_live: float, vix_live: float) -> VolMetrics:
        [span_11](start_span)"""Calculate comprehensive volatility metrics[span_11](end_span)"""
        spot = spot_live
        vix = vix_live
        is_fallback = False
        
        # Fallback to history if live data missing
        if spot <= 0 and not nh.empty:
            spot = nh.iloc[-1]['close']
            is_fallback = True
        if vix <= 0 and not vh.empty:
            vix = vh.iloc[-1]['close']
            is_fallback = True

        # [span_12](start_span)Returns & Realized Vol[span_12](end_span)
        ret = np.log(nh['close']/nh['close'].shift(1)).dropna()
        rv7 = ret.rolling(7).std().iloc[-1]*np.sqrt(252)*100
        rv28 = ret.rolling(28).std().iloc[-1]*np.sqrt(252)*100

        # [span_13](start_span)Parkinson Vol[span_13](end_span)
        const = 1.0/(4.0*np.log(2.0))
        pk7 = np.sqrt((np.log(nh['high']/nh['low'])**2).tail(7).mean()*const)*np.sqrt(252)*100
        pk28 = np.sqrt((np.log(nh['high']/nh['low'])**2).tail(28).mean()*const)*np.sqrt(252)*100

        # [span_14](start_span)Async GARCH execution[span_14](end_span)
        ga7 = await asyncio.to_thread(self._run_garch_sync, ret, 7)
        ga28 = await asyncio.to_thread(self._run_garch_sync, ret, 28)
        
        if ga7 == 0: ga7 = rv7
        if ga28 == 0: ga28 = rv28

        # [span_15](start_span)Vol of Vol[span_15](end_span)
        vov = np.log(vh['close']/vh['close'].shift(1)).dropna().tail(30).std()*np.sqrt(252)*100
        
        def calc_ivp(window):
            if len(vh) < window: return 0.0
            history = vh['close'].tail(window)
            return (history < vix).mean() * 100

        return VolMetrics(
            spot, vix, vov, rv7, rv28, ga7, ga28, pk7, pk28, 
            calc_ivp(30), calc_ivp(90), calc_ivp(252), is_fallback
        )
