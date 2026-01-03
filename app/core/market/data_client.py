import httpx
import pandas as pd
import asyncio
from datetime import date, timedelta, datetime
from urllib.parse import quote
from collections import defaultdict
from typing import List, Tuple, Dict, Optional
import logging

# [span_2](start_span)[span_3](start_span)Derived from[span_2](end_span)[span_3](end_span)
logger = logging.getLogger(__name__)

NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY = "NSE_INDEX|India VIX"

class MarketDataClient:
    """
    Production Async Client for Upstox.
    Handles Historical Data, Live Quotes, and Option Chains.
    """
    def __init__(self, access_token: str, base_url_v2: str, base_url_v3: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "accept": "application/json",
            "Api-Version": "2.0"
        }
        self.base_v2 = base_url_v2
        self.base_v3 = base_url_v3
        self.client = httpx.AsyncClient(headers=self.headers, timeout=10.0)

    async def close(self):
        await self.client.aclose()

    async def get_history(self, key: str, days: int = 400) -> pd.DataFrame:
        [span_4](start_span)"""Fetch historical candles for volatility calculations[span_4](end_span)"""
        try:
            edu = quote(key, safe='')
            td = date.today().strftime("%Y-%m-%d")
            fd = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            url = f"{self.base_v2}/historical-candle/{edu}/day/{td}/{fd}"
            resp = await self.client.get(url)
            
            if resp.status_code == 200:
                data = resp.json().get("data", {}).get("candles", [])
                if data:
                    df = pd.DataFrame(data, columns=["timestamp","open","high","low","close","volume","oi"])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    return df.astype(float).sort_index()
        except Exception as e:
            logger.error(f"Error fetching history for {key}: {str(e)}")
        return pd.DataFrame()

    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        [span_5](start_span)"""Fetch live LTP[span_5](end_span)"""
        try:
            params = {"instrument_key": ",".join(keys)}
            resp = await self.client.get(f"{self.base_v3}/market-quote/ltp", params=params)
            
            if resp.status_code == 200:
                d = resp.json().get('data', {})
                res = {}
                for k in keys:
                    val = d.get(k) or d.get(k.replace('|', ':'))
                    if val: res[k] = val['last_price']
                return res
        except Exception as e:
            logger.error(f"Error fetching live quotes: {str(e)}")
        return {}

    async def get_expiries_and_lot(self) -> Tuple[Optional[str], Optional[str], int]:
        [span_6](start_span)"""Get Current Week and Month expiries + Lot size[span_6](end_span)"""
        try:
            resp = await self.client.get(f"{self.base_v2}/option/contract", params={"instrument_key": NIFTY_KEY})
            if resp.status_code == 200:
                d = resp.json().get('data', [])
                if not d: return None, None, 0
                
                lot = next((int(c['lot_size']) for c in d if 'lot_size' in c), 0)
                dates = sorted([datetime.strptime(c['expiry'], "%Y-%m-%d").date() for c in d if c.get('expiry')])
                
                valid = [x for x in dates if x >= date.today()]
                if not valid: return None, None, lot
                
                wk = valid[0]
                mmap = defaultdict(list)
                for x in dates: mmap[(x.year, x.month)].append(x)
                
                curr_month_dates = mmap[(date.today().year, date.today().month)]
                if curr_month_dates and max(curr_month_dates) >= date.today():
                    mo = max(curr_month_dates)
                else:
                    ny = date.today().year + (1 if date.today().month == 12 else 0)
                    nm = 1 if date.today().month == 12 else date.today().month + 1
                    mo = max(mmap[(ny, nm)]) if mmap.get((ny, nm)) else wk
                
                return wk.strftime("%Y-%m-%d"), mo.strftime("%Y-%m-%d"), lot
        except Exception as e:
            logger.error(f"Error fetching expiries: {str(e)}")
        return None, None, 0

    async def fetch_chain_data(self, expiry: str) -> pd.DataFrame:
        [span_7](start_span)"""Fetch option chain and extract Greeks + Keys[span_7](end_span)"""
        try:
            params = {"instrument_key": NIFTY_KEY, "expiry_date": expiry}
            resp = await self.client.get(f"{self.base_v2}/option/chain", params=params)
            
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                return pd.DataFrame([{
                    'strike': x['strike_price'],
                    'ce_key': x['call_options']['instrument_key'],
                    'pe_key': x['put_options']['instrument_key'],
                    'ce_iv': x['call_options']['option_greeks']['iv'], 
                    'pe_iv': x['put_options']['option_greeks']['iv'],
                    'ce_delta': x['call_options']['option_greeks']['delta'], 
                    'pe_delta': x['put_options']['option_greeks']['delta'],
                    'ce_gamma': x['call_options']['option_greeks']['gamma'], 
                    'pe_gamma': x['put_options']['option_greeks']['gamma'],
                    'ce_oi': x['call_options']['market_data']['oi'], 
                    'pe_oi': x['put_options']['market_data']['oi'],
                } for x in data])
        except Exception as e:
            logger.error(f"Error fetching chain for {expiry}: {str(e)}")
        return pd.DataFrame()

    # --- Supervisor Helper Methods ---
    
    async def get_spot_price(self) -> float:
        """Helper for Supervisor"""
        quotes = await self.get_live_quote([NIFTY_KEY])
        return quotes.get(NIFTY_KEY, 0.0)

    async def get_vix(self) -> float:
        """Helper for Supervisor"""
        quotes = await self.get_live_quote([VIX_KEY])
        return quotes.get(VIX_KEY, 0.0)
    
    async def get_active_option_instruments(self) -> List[str]:
        """Get active strike keys for WebSocket subscription"""
        try:
            spot = await self.get_spot_price()
            wk, _, _ = await self.get_expiries_and_lot()
            if not wk or spot == 0: return [NIFTY_KEY, VIX_KEY]
            
            chain = await self.fetch_chain_data(wk)
            if chain.empty: return [NIFTY_KEY, VIX_KEY]
            
            # Select 20 strikes around ATM
            nearby = chain.iloc[(chain['strike'] - spot).abs().argsort()[:20]]
            keys = [NIFTY_KEY, VIX_KEY]
            keys.extend(nearby['ce_key'].tolist())
            keys.extend(nearby['pe_key'].tolist())
            return keys
        except:
            return [NIFTY_KEY, VIX_KEY]
