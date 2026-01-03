import httpx
import pandas as pd
import asyncio
from datetime import date, timedelta, datetime
from urllib.parse import quote
from collections import defaultdict
from typing import List, Tuple, Dict, Optional
import logging
from app.config import settings

logger = logging.getLogger(__name__)

# Constants from your file
NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY = "NSE_INDEX|India VIX"

class MarketDataClient:
    """
    Aligned with AsyncFetcher logic from Ana_260102_191309.txt
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

    # Exact Logic from Source 566
    async def get_history(self, key: str, d: int = 400) -> pd.DataFrame:
        try:
            edu = quote(key, safe='')
            td = date.today().strftime("%Y-%m-%d")
            fd = (date.today() - timedelta(days=d)).strftime("%Y-%m-%d")
            
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

    # Exact Logic from Source 569 (renamed to get_live_quote to match Supervisor)
    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
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

    # Exact Logic from Source 571
    async def get_expiries_and_lot(self) -> Tuple[Optional[str], Optional[str], int]:
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
                    mo = max(mmap[(ny, nm)]) if mmap[(ny, nm)] else wk
                
                return wk.strftime("%Y-%m-%d"), mo.strftime("%Y-%m-%d"), lot
        except Exception as e:
            logger.error(f"Error fetching expiries: {str(e)}")
        return None, None, 0

    # Exact Logic from Source 577
    async def get_option_chain(self, exp: str) -> pd.DataFrame:
        try:
            params = {"instrument_key": NIFTY_KEY, "expiry_date": exp}
            resp = await self.client.get(f"{self.base_v2}/option/chain", params=params)
            
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                return pd.DataFrame([{
                    'strike': x['strike_price'],
                    # Key preservation for Executor
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
            logger.error(f"Error fetching chain for {exp}: {str(e)}")
        return pd.DataFrame()

    # --- Helpers required by Supervisor (Not in your brain file, but needed for system to work) ---
    async def get_spot_price(self) -> float:
        quotes = await self.get_live_quote([NIFTY_KEY])
        return quotes.get(NIFTY_KEY, 0.0)

    async def get_vix(self) -> float:
        quotes = await self.get_live_quote([VIX_KEY])
        return quotes.get(VIX_KEY, 0.0)
