import httpx
import pandas as pd
from datetime import date, timedelta, datetime
from urllib.parse import quote
from typing import List, Tuple, Dict, Optional
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY = "NSE_INDEX|India VIX"

class MarketDataClient:
    def __init__(self, access_token: str, base_url_v2: str, base_url_v3: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "accept": "application/json",
            "Api-Version": "2.0"
        }
        self.base_v2 = base_url_v2
        self.base_v3 = base_url_v3
        # Tighter timeout (5s) so we fail fast instead of hanging
        self.client = httpx.AsyncClient(headers=self.headers, timeout=5.0)

    async def close(self):
        await self.client.aclose()

    # Retry 2 times with 0.5s wait = Max 1.5s delay on failure
    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_history(self, key: str, days: int = 400) -> pd.DataFrame:
        edu = quote(key, safe='')
        td = date.today().strftime("%Y-%m-%d")
        fd = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        url = f"{self.base_v2}/historical-candle/{edu}/day/{td}/{fd}"

        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("candles", [])
            
            if not data: return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            return df.astype(float).sort_index()
        except Exception as e:
            logger.error(f"History Fetch Error: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        if not keys: return {}
        str_keys = ",".join(keys)
        url = f"{self.base_v3}/market-quote/ltp"
        
        try:
            resp = await self.client.get(url, params={"instrument_key": str_keys})
            resp.raise_for_status()
            data = resp.json().get('data', {})
            results = {}
            for k, details in data.items():
                results[k.replace(':', '')] = details.get('last_price', 0.0)
            
            if NIFTY_KEY in keys and results.get(NIFTY_KEY, 0) == 0:
                raise ValueError("Zero price for NIFTY")
            return results
        except Exception:
            raise 

    async def get_spot_price(self) -> float:
        try:
            q = await self.get_live_quote([NIFTY_KEY])
            return q.get(NIFTY_KEY, 0.0)
        except: return 0.0

    async def get_vix(self) -> float:
        try:
            q = await self.get_live_quote([VIX_KEY])
            return q.get(VIX_KEY, 0.0)
        except: return 0.0

    async def get_option_chain(self, expiry_date: str) -> pd.DataFrame:
        try:
            url = f"{self.base_v2}/option/chain"
            resp = await self.client.get(url, params={"instrument_key": NIFTY_KEY, "expiry_date": expiry_date})
            data = resp.json().get('data', [])
            if not data: return pd.DataFrame()
            
            rows = [{
                'strike': x['strike_price'],
                'ce_key': x['call_options']['instrument_key'],
                'pe_key': x['put_options']['instrument_key'],
                'ce_iv': x['call_options']['option_greeks'].get('iv', 0),
                'pe_iv': x['put_options']['option_greeks'].get('iv', 0),
                'ce_delta': x['call_options']['option_greeks'].get('delta', 0),
                'pe_delta': x['put_options']['option_greeks'].get('delta', 0),
                'ce_oi': x['call_options']['market_data'].get('oi', 0),
                'pe_oi': x['put_options']['market_data'].get('oi', 0),
                'ce_gamma': x['call_options']['option_greeks'].get('gamma', 0),
                'pe_gamma': x['put_options']['option_greeks'].get('gamma', 0)
            } for x in data if x.get('call_options') and x.get('put_options')]
            
            return pd.DataFrame(rows)
        except Exception:
            return pd.DataFrame()

    async def get_expiries_and_lot(self) -> Tuple[Optional[str], Optional[str], int]:
        try:
            resp = await self.client.get(f"{self.base_v2}/option/contract", params={"instrument_key": NIFTY_KEY})
            data = resp.json().get('data', [])
            if not data: return None, None, 0
            
            lot = next((int(c['lot_size']) for c in data if 'lot_size' in c), 50)
            dates = sorted([datetime.strptime(c['expiry'], "%Y-%m-%d").date() for c in data if c.get('expiry')])
            futures = [d for d in dates if d >= date.today()]
            
            if not futures: return None, None, lot
            return futures[0].strftime("%Y-%m-%d"), futures[-1].strftime("%Y-%m-%d"), lot
        except:
            return None, None, 0
