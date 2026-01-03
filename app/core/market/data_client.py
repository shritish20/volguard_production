import httpx
import pandas as pd
import asyncio
from datetime import date, timedelta, datetime
from urllib.parse import quote
from collections import defaultdict
from typing import List, Tuple, Dict, Optional
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY = "NSE_INDEX|India VIX"

class MarketDataClient:
    """
    Production Async Client for Upstox (V2 + V3).
    Includes retries and strict error handling.
    """
    def __init__(self, access_token: str, base_url_v2: str, base_url_v3: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "accept": "application/json",
            "Api-Version": "2.0"
        }
        self.base_v2 = base_url_v2
        self.base_v3 = base_url_v3
        # Use a longer timeout for production reliability
        self.client = httpx.AsyncClient(headers=self.headers, timeout=15.0)

    async def close(self):
        await self.client.aclose()

    # --- HISTORICAL DATA ---
    
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def get_history(self, key: str, days: int = 400) -> pd.DataFrame:
        """Fetch historical candles with auto-retry"""
        edu = quote(key, safe='')
        td = date.today().strftime("%Y-%m-%d")
        fd = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        url = f"{self.base_v2}/historical-candle/{edu}/day/{td}/{fd}"
        
        try:
            resp = await self.client.get(url)
            resp.raise_for_status() # Raise error for 4xx/5xx
            
            data = resp.json().get("data", {}).get("candles", [])
            if not data:
                return pd.DataFrame()
                
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            return df.astype(float).sort_index()
            
        except Exception as e:
            logger.error(f"Failed to fetch history for {key}: {str(e)}")
            raise # Let retry handle it

    # --- LIVE DATA ---

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        """Fetch live LTP. Raises error if data is critical and missing."""
        if not keys: return {}
        
        str_keys = ",".join(keys)
        url = f"{self.base_v3}/market-quote/ltp"
        params = {"instrument_key": str_keys}
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            
            data = resp.json().get('data', {})
            results = {}
            
            for k, details in data.items():
                clean_key = k.replace(':', '')
                if details:
                    results[clean_key] = details.get('last_price', 0.0)
            
            # Validation: If we asked for NIFTY and got 0 or nothing, that's a CRITICAL failure
            if NIFTY_KEY in keys and results.get(NIFTY_KEY, 0) == 0:
                raise ValueError("Received Zero/Null price for NIFTY Index")
                
            return results
            
        except Exception as e:
            logger.error(f"Live Quote Error: {str(e)}")
            raise # Propagate so Supervisor can switch to DEGRADED mode

    async def get_spot_price(self) -> float:
        try:
            quotes = await self.get_live_quote([NIFTY_KEY])
            return quotes.get(NIFTY_KEY, 0.0)
        except:
            return 0.0 # Supervisor handles 0 as error

    async def get_vix(self) -> float:
        try:
            quotes = await self.get_live_quote([VIX_KEY])
            return quotes.get(VIX_KEY, 0.0)
        except:
            return 0.0

    # --- OPTION CHAINS ---

    async def get_option_chain(self, expiry_date: str) -> pd.DataFrame:
        """Fetch option chain with validation"""
        try:
            params = {"instrument_key": NIFTY_KEY, "expiry_date": expiry_date}
            url = f"{self.base_v2}/option/chain"
            
            resp = await self.client.get(url, params=params)
            if resp.status_code != 200:
                logger.warning(f"Option Chain API failed: {resp.status_code}")
                return pd.DataFrame()
                
            data = resp.json().get('data', [])
            if not data: return pd.DataFrame()
            
            rows = [
                {
                    'strike': x['strike_price'],
                    'ce_key': x['call_options']['instrument_key'],
                    'pe_key': x['put_options']['instrument_key'],
                    'ce_iv': x['call_options']['option_greeks'].get('iv', 0) or 0,
                    'pe_iv': x['put_options']['option_greeks'].get('iv', 0) or 0,
                    'ce_delta': x['call_options']['option_greeks'].get('delta', 0) or 0,
                    'pe_delta': x['put_options']['option_greeks'].get('delta', 0) or 0,
                    'ce_oi': x['call_options']['market_data'].get('oi', 0),
                    'pe_oi': x['put_options']['market_data'].get('oi', 0),
                    'ce_gamma': x['call_options']['option_greeks'].get('gamma', 0) or 0,
                    'pe_gamma': x['put_options']['option_greeks'].get('gamma', 0) or 0
                }
                for x in data if x.get('call_options') and x.get('put_options')
            ]
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.error(f"Chain Fetch Error: {str(e)}")
            return pd.DataFrame()

    async def get_expiries_and_lot(self) -> Tuple[Optional[str], Optional[str], int]:
        try:
            resp = await self.client.get(f"{self.base_v2}/option/contract", params={"instrument_key": NIFTY_KEY})
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                if not data: return None, None, 0
                
                lot = next((int(c['lot_size']) for c in data if 'lot_size' in c), 50)
                dates = sorted([datetime.strptime(c['expiry'], "%Y-%m-%d").date() for c in data if c.get('expiry')])
                future_dates = [d for d in dates if d >= date.today()]
                
                if not future_dates: return None, None, lot
                
                wk = future_dates[0]
                # Simple Monthly logic: Find furthest in same month or next
                mo = future_dates[-1] 
                
                return wk.strftime("%Y-%m-%d"), mo.strftime("%Y-%m-%d"), lot
            return None, None, 0
        except Exception as e:
            logger.error(f"Expiry Fetch Error: {str(e)}")
            return None, None, 0
