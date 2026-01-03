import httpx
import pandas as pd
import asyncio
from datetime import date, timedelta, datetime
from urllib.parse import quote
from collections import defaultdict
from typing import List, Tuple, Dict, Optional
import logging

logger = logging.getLogger(__name__)

NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY = "NSE_INDEX|India VIX"

class MarketDataClient:
    """
    Production Async Client for Upstox (V2 + V3).
    Handles Historical Data (Dashboard), Live Quotes (Supervisor), and Chains (Trading).
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

    # --- HISTORICAL DATA (For Dashboard/Analytics) ---
    async def get_history(self, key: str, days: int = 400) -> pd.DataFrame:
        """Fetch historical candles for volatility calculations"""
        try:
            edu = quote(key, safe='')
            td = date.today().strftime("%Y-%m-%d")
            fd = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            # Using V2 History Endpoint (Compatible with Schema)
            url = f"{self.base_v2}/historical-candle/{edu}/day/{td}/{fd}"
            resp = await self.client.get(url)
            
            if resp.status_code == 200:
                data = resp.json().get("data", {}).get("candles", [])
                if data:
                    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    return df.astype(float).sort_index()
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching history for {key}: {str(e)}")
            return pd.DataFrame()

    # --- LIVE DATA (For Supervisor) ---
    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        """Fetch live LTP using V3 API"""
        try:
            str_keys = ",".join(keys)
            # Schema Match: V3 LTP
            url = f"{self.base_v3}/market-quote/ltp"
            params = {"instrument_key": str_keys}
            
            resp = await self.client.get(url, params=params)
            
            if resp.status_code == 200:
                data = resp.json().get('data', {})
                results = {}
                for k, details in data.items():
                    # V3 response keys sometimes allow ':' or '|'
                    clean_key = k.replace(':', '|')
                    if details:
                        results[clean_key] = details.get('last_price', 0.0)
                return results
            return {}
        except Exception as e:
            logger.error(f"Error fetching live quotes: {str(e)}")
            return {}

    async def get_spot_price(self) -> float:
        quotes = await self.get_live_quote([NIFTY_KEY])
        return quotes.get(NIFTY_KEY, 0.0)

    async def get_vix(self) -> float:
        quotes = await self.get_live_quote([VIX_KEY])
        return quotes.get(VIX_KEY, 0.0)

    # --- OPTION CHAINS (For Trading Engine) ---
    async def get_option_chain(self, expiry_date: str) -> pd.DataFrame:
        """
        Fetch option chain for a specific expiry.
        Renamed from 'fetch_chain_data' to match TradingEngine calls.
        """
        try:
            params = {"instrument_key": NIFTY_KEY, "expiry_date": expiry_date}
            # Using V2 Option Chain API
            url = f"{self.base_v2}/option/chain"
            
            resp = await self.client.get(url, params=params)
            
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                if not data: return pd.DataFrame()

                # Process into DataFrame for Engine
                rows = []
                for x in data:
                    rows.append({
                        'strike': x['strike_price'],
                        'ce_key': x['call_options']['instrument_key'],
                        'pe_key': x['put_options']['instrument_key'],
                        'ce_iv': x['call_options']['option_greeks'].get('iv', 0),
                        'pe_iv': x['put_options']['option_greeks'].get('iv', 0),
                        'ce_delta': x['call_options']['option_greeks'].get('delta', 0),
                        'pe_delta': x['put_options']['option_greeks'].get('delta', 0),
                        'ce_oi': x['call_options']['market_data'].get('oi', 0),
                        'pe_oi': x['put_options']['market_data'].get('oi', 0),
                    })
                return pd.DataFrame(rows)
            
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching chain for {expiry_date}: {str(e)}")
            return pd.DataFrame()

    # --- METADATA (For Dashboard) ---
    async def get_expiries_and_lot(self) -> Tuple[Optional[str], Optional[str], int]:
        """Helper to find current week/month expiry dates"""
        try:
            resp = await self.client.get(f"{self.base_v2}/option/contract", params={"instrument_key": NIFTY_KEY})
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                if not data: return None, None, 0
                
                # Extract Lot Size (first non-zero)
                lot = next((int(c['lot_size']) for c in data if 'lot_size' in c), 50)
                
                # Parse Dates
                dates = sorted([datetime.strptime(c['expiry'], "%Y-%m-%d").date() for c in data if c.get('expiry')])
                future_dates = [d for d in dates if d >= date.today()]
                
                if not future_dates: return None, None, lot
                
                wk = future_dates[0] # Nearest
                # Simple month logic: Last Thursday logic is complex, just grabbing the furthest in same month or next
                mo = future_dates[-1] 
                
                return wk.strftime("%Y-%m-%d"), mo.strftime("%Y-%m-%d"), lot
            return None, None, 0
        except Exception as e:
            logger.error(f"Error fetching expiries: {str(e)}")
            return None, None, 0
