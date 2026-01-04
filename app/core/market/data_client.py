# app/core/market/data_client.py

import httpx
import pandas as pd
import logging
import asyncio
from datetime import date, datetime, timedelta
from urllib.parse import quote
from typing import List, Tuple, Dict, Optional, Union
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logger = logging.getLogger(__name__)

# Constants
NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY = "NSE_INDEX|India VIX"

class MarketDataClient:
    """
    VolGuard Smart Market Client (VolGuard 3.0)
    
    Architecture:
    V3: Execution, History, Fast LTP
    V2: Rich Data (Chain, Depth, Holidays)
    Async: strictly non-blocking (httpx)
    """

    def __init__(self, access_token: str, base_url_v2: str = "https://api.upstox.com/v2", base_url_v3: str = "https://api.upstox.com/v3"):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Api-Version": "2.0",  # Default to 2.0, overridden in V3 calls if needed
        }
        self.base_v2 = base_url_v2
        self.base_v3 = base_url_v3
        
        # Async Client with reasonable timeouts
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=10.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

    async def close(self):
        await self.client.aclose()

    # ==========================================
    # 1. MASTER CLOCK & METADATA (V2)
    # ==========================================

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def get_holidays(self) -> List[date]:
        """
        Fetches exchange holidays to prevent trading on off-days.
        Endpoint: /v2/market/holidays
        """
        url = f"{self.base_v2}/market/holidays"
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            
            holidays = []
            for item in data:
                # Filter for NSE trading holidays
                if "NSE" in item.get("exchange", "") and item.get("closed", False):
                    d_str = item.get("date")
                    if d_str:
                        holidays.append(datetime.strptime(d_str, "%Y-%m-%d").date())
            return holidays
        except Exception as e:
            logger.error(f"Failed to fetch holidays: {e}")
            return []

    async def get_contract_details(self, symbol: str = "NIFTY") -> Dict:
        """
        Dynamic Lot Size & Freeze Limits.
        Endpoint: /v2/option/contract
        """
        url = f"{self.base_v2}/option/contract"
        params = {"instrument_key": NIFTY_KEY}
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            
            if not data:
                return {}
            
            # Get the first valid contract to extract metadata
            contract = data[0]
            
            return {
                "lot_size": int(contract.get("lot_size", 50)),
                "freeze_limit": int(contract.get("freeze_quantity", 1800)),
                "tick_size": float(contract.get("tick_size", 0.05))
            }
        except Exception as e:
            logger.error(f"Failed to fetch contract details: {e}")
            return {"lot_size": 50, "freeze_limit": 1800}  # Safe defaults

    # ==========================================
    # 2. HISTORICAL DATA (V3 "SMART" SPLIT)
    # ==========================================

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    async def get_daily_candles(self, instrument_key: str, days: int = 365) -> pd.DataFrame:
        """
        TIER 1: COLD STORAGE
        Fetch Daily Candles for Postgres.
        
        FIXED: Uses plural 'days' instead of singular 'day'.
        Endpoint: /v3/historical-candle/{key}/days/{interval}/{to}/{from}
        """
        encoded_key = quote(instrument_key, safe="")
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        # FIXED: Changed '/day/1/' to '/days/1/'
        url = f"{self.base_v3}/historical-candle/{encoded_key}/days/1/{to_date}/{from_date}"
        
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("candles", [])
            
            if not data:
                return pd.DataFrame()
            
            # Upstox: [Timestamp, Open, High, Low, Close, Volume, OI]
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            return df.sort_values("timestamp").reset_index(drop=True)
        except Exception as e:
            logger.error(f"Daily candle fetch failed for {instrument_key}: {e}")
            return pd.DataFrame()

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_intraday_candles(self, instrument_key: str, interval_minutes: int = 1) -> pd.DataFrame:
        """
        TIER 2: WARM CACHE
        Fetch Intraday Candles for Fast Vol / Crash Detection.
        
        FIXED: Uses plural 'minutes' instead of singular 'minute'.
        Endpoint: /v3/historical-candle/intraday/{key}/minutes/{interval}
        """
        encoded_key = quote(instrument_key, safe="")
        
        # FIXED: Changed '/minute/' to '/minutes/'
        url = f"{self.base_v3}/historical-candle/intraday/{encoded_key}/minutes/{interval_minutes}"
        
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("candles", [])
            
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Sort is critical for volatility calc
            return df.sort_values("timestamp").reset_index(drop=True)
        except Exception as e:
            logger.error(f"Intraday fetch failed for {instrument_key}: {e}")
            return pd.DataFrame()

    # ==========================================
    # 3. LIVE MARKET DATA (HYBRID V2/V3)
    # ==========================================

    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        """
        Fast LTP Fetcher (V3).
        Used for Supervisor Loop (Lightweight).
        """
        if not keys:
            return {}
            
        # V3 Endpoint
        url = f"{self.base_v3}/market-quote/ltp"
        params = {"instrument_key": ",".join(keys)}
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            
            results = {}
            for k, v in data.items():
                # Clean key formatting
                clean_k = k.replace(":", "").replace("NSE_INDEX|", "NSE_INDEX:")
                # V3 returns formatted keys sometimes, we map back to input
                # Or just map the response directly
                results[k] = v.get("last_price", 0.0)
                
            return results
        except Exception as e:
            logger.error(f"LTP fetch failed: {e}")
            return {}

    async def get_quote_depth(self, instrument_key: str) -> Dict:
        """
        LIQUIDITY GATE (V2).
        Fetches Bid/Ask Spread and Volumes.
        Endpoint: /v2/market-quote/quotes
        """
        url = f"{self.base_v2}/market-quote/quotes"
        params = {"instrument_key": instrument_key}
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            
            data = resp.json().get("data", {}).get(instrument_key, {})
            if not data:
                return {"liquid": False, "spread": 999.9}
                
            depth = data.get("depth", {})
            buy_depth = depth.get("buy", [])
            sell_depth = depth.get("sell", [])
            
            if not buy_depth or not sell_depth:
                return {"liquid": False, "spread": 999.9, "reason": "NO_DEPTH"}
                
            best_bid = float(buy_depth[0].get("price", 0))
            best_ask = float(sell_depth[0].get("price", 0))
            
            # Spread Logic
            spread = best_ask - best_bid
            is_liquid = spread <= 5.0 and best_bid > 0  # Configurable threshold
            
            return {
                "liquid": is_liquid,
                "spread": round(spread, 2),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "total_buy_qty": data.get("total_buy_quantity", 0),
                "total_sell_qty": data.get("total_sell_quantity", 0),
                "upper_circuit": data.get("upper_circuit_limit"),
                "lower_circuit": data.get("lower_circuit_limit")
            }
        except Exception as e:
            logger.error(f"Depth fetch failed for {instrument_key}: {e}")
            return {"liquid": False, "spread": 999.9, "error": str(e)}

    # ==========================================
    # 4. STRUCTURE DATA (V2)
    # ==========================================

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_option_chain(self, expiry_date: str) -> pd.DataFrame:
        """
        Fetches Full Option Chain (V2).
        Required for Structure Engine (OI, Greeks, PCR).
        """
        url = f"{self.base_v2}/option/chain"
        params = {
            "instrument_key": NIFTY_KEY,
            "expiry_date": expiry_date
        }
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            
            data = resp.json().get("data", [])
            if not data:
                return pd.DataFrame()
            
            rows = []
            for x in data:
                ce = x.get("call_options", {})
                pe = x.get("put_options", {})
                
                # Filter bad data
                if not ce or not pe:
                    continue
                    
                ce_greeks = ce.get("option_greeks", {})
                pe_greeks = pe.get("option_greeks", {})
                
                rows.append({
                    "strike": float(x["strike_price"]),
                    "ce_key": ce.get("instrument_key"),
                    "pe_key": pe.get("instrument_key"),
                    "ce_iv": float(ce_greeks.get("iv", 0) or 0),
                    "pe_iv": float(pe_greeks.get("iv", 0) or 0),
                    "ce_delta": float(ce_greeks.get("delta", 0) or 0),
                    "pe_delta": float(pe_greeks.get("delta", 0) or 0),
                    "ce_gamma": float(ce_greeks.get("gamma", 0) or 0),
                    "pe_gamma": float(pe_greeks.get("gamma", 0) or 0),
                    "ce_oi": int(ce.get("market_data", {}).get("oi", 0)),
                    "pe_oi": int(pe.get("market_data", {}).get("oi", 0)),
                })
                
            return pd.DataFrame(rows).sort_values("strike").reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"Option chain fetch failed: {e}")
            return pd.DataFrame()

    async def get_spot_price(self) -> float:
        res = await self.get_live_quote([NIFTY_KEY])
        return res.get(NIFTY_KEY, 0.0)

    async def get_vix(self) -> float:
        res = await self.get_live_quote([VIX_KEY])
        return res.get(VIX_KEY, 0.0)

    # ==========================================
    # 5. EXPIRY UTILITIES (V2)
    # ==========================================

    async def get_expiries(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns (Weekly Expiry, Monthly Expiry).
        Excludes expiries ending today/tomorrow (Safety).
        """
        url = f"{self.base_v2}/option/contract"
        params = {"instrument_key": NIFTY_KEY}
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            
            if not data:
                return None, None
            
            # Parse and Sort Unique Expiries
            expiries = sorted(list(set(
                datetime.strptime(c["expiry"], "%Y-%m-%d").date()
                for c in data if c.get("expiry")
            )))
            
            # Filter Logic: Must be at least 1 day away to avoid expiry gamma risk
            today = date.today()
            valid_expiries = [d for d in expiries if (d - today).days >= 1]
            
            if not valid_expiries:
                return None, None
                
            # Logic:
            # Weekly = Nearest valid
            # Monthly = Farthest in current or next month (simplified for now)
            
            weekly = valid_expiries[0].strftime("%Y-%m-%d")
            monthly = valid_expiries[-1].strftime("%Y-%m-%d")
            
            return weekly, monthly
            
        except Exception as e:
            logger.error(f"Expiry fetch failed: {e}")
            return None, None
