import httpx
import pandas as pd
from datetime import date, timedelta, datetime
from urllib.parse import quote
from typing import List, Dict, Tuple, Optional
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

# -------------------------------
# Instrument Keys
# -------------------------------
NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY   = "NSE_INDEX|India VIX"

# -------------------------------
# Market Data Client (REST ONLY)
# -------------------------------
class MarketDataClient:
    """
    Upstox REST-only Market Data Client
    - v2: Option Chain, Contracts
    - v3: LTP, Historical Candles
    """

    def __init__(self, access_token: str, base_url_v2: str, base_url_v3: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.base_v2 = base_url_v2.rstrip("/")
        self.base_v3 = base_url_v3.rstrip("/")
        self.client = httpx.AsyncClient(headers=self.headers, timeout=6.0)

    async def close(self):
        await self.client.aclose()

    # ==========================================================
    # LTP (v3)
    # ==========================================================
    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        if not keys:
            return {}

        encoded = ",".join(keys)
        url = f"{self.base_v3}/market-quote/ltp"

        resp = await self.client.get(url, params={"instrument_key": encoded})
        resp.raise_for_status()

        data = resp.json().get("data", {})
        return {
            k.replace(":", ""): v.get("last_price", 0.0)
            for k, v in data.items()
        }

    async def get_spot_price(self) -> float:
        try:
            q = await self.get_live_quote([NIFTY_KEY])
            return float(q.get(NIFTY_KEY, 0.0))
        except Exception:
            return 0.0

    async def get_vix(self) -> float:
        try:
            q = await self.get_live_quote([VIX_KEY])
            return float(q.get(VIX_KEY, 0.0))
        except Exception:
            return 0.0

    # ==========================================================
    # FAST CANDLES (v3) – 5 MIN / INTRADAY
    # ==========================================================
    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_intraday_candles(
        self,
        instrument_key: str,
        interval_minutes: int = 5
    ) -> pd.DataFrame:
        """
        FAST analytics:
        - RV
        - Parkinson
        - Short-term vol spike
        """

        encoded = quote(instrument_key, safe="")
        url = f"{self.base_v3}/historical-candle/{encoded}/minutes/{interval_minutes}"

        today = date.today().strftime("%Y-%m-%d")
        yesterday = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")

        full_url = f"{url}/{today}/{yesterday}"

        resp = await self.client.get(full_url)
        resp.raise_for_status()

        candles = resp.json().get("data", {}).get("candles", [])
        return self._to_df(candles)

    # ==========================================================
    # SLOW CANDLES (v3) – DAILY
    # ==========================================================
    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_daily_candles(
        self,
        instrument_key: str,
        days: int = 400
    ) -> pd.DataFrame:
        """
        SLOW analytics:
        - GARCH
        - IVP
        - Regime detection
        """

        encoded = quote(instrument_key, safe="")
        url = f"{self.base_v3}/historical-candle/{encoded}/days/1"

        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")

        full_url = f"{url}/{to_date}/{from_date}"

        resp = await self.client.get(full_url)
        resp.raise_for_status()

        candles = resp.json().get("data", {}).get("candles", [])
        return self._to_df(candles)

    # ==========================================================
    # OPTION CHAIN (v2)
    # ==========================================================
    async def get_option_chain(self, expiry_date: str) -> pd.DataFrame:
        try:
            url = f"{self.base_v2}/option/chain"
            resp = await self.client.get(
                url,
                params={
                    "instrument_key": NIFTY_KEY,
                    "expiry_date": expiry_date
                }
            )
            resp.raise_for_status()

            rows = []
            for x in resp.json().get("data", []):
                if not x.get("call_options") or not x.get("put_options"):
                    continue

                rows.append({
                    "strike": x["strike_price"],
                    "ce_iv": x["call_options"]["option_greeks"].get("iv", 0),
                    "pe_iv": x["put_options"]["option_greeks"].get("iv", 0),
                    "ce_delta": x["call_options"]["option_greeks"].get("delta", 0),
                    "pe_delta": x["put_options"]["option_greeks"].get("delta", 0),
                    "ce_oi": x["call_options"]["market_data"].get("oi", 0),
                    "pe_oi": x["put_options"]["market_data"].get("oi", 0),
                    "ce_gamma": x["call_options"]["option_greeks"].get("gamma", 0),
                    "pe_gamma": x["put_options"]["option_greeks"].get("gamma", 0),
                })

            return pd.DataFrame(rows)

        except Exception as e:
            logger.error(f"Option chain error: {e}")
            return pd.DataFrame()

    # ==========================================================
    # EXPIRIES & LOT SIZE (v2)
    # ==========================================================
    async def get_expiries_and_lot(self) -> Tuple[Optional[str], Optional[str], int]:
        try:
            url = f"{self.base_v2}/option/contract"
            resp = await self.client.get(url, params={"instrument_key": NIFTY_KEY})
            resp.raise_for_status()

            data = resp.json().get("data", [])
            if not data:
                return None, None, 0

            lot = int(next((c["lot_size"] for c in data if "lot_size" in c), 50))
            expiries = sorted(
                datetime.strptime(c["expiry"], "%Y-%m-%d").date()
                for c in data if c.get("expiry")
            )

            future_exp = [e for e in expiries if e >= date.today()]
            if not future_exp:
                return None, None, lot

            return (
                future_exp[0].strftime("%Y-%m-%d"),
                future_exp[-1].strftime("%Y-%m-%d"),
                lot
            )

        except Exception as e:
            logger.error(f"Expiry fetch error: {e}")
            return None, None, 0

    # ==========================================================
    # Helpers
    # ==========================================================
    def _to_df(self, candles: List[List]) -> pd.DataFrame:
        if not candles:
            return pd.DataFrame()

        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        return df.astype(float).sort_index()
