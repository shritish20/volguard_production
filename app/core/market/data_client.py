# app/core/market/data_client.py

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
    """
    Single source of market truth.
    This class MUST fail fast on bad data.
    """

    def __init__(self, access_token: str, base_url_v2: str, base_url_v3: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "accept": "application/json",
            "Api-Version": "2.0",
        }
        self.base_v2 = base_url_v2
        self.base_v3 = base_url_v3
        self.client = httpx.AsyncClient(headers=self.headers, timeout=5.0)

    async def close(self):
        await self.client.aclose()

    # ======================================================
    # HISTORICAL DATA
    # ======================================================
    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_history(self, key: str, days: int = 400) -> pd.DataFrame:
        edu = quote(key, safe="")
        td = date.today().strftime("%Y-%m-%d")
        fd = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        url = f"{self.base_v2}/historical-candle/{edu}/day/{td}/{fd}"

        resp = await self.client.get(url)
        resp.raise_for_status()

        data = resp.json().get("data", {}).get("candles", [])
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(
            data,
            columns=["timestamp", "open", "high", "low", "close", "volume", "oi"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

        return df.astype(float).sort_index()

    # ======================================================
    # LIVE QUOTES
    # ======================================================
    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        if not keys:
            return {}

        url = f"{self.base_v3}/market-quote/ltp"
        resp = await self.client.get(url, params={"instrument_key": ",".join(keys)})
        resp.raise_for_status()

        data = resp.json().get("data", {})
        results = {}

        for k, details in data.items():
            clean_key = k.replace(":", "").strip()
            results[clean_key] = details.get("last_price", 0.0)

        return results

    async def get_spot_price(self) -> float:
        try:
            q = await self.get_live_quote([NIFTY_KEY])
            return q.get(NIFTY_KEY, 0.0)
        except Exception:
            return 0.0

    async def get_vix(self) -> float:
        try:
            q = await self.get_live_quote([VIX_KEY])
            return q.get(VIX_KEY, 0.0)
        except Exception:
            return 0.0

    # ======================================================
    # OPTION CHAIN
    # ======================================================
    async def get_option_chain(self, expiry_date: str) -> pd.DataFrame:
        url = f"{self.base_v2}/option/chain"
        resp = await self.client.get(
            url, params={"instrument_key": NIFTY_KEY, "expiry_date": expiry_date}
        )
        resp.raise_for_status()

        data = resp.json().get("data", [])
        if not data:
            return pd.DataFrame()

        rows = []

        for x in data:
            ce = x.get("call_options")
            pe = x.get("put_options")
            if not ce or not pe:
                continue

            greeks_ce = ce.get("option_greeks") or {}
            greeks_pe = pe.get("option_greeks") or {}
            mkt_ce = ce.get("market_data") or {}
            mkt_pe = pe.get("market_data") or {}

            # Reject strikes with broken greeks
            if (
                greeks_ce.get("iv") is None
                or greeks_pe.get("iv") is None
                or greeks_ce.get("delta") is None
                or greeks_pe.get("delta") is None
            ):
                continue

            rows.append(
                {
                    "strike": float(x["strike_price"]),
                    "ce_key": ce["instrument_key"],
                    "pe_key": pe["instrument_key"],
                    "ce_iv": float(greeks_ce["iv"]),
                    "pe_iv": float(greeks_pe["iv"]),
                    "ce_delta": float(greeks_ce["delta"]),
                    "pe_delta": float(greeks_pe["delta"]),
                    "ce_gamma": float(greeks_ce.get("gamma", 0.0)),
                    "pe_gamma": float(greeks_pe.get("gamma", 0.0)),
                    "ce_oi": int(mkt_ce.get("oi", 0)),
                    "pe_oi": int(mkt_pe.get("oi", 0)),
                }
            )

        df = pd.DataFrame(rows)
        return df.dropna().reset_index(drop=True)

    # ======================================================
    # EXPIRY & LOT
    # ======================================================
    async def get_expiries_and_lot(self) -> Tuple[Optional[str], Optional[str], int]:
        resp = await self.client.get(
            f"{self.base_v2}/option/contract",
            params={"instrument_key": NIFTY_KEY},
        )
        resp.raise_for_status()

        data = resp.json().get("data", [])
        if not data:
            return None, None, 0

        lot = int(next((c["lot_size"] for c in data if c.get("lot_size")), 50))

        expiries = sorted(
            {
                datetime.strptime(c["expiry"], "%Y-%m-%d").date()
                for c in data
                if c.get("expiry")
            }
        )

        valid = [
            d
            for d in expiries
            if (d - date.today()).days >= 2
        ]

        if not valid:
            return None, None, lot

        return valid[0].strftime("%Y-%m-%d"), valid[-1].strftime("%Y-%m-%d"), lot
