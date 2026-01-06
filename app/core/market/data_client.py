# app/core/market/data_client.py

import httpx
import pandas as pd
import logging
from datetime import date, datetime, timedelta
from urllib.parse import quote
from typing import List, Dict
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

# ============================================================
# CONSTANTS (DATA ONLY – NO STRUCTURE)
# ============================================================
NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY   = "NSE_INDEX|India VIX"


class MarketDataClient:
    """
    VolGuard Market Data Client (CLEAN PIPE)

    RESPONSIBILITIES:
    ✅ Fetch market prices
    ✅ Fetch historical candles
    ✅ Fetch option chains
    ✅ Fetch quote depth
    ❌ NO expiry logic
    ❌ NO lot size logic
    ❌ NO structural assumptions

    STRUCTURAL TRUTH COMES FROM:
    → InstrumentRegistry ONLY
    """

    def __init__(
        self,
        access_token: str,
        base_url_v2: str = "https://api.upstox.com/v2",
        base_url_v3: str = "https://api.upstox.com/v3",
    ):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Api-Version": "2.0",
        }

        self.base_v2 = base_url_v2
        self.base_v3 = base_url_v3

        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=10.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )

    async def close(self):
        await self.client.aclose()

    # ============================================================
    # 1. EXCHANGE METADATA (NON-STRUCTURAL)
    # ============================================================

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def get_holidays(self) -> List[date]:
        """
        Fetches exchange holidays (runtime safety only).
        Endpoint: /v2/market/holidays
        """
        url = f"{self.base_v2}/market/holidays"

        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json().get("data", [])

            holidays = []
            for item in data:
                if "NSE" in item.get("exchange", "") and item.get("closed", False):
                    d = item.get("date")
                    if d:
                        holidays.append(datetime.strptime(d, "%Y-%m-%d").date())

            return holidays

        except Exception as e:
            logger.error(f"Holiday fetch failed: {e}")
            return []

    # ============================================================
    # 2. HISTORICAL DATA (V3)
    # ============================================================

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    async def get_daily_candles(
        self,
        instrument_key: str,
        days: int = 365,
    ) -> pd.DataFrame:
        """
        Daily candles (cold storage / analytics).
        Endpoint: /v3/historical-candle/{key}/days/1/{to}/{from}
        """
        encoded_key = quote(instrument_key, safe="")
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")

        url = f"{self.base_v3}/historical-candle/{encoded_key}/days/1/{to_date}/{from_date}"

        try:
            resp = await self.client.get(url)
            resp.raise_for_status()

            candles = resp.json().get("data", {}).get("candles", [])
            if not candles:
                return pd.DataFrame()

            df = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume", "oi"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            return df.sort_values("timestamp").reset_index(drop=True)

        except Exception as e:
            logger.error(f"Daily candle fetch failed for {instrument_key}: {e}")
            return pd.DataFrame()

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_intraday_candles(
        self,
        instrument_key: str,
        interval_minutes: int = 1,
    ) -> pd.DataFrame:
        """
        Intraday candles (warm cache / fast vol).
        Endpoint: /v3/historical-candle/intraday/{key}/minutes/{interval}
        """
        encoded_key = quote(instrument_key, safe="")
        url = f"{self.base_v3}/historical-candle/intraday/{encoded_key}/minutes/{interval_minutes}"

        try:
            resp = await self.client.get(url)
            resp.raise_for_status()

            candles = resp.json().get("data", {}).get("candles", [])
            if not candles:
                return pd.DataFrame()

            df = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume", "oi"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            return df.sort_values("timestamp").reset_index(drop=True)

        except Exception as e:
            logger.error(f"Intraday fetch failed for {instrument_key}: {e}")
            return pd.DataFrame()

    # ============================================================
    # 3. LIVE MARKET DATA (V3)
    # ============================================================

    async def get_live_quote(self, keys: List[str]) -> Dict[str, float]:
        """
        Fast LTP fetcher.
        Endpoint: /v3/market-quote/ltp
        """
        if not keys:
            return {}

        url = f"{self.base_v3}/market-quote/ltp"
        params = {"instrument_key": ",".join(keys)}

        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()

            data = resp.json().get("data", {})
            return {k: v.get("last_price", 0.0) for k, v in data.items()}

        except Exception as e:
            logger.error(f"LTP fetch failed: {e}")
            return {}

    async def get_spot_price(self) -> float:
        res = await self.get_live_quote([NIFTY_KEY])
        return res.get(NIFTY_KEY, 0.0)

    async def get_vix(self) -> float:
        res = await self.get_live_quote([VIX_KEY])
        return res.get(VIX_KEY, 0.0)

    # ============================================================
    # 4. QUOTE DEPTH (LIQUIDITY CHECK)
    # ============================================================

    async def get_quote_depth(self, instrument_key: str) -> Dict:
        """
        Bid/Ask depth snapshot.
        Endpoint: /v2/market-quote/quotes
        """
        url = f"{self.base_v2}/market-quote/quotes"
        params = {"instrument_key": instrument_key}

        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()

            data = resp.json().get("data", {}).get(instrument_key, {})
            if not data:
                return {"liquid": False, "spread": float("inf")}

            depth = data.get("depth", {})
            buy = depth.get("buy", [])
            sell = depth.get("sell", [])

            if not buy or not sell:
                return {"liquid": False, "spread": float("inf")}

            best_bid = float(buy[0].get("price", 0))
            best_ask = float(sell[0].get("price", 0))

            spread = best_ask - best_bid
            return {
                "liquid": spread <= 5.0 and best_bid > 0,
                "spread": round(spread, 2),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "total_buy_qty": data.get("total_buy_quantity", 0),
                "total_sell_qty": data.get("total_sell_quantity", 0),
            }

        except Exception as e:
            logger.error(f"Depth fetch failed for {instrument_key}: {e}")
            return {"liquid": False, "spread": float("inf")}

    # ============================================================
    # 5. OPTION CHAIN (STRUCTURE INPUT)
    # ============================================================

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def get_option_chain(self, expiry_date: str) -> pd.DataFrame:
        """
        Full option chain for a GIVEN expiry.
        Expiry must come from InstrumentRegistry.
        """
        url = f"{self.base_v2}/option/chain"
        params = {
            "instrument_key": NIFTY_KEY,
            "expiry_date": expiry_date,
        }

        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()

            rows = []
            for x in resp.json().get("data", []):
                ce = x.get("call_options")
                pe = x.get("put_options")
                if not ce or not pe:
                    continue

                ce_g = ce.get("option_greeks", {})
                pe_g = pe.get("option_greeks", {})

                rows.append({
                    "strike": float(x["strike_price"]),
                    "ce_key": ce.get("instrument_key"),
                    "pe_key": pe.get("instrument_key"),
                    "ce_iv": float(ce_g.get("iv", 0) or 0),
                    "pe_iv": float(pe_g.get("iv", 0) or 0),
                    "ce_delta": float(ce_g.get("delta", 0) or 0),
                    "pe_delta": float(pe_g.get("delta", 0) or 0),
                    "ce_gamma": float(ce_g.get("gamma", 0) or 0),
                    "pe_gamma": float(pe_g.get("gamma", 0) or 0),
                    "ce_oi": int(ce.get("market_data", {}).get("oi", 0)),
                    "pe_oi": int(pe.get("market_data", {}).get("oi", 0)),
                })

            return pd.DataFrame(rows).sort_values("strike").reset_index(drop=True)

        except Exception as e:
            logger.error(f"Option chain fetch failed for {expiry_date}: {e}")
            return pd.DataFrame()
