# app/services/instrument_registry.py

import pandas as pd
import httpx
import logging
import os
import gzip
import io
import json
from datetime import datetime, date
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

UPSTOX_MASTER_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
)

class InstrumentRegistry:
    """
    VolGuard Instrument Registry (AUTHORITATIVE)
    
    SINGLE SOURCE OF TRUTH FOR:
    - Expiries (Weekly / Monthly)
    - Lot Size
    - Tick Size
    - Freeze Quantity
    - Instrument Existence

    ZERO ASSUMPTIONS.
    Exchange/Broker master only.
    """

    def __init__(self, cache_file: str = "instruments_cache.json"):
        self.cache_file = cache_file
        self.master_df: pd.DataFrame = pd.DataFrame()
        self.last_update: Optional[date] = None

    # ------------------------------------------------------------------
    # LOAD & CACHE MASTER
    # ------------------------------------------------------------------
    def load_master(self, force_refresh: bool = False) -> None:
        if not force_refresh and os.path.exists(self.cache_file):
            file_date = datetime.fromtimestamp(
                os.path.getmtime(self.cache_file)
            ).date()
            if file_date == date.today():
                logger.info("📦 Loading instrument master from cache")
                self.master_df = pd.read_json(self.cache_file)
                self._normalize()
                return

        logger.info("⬇️ Downloading fresh Upstox instrument master")
        with httpx.Client(timeout=60.0) as client:
            resp = client.get(UPSTOX_MASTER_URL)
            resp.raise_for_status()

            with gzip.open(io.BytesIO(resp.content), "rt", encoding="utf-8") as f:
                raw = json.load(f)

        self.master_df = pd.DataFrame(raw)
        self._normalize()
        self.master_df.to_json(self.cache_file, index=False)
        self.last_update = date.today()

        logger.info(f"✅ Instrument master loaded: {len(self.master_df)} rows")

    # ------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------
    def _normalize(self) -> None:
        df = self.master_df

        if "trading_symbol" in df.columns:
            df.rename(columns={"trading_symbol": "tradingsymbol"}, inplace=True)

        if "expiry" in df.columns:
            df["expiry"] = pd.to_datetime(df["expiry"], unit="ms", errors="coerce")

        self.master_df = df

    # ------------------------------------------------------------------
    # NIFTY EXPIRY RESOLUTION (NO ASSUMPTIONS)
    # ------------------------------------------------------------------
    def get_nifty_expiries(self) -> Tuple[str, str]:
        """
        Returns:
            (weekly_expiry, monthly_expiry)

        Logic:
        - Derived ONLY from instrument master
        - Uses Upstox `weekly` flag
        - No calendar assumptions
        """

        if self.master_df.empty:
            raise RuntimeError("Instrument master not loaded")

        today = pd.Timestamp.today().normalize()

        opts = self.master_df[
            (self.master_df["segment"] == "NSE_FO") &
            (self.master_df["underlying_symbol"] == "NIFTY") &
            (self.master_df["instrument_type"].isin(["CE", "PE"])) &
            (self.master_df["expiry"] >= today)
        ].copy()

        if opts.empty:
            raise RuntimeError("No valid NIFTY option contracts found")

        # Weekly expiry (authoritative)
        weekly_expiry = (
            opts[opts["weekly"] == True]["expiry"]
            .min()
            .date()
        )

        # Monthly expiry (authoritative)
        monthly_expiry = (
            opts[opts["weekly"] == False]["expiry"]
            .min()
            .date()
        )

        return weekly_expiry.strftime("%Y-%m-%d"), monthly_expiry.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # LOT SIZE / TICK SIZE / FREEZE LIMIT
    # ------------------------------------------------------------------
    def get_nifty_contract_specs(self, expiry: str) -> Dict:
        """
        Fetches structural specs for NIFTY options for a given expiry.
        """

        expiry_dt = pd.to_datetime(expiry)

        df = self.master_df[
            (self.master_df["segment"] == "NSE_FO") &
            (self.master_df["underlying_symbol"] == "NIFTY") &
            (self.master_df["instrument_type"].isin(["CE", "PE"])) &
            (self.master_df["expiry"] == expiry_dt)
        ]

        if df.empty:
            raise RuntimeError(f"No contracts found for expiry {expiry}")

        row = df.iloc[0]

        return {
            "lot_size": int(row["lot_size"]),
            "tick_size": float(row["tick_size"]),
            "freeze_quantity": int(row["freeze_quantity"]),
        }


# GLOBAL SINGLETON
registry = InstrumentRegistry()
