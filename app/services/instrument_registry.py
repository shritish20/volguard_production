# app/services/instrument_registry.py

import gzip
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import logging
import os
import httpx
import threading

logger = logging.getLogger(__name__)


class InstrumentRegistry:
    """
    Single source of truth for instrument metadata.
    Thread-safe, cached, and production-safe.
    """

    _instance = None
    _lock = threading.Lock()

    MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    DEFAULT_PATH = "data/complete.json.gz"
    MAX_FILE_AGE_DAYS = 1  # Re-download daily

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._data = None
            cls._instance._index = {}
            cls._instance._loaded_at = None
        return cls._instance

    # --------------------------------------------------
    # LOADING
    # --------------------------------------------------
    def load_master(self, file_path: str = DEFAULT_PATH):
        with self._lock:
            if self._data is not None:
                return

            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            if self._needs_refresh(file_path):
                logger.info("Instrument master missing or stale. Downloading...")
                self._download_master(file_path)

            try:
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    raw = json.load(f)

                df = pd.DataFrame(raw)

                # Defensive schema normalization
                required_cols = {
                    "instrument_key", "exchange", "instrument_type",
                    "name", "expiry", "lot_size", "strike_price"
                }
                missing = required_cols - set(df.columns)
                if missing:
                    raise ValueError(f"Instrument master missing columns: {missing}")

                df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
                df["lot_size"] = pd.to_numeric(df["lot_size"], errors="coerce").fillna(0).astype(int)
                df["strike_price"] = pd.to_numeric(df["strike_price"], errors="coerce").fillna(0.0)

                # Index for fast lookup
                self._index = df.set_index("instrument_key").to_dict("index")
                self._data = df
                self._loaded_at = datetime.utcnow()

                logger.info(f"Instrument registry loaded: {len(df)} instruments")

            except Exception as e:
                logger.critical(f"Failed to load instrument master: {e}")
                raise

    def _needs_refresh(self, file_path: str) -> bool:
        if not os.path.exists(file_path):
            return True

        age = datetime.utcnow() - datetime.utcfromtimestamp(os.path.getmtime(file_path))
        return age > timedelta(days=self.MAX_FILE_AGE_DAYS)

    def _download_master(self, file_path: str):
        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.get(self.MASTER_URL)
                resp.raise_for_status()
                with open(file_path, "wb") as f:
                    f.write(resp.content)
            logger.info("Instrument master download complete.")
        except Exception as e:
            logger.critical(f"Instrument master download failed: {e}")
            raise

    # --------------------------------------------------
    # LOOKUPS
    # --------------------------------------------------
    def get_instrument_details(self, instrument_key: str) -> Dict:
        if self._data is None:
            self.load_master()

        item = self._index.get(instrument_key)
        if not item:
            return {}

        return {
            "symbol": item.get("trading_symbol"),
            "strike": float(item.get("strike_price", 0.0)),
            "lot_size": int(item.get("lot_size", 0)),
            "expiry": item.get("expiry"),
            "name": item.get("name"),
        }

    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        if self._data is None:
            self.load_master()

        today = datetime.utcnow()
        target_names = {symbol, f"{symbol} 50", symbol.upper(), f"{symbol.upper()} 50"}

        df = self._data
        mask = (
            (df["exchange"] == "NSE")
            & (df["instrument_type"] == "FUT")
            & (df["name"].isin(target_names))
            & (df["expiry"] >= today)
        )

        futures = df.loc[mask].sort_values("expiry")
        if futures.empty:
            return None

        return futures.iloc[0]["instrument_key"]

    def get_option_symbols(self, underlying: str = "NIFTY") -> List[str]:
        if self._data is None:
            self.load_master()

        today = datetime.utcnow()
        target_names = {underlying, f"{underlying} 50", underlying.upper(), f"{underlying.upper()} 50"}

        df = self._data
        mask = (
            (df["exchange"] == "NSE")
            & (df["instrument_type"].isin(["CE", "PE"]))
            & (df["name"].isin(target_names))
            & (df["expiry"] >= today)
        )

        return df.loc[mask, "instrument_key"].tolist()


registry = InstrumentRegistry()
