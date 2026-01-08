import pandas as pd
import httpx
import logging
import os
import gzip
import io
import json
from datetime import datetime, date
from typing import Dict, Optional, Tuple, Union

logger = logging.getLogger(__name__)

UPSTOX_MASTER_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
)

class InstrumentRegistry:
    """
    VolGuard Instrument Registry (AUTHORITATIVE)
    
    SINGLE SOURCE OF TRUTH FOR:
    - Expiries (Weekly / Monthly)
    - Lot Size / Tick Size
    - Instrument Existence

    ADAPTED FOR VOLGUARD 4.1:
    - Returns datetime.date objects (not strings) for DTE math.
    - Handles authoritative Upstox Master JSON.
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
            try:
                file_date = datetime.fromtimestamp(
                    os.path.getmtime(self.cache_file)
                ).date()
                if file_date == date.today():
                    # logger.info("📦 Loading instrument master from cache")
                    self.master_df = pd.read_json(self.cache_file)
                    self._normalize()
                    if not self.master_df.empty:
                        return
            except Exception as e:
                logger.warning(f"Cache load failed, forcing download: {e}")

        logger.info("⬇️ Downloading fresh Upstox instrument master...")
        try:
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
        except Exception as e:
            logger.critical(f"Failed to download Instrument Master: {e}")
            # If download fails, try to load old cache even if stale
            if os.path.exists(self.cache_file):
                logger.warning("Using stale cache due to download failure")
                self.master_df = pd.read_json(self.cache_file)
                self._normalize()

    # ------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------
    def _normalize(self) -> None:
        df = self.master_df
        if df.empty: return

        if "trading_symbol" in df.columns:
            df.rename(columns={"trading_symbol": "tradingsymbol"}, inplace=True)

        # Convert expiry from ms timestamp to datetime
        if "expiry" in df.columns:
            df["expiry"] = pd.to_datetime(df["expiry"], unit="ms", errors="coerce")
        
        self.master_df = df

    # ------------------------------------------------------------------
    # NIFTY EXPIRY RESOLUTION
    # ------------------------------------------------------------------
    def get_nifty_expiries(self) -> Tuple[Optional[date], Optional[date]]:
        """
        Returns:
            (weekly_expiry_date, monthly_expiry_date)
        
        CRITICAL CHANGE: Returns date objects, NOT strings.
        """
        if self.master_df.empty:
            self.load_master()
            if self.master_df.empty:
                return None, None

        today = pd.Timestamp.today().normalize()

        # Filter for NIFTY Options expiring in future
        opts = self.master_df[
            (self.master_df["segment"] == "NSE_FO") &
            (self.master_df["underlying_symbol"] == "NIFTY") &
            (self.master_df["instrument_type"].isin(["CE", "PE", "OPTIDX"])) &
            (self.master_df["expiry"] >= today)
        ].copy()

        if opts.empty:
            logger.error("No valid NIFTY option contracts found in master")
            return None, None

        # 1. Weekly Expiry
        # Try authoritative 'weekly' flag first
        if "weekly" in opts.columns:
             weeklies = opts[opts["weekly"] == True]
             if not weeklies.empty:
                 weekly_expiry = weeklies["expiry"].min().date()
             else:
                 # Fallback: Nearest expiry is weekly
                 weekly_expiry = opts["expiry"].min().date()
        else:
             weekly_expiry = opts["expiry"].min().date()

        # 2. Monthly Expiry
        # Try 'weekly' == False flag
        monthly_expiry = None
        if "weekly" in opts.columns:
            monthlies = opts[opts["weekly"] == False]
            if not monthlies.empty:
                monthly_expiry = monthlies["expiry"].min().date()
        
        # Fallback logic for Monthly if flag missing or ambiguous
        if not monthly_expiry:
            # Find the last expiry of the current month of the weekly expiry
            target_month = weekly_expiry.month
            target_year = weekly_expiry.year
            
            # Get all expiries for this month
            same_month_exps = opts[
                (opts["expiry"].dt.month == target_month) & 
                (opts["expiry"].dt.year == target_year)
            ]["expiry"].unique()
            
            if len(same_month_exps) > 0:
                monthly_expiry = pd.to_datetime(max(same_month_exps)).date()
            else:
                # If we are at end of month, get next month's monthly
                monthly_expiry = weekly_expiry # Fallback

        return weekly_expiry, monthly_expiry

    # ------------------------------------------------------------------
    # CONTRACT SPECS
    # ------------------------------------------------------------------
    def get_nifty_contract_specs(self, expiry: Union[str, date]) -> Dict:
        """
        Fetches structural specs. Accepts Date or String.
        """
        if self.master_df.empty:
            return {"lot_size": 50}

        # Normalize input to Timestamp for filtering
        if isinstance(expiry, date):
            expiry_dt = pd.Timestamp(expiry)
        else:
            expiry_dt = pd.to_datetime(expiry)

        df = self.master_df[
            (self.master_df["segment"] == "NSE_FO") &
            (self.master_df["underlying_symbol"] == "NIFTY") &
            (self.master_df["instrument_type"].isin(["CE", "PE", "OPTIDX"])) &
            (self.master_df["expiry"] == expiry_dt)
        ]

        if df.empty:
            # logger.warning(f"No contracts found for expiry {expiry}")
            return {"lot_size": 50, "tick_size": 0.05, "freeze_quantity": 1800}

        row = df.iloc[0]

        return {
            "lot_size": int(row.get("lot_size", 50)),
            "tick_size": float(row.get("tick_size", 0.05)),
            "freeze_quantity": int(row.get("freeze_quantity", 1800)),
        }


# GLOBAL SINGLETON
registry = InstrumentRegistry()
