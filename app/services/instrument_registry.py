# app/services/instrument_registry.py

import pandas as pd
import httpx
import logging
import os
import gzip
import io
import json
from datetime import datetime, date
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class InstrumentRegistry:
    """
    VolGuard Smart Registry (VolGuard 3.0)
    
    Responsibilities:
    1. MASTER DATA: Downloads & Caches Upstox Contract Master (Verified JSON).
    2. RESOLUTION: Converts Symbols <-> Tokens.
    3. METADATA: Provides Lot Size, Tick Size, Freeze Limits.
    4. DYNAMIC CONTRACTS: Finds 'Current Month Future' automatically.
    """

    def __init__(self, cache_file: str = "instruments_cache.json"):
        self.cache_file = cache_file
        self.master_df = pd.DataFrame()
        self.symbol_map = {} 
        self.token_map = {}  
        self.last_update = None

    def load_master(self, force_refresh: bool = False):
        """
        Loads master contract file. 
        Tries local cache first, else downloads from Upstox.
        """
        # 1. Try Local Cache
        if not force_refresh and os.path.exists(self.cache_file):
            file_time = datetime.fromtimestamp(os.path.getmtime(self.cache_file)).date()
            if file_time == date.today():
                logger.info("Loading instruments from local cache...")
                try:
                    self.master_df = pd.read_json(self.cache_file)
                    # JSON serialization turns timestamps to ints, need to convert back if loading from cache
                    if 'expiry' in self.master_df.columns:
                         self.master_df['expiry'] = pd.to_datetime(self.master_df['expiry'], unit='ms', errors='ignore')
                    self._build_maps()
                    return
                except Exception as e:
                    logger.warning(f"Cache corrupt, forcing download: {e}")

        # 2. Download Fresh
        logger.info("Downloading fresh instrument master from Upstox (JSON)...")
        try:
            url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
            
            with httpx.Client() as client:
                resp = client.get(url, timeout=60.0)
                resp.raise_for_status()
                
                with gzip.open(io.BytesIO(resp.content), 'rt', encoding='utf-8') as f:
                    data = json.load(f)
            
            # 3. Process Data
            temp_df = pd.DataFrame(data)
            
            # Normalization (Handle naming variations)
            if 'trading_symbol' in temp_df.columns:
                temp_df.rename(columns={'trading_symbol': 'tradingsymbol'}, inplace=True)
            
            # 4. Filter for Equity Derivatives (NSE_FO) & Nifty Indices
            # Note: Upstox JSON 'segment' is usually "NSE_FO" for F&O
            # We also filter for NIFTY/BANKNIFTY to keep RAM usage low (~10k rows vs 80k)
            self.master_df = temp_df[
                (temp_df['segment'] == 'NSE_FO') & 
                (temp_df['tradingsymbol'].str.contains('NIFTY|BANKNIFTY|FINNIFTY', case=False, na=False))
            ].copy()
            
            # 5. Fix Expiry (Critical: Convert ms timestamp to datetime)
            if 'expiry' in self.master_df.columns:
                # Upstox sends expiry as 1774463399000 (ms)
                self.master_df['expiry'] = pd.to_datetime(self.master_df['expiry'], unit='ms')
            
            # Save to cache
            self.master_df.to_json(self.cache_file, index=False)
            self._build_maps()
            logger.info(f"Instrument Master Loaded: {len(self.master_df)} contracts")
            
        except Exception as e:
            logger.critical(f"Failed to download Instrument Master: {e}")
            # Fallback
            if os.path.exists(self.cache_file):
                logger.warning("Using stale cache due to download failure.")
                self.master_df = pd.read_json(self.cache_file)
                self._build_maps()
            else:
                raise RuntimeError("Instrument Registry Failed: No Data Available")

    def _build_maps(self):
        """Indexing for O(1) lookups"""
        if self.master_df.empty:
            return

        self.token_map = self.master_df.set_index('instrument_key').to_dict('index')
        self.symbol_map = dict(zip(self.master_df['tradingsymbol'], self.master_df['instrument_key']))

    def get_instrument_details(self, instrument_key: str) -> Dict:
        return self.token_map.get(instrument_key, {})

    def get_token_by_symbol(self, symbol: str) -> Optional[str]:
        return self.symbol_map.get(symbol)

    def get_current_future(self, underlying: str = "NIFTY") -> Optional[str]:
        """
        Smart Resolution: Finds the current month's Future token.
        """
        try:
            # Filter for Futures of the underlying
            futs = self.master_df[
                (self.master_df['instrument_type'] == 'FUT') & 
                (self.master_df['tradingsymbol'].str.startswith(underlying))
            ].copy()
            
            if futs.empty:
                return None
            
            # Sort by expiry
            futs = futs.sort_values('expiry')
            
            # Find first expiry >= Today
            today = pd.Timestamp.now().normalize()
            valid_futs = futs[futs['expiry'] >= today]
            
            if valid_futs.empty:
                return None
                
            return valid_futs.iloc[0]['instrument_key']
            
        except Exception as e:
            logger.error(f"Future resolution failed: {e}")
            return None

# Global Singleton Instance
registry = InstrumentRegistry()
