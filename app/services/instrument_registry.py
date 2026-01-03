import gzip
import json
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict
import logging
import os

logger = logging.getLogger(__name__)

class InstrumentRegistry:
    """
    Single source of truth for Instrument Keys.
    Parses 'data/complete.json.gz'.
    """
    _instance = None
    _data = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InstrumentRegistry, cls).__new__(cls)
        return cls._instance

    def load_master(self, file_path: str = "data/complete.json.gz"):
        if not os.path.exists(file_path):
            logger.error(f"Instrument master file not found at {file_path}")
            return
        try:
            logger.info("Loading instrument master...")
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
            
            self._data = pd.DataFrame(data)
            # CRITICAL FIX: Handle Millisecond Timestamp from your file
            if 'expiry' in self._data.columns:
                self._data['expiry'] = pd.to_datetime(self._data['expiry'], unit='ms', errors='coerce')
            
            logger.info(f"Loaded {len(self._data)} instruments.")
        except Exception as e:
            logger.critical(f"Failed to load instrument master: {e}")
            raise

    def get_instrument_details(self, instrument_key: str) -> Dict:
        """Fetch metadata (Strike, Expiry) for a token."""
        if self._data is None: self.load_master()
        try:
            row = self._data[self._data['instrument_key'] == instrument_key]
            if row.empty: return {}
            item = row.iloc[0]
            return {
                "symbol": item.get('trading_symbol'),
                "strike": float(item.get('strike_price', 0)),
                "lot_size": int(item.get('lot_size', 0)),
                "expiry": item.get('expiry'),
                "name": item.get('name')
            }
        except Exception:
            return {}

    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        """Get the active Future Token (replaces hardcoded placeholder)."""
        if self._data is None: self.load_master()
        today = datetime.now()
        # Robust Name Matching
        target_names = [symbol, symbol + " 50", symbol.upper(), symbol.upper() + " 50"]
        
        mask = (
            (self._data['exchange'] == 'NSE') &
            (self._data['instrument_type'] == 'FUT') & 
            (self._data['name'].isin(target_names)) & 
            (self._data['expiry'] >= today)
        )
        futures = self._data.loc[mask].sort_values('expiry')
        if futures.empty: return None
        return futures.iloc[0]['instrument_key']

    def get_option_symbols(self, underlying: str = "NIFTY") -> List[str]:
        """Get all option tokens for an underlying."""
        if self._data is None: self.load_master()
        today = datetime.now()
        target_names = [underlying, underlying + " 50", underlying.upper(), underlying.upper() + " 50"]
        mask = (
            (self._data['exchange'] == 'NSE') &
            (self._data['name'].isin(target_names)) &
            (self._data['instrument_type'].isin(['CE', 'PE'])) &
            (self._data['expiry'] >= today)
        )
        if mask.any(): return self._data.loc[mask, 'instrument_key'].tolist()
        return []

registry = InstrumentRegistry()
