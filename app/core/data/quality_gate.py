import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DataQualityGate:
    """
    Ensures market data is healthy before any trading decision is made.
    Prevents "Garbage In, Garbage Out".
    """
    def __init__(self):
        self.max_latency_seconds = 15  # Max allowed data age
        self.min_valid_greeks = 0.1    # Threshold to detect "Zero Greeks" bug

    def validate_snapshot(self, snapshot: Dict) -> Tuple[bool, str]:
        """
        Validates the Spot, VIX, and Timestamp of the snapshot.
        Returns: (is_valid, reason)
        """
        # 1. Check Completeness
        if not snapshot:
            return False, "Empty Snapshot"
            
        spot = snapshot.get("spot", 0)
        vix = snapshot.get("vix", 0)
        
        # 2. Zero Check
        if spot <= 0:
            return False, f"Invalid Spot Price: {spot}"
        if vix <= 0:
            return False, f"Invalid VIX: {vix}"

        # 3. Latency Check (If source provides timestamp)
        # Note: Upstox LTP doesn't always send timestamp, assuming near-realtime if fetched successfully
        # But if we had a 'last_updated' field, we would check it here.
        
        return True, "OK"

    def validate_structure(self, chain: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validates the Option Chain integrity.
        """
        if chain.empty:
            return False, "Empty Option Chain"
            
        # 1. Check for Missing Greeks (Common API failure)
        # If > 50% of rows have 0 IV, the chain is broken
        zero_iv_count = (chain['ce_iv'] == 0).sum() + (chain['pe_iv'] == 0).sum()
        total_legs = len(chain) * 2
        
        if zero_iv_count / total_legs > 0.5:
            return False, f"Data Quality Critical: {zero_iv_count}/{total_legs} legs have 0 IV"

        # 2. Strike Continuity
        # Strikes should be evenly spaced (e.g., 50 diff). 
        # Large gaps imply missing data packets.
        strikes = sorted(chain['strike'].unique())
        if len(strikes) > 2:
            diffs = np.diff(strikes)
            # Most common diff
            mode_diff = float(max(set(diffs), key=list(diffs).count))
            # Check for gaps > 2x the standard interval
            if np.any(diffs > mode_diff * 2.1):
                return False, "Data Quality Warning: Significant Gaps in Strike Prices detected"

        return True, "OK"

    def check_market_hours(self) -> bool:
        """Simple check to avoid trading outside hours"""
        now = datetime.now()
        # IST Hours approx 9:15 to 15:30
        start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        return start <= now <= end
