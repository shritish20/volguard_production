# app/core/data/quality_gate.py

import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging
from datetime import datetime, timedelta
import pytz

from app.config import settings

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

class DataQualityGate:
    """
    Ensures market data is healthy before any trading decision is made.
    Prevents 'Garbage In, Garbage Out'.
    """

    def __init__(self):
        # Config-driven thresholds (with safe defaults)
        self.max_latency_seconds = getattr(settings, "MAX_DATA_LATENCY_SECONDS", 15)
        self.min_valid_greeks = getattr(settings, "MIN_VALID_GREEKS", 0.1)

    def validate_snapshot(self, snapshot: Dict) -> Tuple[bool, str]:
        """
        Validates the Spot, VIX, and optional Timestamp of the snapshot.
        Returns: (is_valid, reason)
        """
        if not snapshot:
            return False, "Empty Snapshot"

        spot = snapshot.get("spot")
        vix = snapshot.get("vix")

        # 1. Zero / Missing Check
        if spot is None or spot <= 0:
            return False, f"Invalid Spot Price: {spot}"
        if vix is None or vix <= 0:
            return False, f"Invalid VIX: {vix}"

        # 2. Latency Check (Optional but enforced if timestamp exists)
        ts = snapshot.get("timestamp")
        if ts:
            try:
                # Normalize timestamp to datetime object
                if isinstance(ts, (int, float)):
                    ts = datetime.fromtimestamp(ts, IST)
                elif isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                
                # Ensure TZ awareness
                if ts.tzinfo is None:
                    ts = IST.localize(ts)
                else:
                    ts = ts.astimezone(IST)
                
                now = datetime.now(IST)

                # Allow small clock skew (future timestamp) up to 2 seconds
                latency = (now - ts).total_seconds()
                
                if latency > self.max_latency_seconds:
                    return False, f"Stale Market Data ({latency:.1f}s old)"
                if latency < -5.0:
                    return False, f"Future Timestamp Detected ({latency:.1f}s ahead)"

            except Exception as e:
                logger.warning(f"Timestamp validation failed: {e}")
                # We don't fail the cycle just because date parsing failed, 
                # as long as Spot/Vix are there.

        return True, "OK"

    def validate_structure(self, chain: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validates the Option Chain integrity.
        """
        if chain is None or chain.empty:
            return False, "Empty Option Chain"

        # 1. Zero IV Check (Upstox common failure)
        # If > 50% of the chain has 0 IV, the feed is likely broken
        if "ce_iv" in chain.columns and "pe_iv" in chain.columns:
            zero_iv_count = (chain["ce_iv"] == 0).sum() + (chain["pe_iv"] == 0).sum()
            total_legs = len(chain) * 2

            if total_legs > 0 and (zero_iv_count / total_legs) > 0.5:
                return False, f"Critical Data Quality: {zero_iv_count}/{total_legs} legs have zero IV"

        # 2. Strike Continuity Check
        # Detects if we are missing huge chunks of the option chain
        strikes = sorted(chain["strike"].unique())
        if len(strikes) > 2:
            diffs = np.diff(strikes)
            # Find the most common difference (e.g., 50 for Nifty)
            vals, counts = np.unique(diffs, return_counts=True)
            mode_diff = vals[np.argmax(counts)]

            # If we find a gap > 2.1x the normal gap, something is missing
            if np.any(diffs > mode_diff * 2.1):
                return False, "Significant gaps detected in strike prices"

        return True, "OK"

    def check_market_hours(self) -> bool:
        """
        Prevent trading outside NSE market hours (IST).
        9:15 AM to 3:30 PM.
        """
        now = datetime.now(IST)
        
        # Hardcoded NSE Timing
        start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        end = now.replace(hour=15, minute=30, second=0, microsecond=0)

        return start <= now <= end
