# app/core/data/quality_gate.py

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
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

    # ------------------------------------------------------------------
    # FIX #7: Data Quality - Wider ATM Range (10% instead of 5%)
    # ------------------------------------------------------------------
    def validate_structure(self, chain: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validates the Option Chain integrity.
        FIXED: Uses wider ATM range for realistic market conditions.
        """
        if chain is None or chain.empty:
            return False, "Empty Option Chain"
        
        # 1. Determine Spot Price
        if 'spot' in chain.columns:
            spot = chain['spot'].iloc[0]
        else:
            # Estimate spot as middle strike (fallback)
            spot = chain['strike'].median()
        
        # ✅ FIX: Widen ATM range to ±10% for realistic coverage
        # At NIFTY 21500, this gives us 19350 to 23650 (80+ strikes)
        atm_lower = spot * 0.90
        atm_upper = spot * 1.10
        
        atm_chain = chain[
            (chain['strike'] >= atm_lower) &
            (chain['strike'] <= atm_upper)
        ].copy()
        
        if atm_chain.empty:
            return False, f"No ATM strikes found in ±10% range (spot: {spot})"
        
        # Log coverage for monitoring
        logger.debug(f"ATM chain coverage: {len(atm_chain)} strikes in range {atm_lower}-{atm_upper}")
        
        # 2. Zero IV Check (ATM-focused)
        if "ce_iv" in atm_chain.columns and "pe_iv" in atm_chain.columns:
            zero_ce_count = (atm_chain["ce_iv"] == 0).sum()
            zero_pe_count = (atm_chain["pe_iv"] == 0).sum()
            
            # Allow up to 5% zero IVs in ATM range (deep OTM can be zero)
            max_allowed_zeros = int(len(atm_chain) * 0.05)
            
            if zero_ce_count > max_allowed_zeros or zero_pe_count > max_allowed_zeros:
                return False, f"Too many zero IVs: {zero_ce_count} CE, {zero_pe_count} PE (max allowed: {max_allowed_zeros})"
            
            # Check for absurdly low IV (<3%) in ATM strikes only
            # Calculate true ATM (±2%)
            true_atm = atm_chain[
                (atm_chain['strike'] >= spot * 0.98) &
                (atm_chain['strike'] <= spot * 1.02)
            ]
            
            if not true_atm.empty:
                low_iv_ce = (true_atm["ce_iv"] < 0.03).sum()
                low_iv_pe = (true_atm["pe_iv"] < 0.03).sum()
                
                if low_iv_ce > 0 or low_iv_pe > 0:
                    return False, f"Suspicious: {low_iv_ce} CE and {low_iv_pe} PE strikes have IV < 3% in true ATM range"
        
        # 3. Strike Continuity Check
        strikes = sorted(chain["strike"].unique())
        if len(strikes) > 2:
            diffs = np.diff(strikes)
            vals, counts = np.unique(diffs, return_counts=True)
            
            if len(vals) > 0:
                mode_diff = vals[np.argmax(counts)]
                max_gap = diffs.max()
                
                # Allow gap up to 3x normal (e.g., 150 when normal is 50)
                if max_gap > mode_diff * 3.0:
                    return False, f"Significant gap detected: {max_gap} (expected ~{mode_diff})"
        
        # 4. Bid-Ask Sanity Check (if available)
        if "ce_bid" in chain.columns and "ce_ask" in chain.columns:
            atm_with_quotes = atm_chain.dropna(subset=["ce_bid", "ce_ask", "pe_bid", "pe_ask"])
            
            if not atm_with_quotes.empty:
                # Check for crossed markets
                crossed_ce = (atm_with_quotes["ce_bid"] > atm_with_quotes["ce_ask"]).sum()
                crossed_pe = (atm_with_quotes["pe_bid"] > atm_with_quotes["pe_ask"]).sum()
                
                if crossed_ce > 0 or crossed_pe > 0:
                    return False, f"Crossed market detected: {crossed_ce} CE, {crossed_pe} PE crosses"
                
                # Check for unreasonably wide spreads (>30% of mid, relaxed from 20%)
                ce_mid = (atm_with_quotes["ce_bid"] + atm_with_quotes["ce_ask"]) / 2
                ce_mid = ce_mid.replace(0, 1)  # Safety
                ce_spread_pct = (atm_with_quotes["ce_ask"] - atm_with_quotes["ce_bid"]) / ce_mid
                
                if (ce_spread_pct > 0.30).any():
                    return False, "Excessive bid-ask spread detected (>30%)"
        
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
