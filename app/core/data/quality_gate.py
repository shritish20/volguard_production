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
    # FIX #6: Data Quality - ATM-Only Zero IV Check
    # ------------------------------------------------------------------
    def validate_structure(self, chain: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validates the Option Chain integrity.
        CRITICAL FIX: Focuses checks on ATM strikes to ignore OTM noise.
        """
        if chain is None or chain.empty:
            return False, "Empty Option Chain"

        # 1. Determine Spot Price
        # Get spot price from columns or estimate from strikes
        if 'spot' in chain.columns:
            spot = chain['spot'].iloc[0]
        else:
            # Estimate spot as middle strike if not provided (fallback)
            spot = chain['strike'].median()
        
        # 2. Filter ATM Strikes (±5% Range)
        # We only care about data quality where the liquidity matters most
        atm_chain = chain[
            (chain['strike'] >= spot * 0.95) & 
            (chain['strike'] <= spot * 1.05)
        ].copy()
        
        if atm_chain.empty:
            # If no strikes in 5% range, the chain is likely garbage or spot is wrong
            return False, "No ATM strikes found in chain (±5% range)"
        
        # 3. Zero IV Check (ATM-focused)
        if "ce_iv" in atm_chain.columns and "pe_iv" in atm_chain.columns:
            zero_ce_count = (atm_chain["ce_iv"] == 0).sum()
            zero_pe_count = (atm_chain["pe_iv"] == 0).sum()
            
            # 🔴 ANY zero IV in ATM range is critical as it breaks Greek calcs
            if zero_ce_count > 0 or zero_pe_count > 0:
                return False, f"Critical: {zero_ce_count} CE and {zero_pe_count} PE strikes have zero IV in ATM range"
            
            # Also check for absurdly low IV (<5%) which indicates bad data
            low_iv_ce = (atm_chain["ce_iv"] < 0.05).sum()
            low_iv_pe = (atm_chain["pe_iv"] < 0.05).sum()
            
            if low_iv_ce > 0 or low_iv_pe > 0:
                return False, f"Suspicious: {low_iv_ce} CE and {low_iv_pe} PE strikes have IV < 5% in ATM range"
        
        # 4. Strike Continuity Check (on full chain to ensure market depth)
        strikes = sorted(chain["strike"].unique())
        if len(strikes) > 2:
            diffs = np.diff(strikes)
            
            # Find the most common difference (e.g., 50 for Nifty)
            vals, counts = np.unique(diffs, return_counts=True)
            if len(vals) > 0:
                mode_diff = vals[np.argmax(counts)]
                
                # If we find a gap > 2.5x the normal gap, something is missing
                max_gap = diffs.max()
                if max_gap > mode_diff * 2.5:
                    return False, f"Significant gap detected: {max_gap} (expected ~{mode_diff})"
        
        # 5. Bid-Ask Sanity Check (if depth data available)
        if "ce_bid" in chain.columns and "ce_ask" in chain.columns:
            atm_with_quotes = atm_chain.dropna(subset=["ce_bid", "ce_ask", "pe_bid", "pe_ask"])
            
            if not atm_with_quotes.empty:
                # Check for crossed markets (bid > ask)
                crossed_ce = (atm_with_quotes["ce_bid"] > atm_with_quotes["ce_ask"]).sum()
                crossed_pe = (atm_with_quotes["pe_bid"] > atm_with_quotes["pe_ask"]).sum()
                
                if crossed_ce > 0 or crossed_pe > 0:
                    return False, f"Crossed market detected: {crossed_ce} CE, {crossed_pe} PE crosses"
                
                # Check for unreasonably wide spreads (>20% of mid)
                ce_mid = (atm_with_quotes["ce_bid"] + atm_with_quotes["ce_ask"]) / 2
                ce_mid = ce_mid.replace(0, 1) # Safety division
                ce_spread_pct = (atm_with_quotes["ce_ask"] - atm_with_quotes["ce_bid"]) / ce_mid
                
                if (ce_spread_pct > 0.20).any():
                    return False, "Excessive bid-ask spread detected (>20%)"

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
