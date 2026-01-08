from dataclasses import dataclass
import pandas as pd
import numpy as np
from typing import Tuple
from app.utils.logger import logger

@dataclass
class StructMetrics:
    net_gex: float
    gex_ratio: float
    total_oi_value: float
    gex_regime: str     # STICKY / SLIPPERY / NEUTRAL
    
    pcr: float
    max_pain: float
    skew_25d: float
    
    oi_regime: str      # BULLISH / BEARISH / NEUTRAL
    lot_size: int

class StructureEngine:
    """
    VolGuard 4.1 Structure Engine.
    Implements Net GEX, Max Pain, and Skew logic from v30.1.
    """

    def __init__(self):
        # Configuration from v30.1
        self.GEX_STICKY_RATIO = 0.03  # 3% Threshold
        self.PCR_BULLISH = 1.2
        self.PCR_BEARISH = 0.8

    def calculate_structure(self, 
                          chain: pd.DataFrame, 
                          spot: float, 
                          lot_size: int = 50) -> StructMetrics:
        try:
            if chain.empty or spot == 0:
                return self._get_fallback_structure(lot_size)

            # 1. Market Structure Analysis (GEX)
            # Filter for relevant strikes (Spot +/- 10%) to remove far OTM noise
            subset = chain[
                (chain['strike'] > spot * 0.90) & 
                (chain['strike'] < spot * 1.10)
            ].copy()

            if subset.empty:
                return self._get_fallback_structure(lot_size)

            # Calculate Net Gamma Exposure (in Rupee terms)
            # Formula: (Call Gamma - Put Gamma) * OI * Spot * LotSize
            net_gex = (
                (subset['ce_gamma'] * subset['ce_oi']).sum() - 
                (subset['pe_gamma'] * subset['pe_oi']).sum()
            ) * spot * lot_size

            # Total OI Value (to normalize GEX)
            total_oi_value = (chain['ce_oi'].sum() + chain['pe_oi'].sum()) * spot * lot_size
            
            # GEX Ratio (The "Stickiness" metric)
            gex_ratio = abs(net_gex) / total_oi_value if total_oi_value > 0 else 0

            # GEX Regime Classification
            if gex_ratio > self.GEX_STICKY_RATIO:
                gex_regime = "STICKY"  # Market likely to range-bound
            elif gex_ratio < (self.GEX_STICKY_RATIO * 0.5):
                gex_regime = "SLIPPERY" # Market prone to fast moves
            else:
                gex_regime = "NEUTRAL"

            # 2. Sentiment Analysis (PCR & Max Pain)
            pcr = chain['pe_oi'].sum() / chain['ce_oi'].sum() if chain['ce_oi'].sum() > 0 else 1.0
            max_pain = self._calculate_max_pain(chain)

            # 3. Skew Analysis (25 Delta)
            skew_25d = self._calculate_skew(chain)

            # 4. OI Regime Classification
            if pcr > self.PCR_BULLISH:
                oi_regime = "BULLISH"
            elif pcr < self.PCR_BEARISH:
                oi_regime = "BEARISH"
            else:
                oi_regime = "NEUTRAL"

            return StructMetrics(
                net_gex=net_gex,
                gex_ratio=gex_ratio,
                total_oi_value=total_oi_value,
                gex_regime=gex_regime,
                pcr=pcr,
                max_pain=max_pain,
                skew_25d=skew_25d,
                oi_regime=oi_regime,
                lot_size=lot_size
            )

        except Exception as e:
            logger.error(f"Structure Calculation Failed: {str(e)}")
            return self._get_fallback_structure(lot_size)

    def _calculate_max_pain(self, chain: pd.DataFrame) -> float:
        """
        Calculates the strike price where option writers would suffer the least loss.
        """
        strikes = chain['strike'].values
        ce_oi = chain['ce_oi'].values
        pe_oi = chain['pe_oi'].values
        
        # Vectorized calculation of loss at each strike
        # For each candidate strike (row), calculate loss against all other strikes (cols)
        # However, a simple loop is often faster for small arrays like option chains
        losses = []
        for strike in strikes:
            # If market expires at 'strike':
            # Call holders gain max(0, expiry - k)
            call_loss = np.sum(np.maximum(0, strike - strikes) * ce_oi)
            # Put holders gain max(0, k - expiry)
            put_loss = np.sum(np.maximum(0, strikes - strike) * pe_oi)
            losses.append(call_loss + put_loss)
            
        return strikes[np.argmin(losses)] if losses else 0.0

    def _calculate_skew(self, chain: pd.DataFrame) -> float:
        """
        Calculates 25-Delta Skew (Put IV - Call IV).
        Positive Skew = Bearish Sentiment (Puts are expensive).
        """
        try:
            # Find strike closest to 0.25 Delta for Calls and Puts
            ce_25d_idx = (chain['ce_delta'].abs() - 0.25).abs().argsort()[:1]
            pe_25d_idx = (chain['pe_delta'].abs() - 0.25).abs().argsort()[:1]
            
            if len(ce_25d_idx) > 0 and len(pe_25d_idx) > 0:
                pe_iv = chain.iloc[pe_25d_idx]['pe_iv'].values[0]
                ce_iv = chain.iloc[ce_25d_idx]['ce_iv'].values[0]
                return pe_iv - ce_iv
            return 0.0
        except:
            return 0.0

    def _get_fallback_structure(self, lot_size: int) -> StructMetrics:
        return StructMetrics(
            net_gex=0, gex_ratio=0, total_oi_value=0, gex_regime="NEUTRAL",
            pcr=1.0, max_pain=0, skew_25d=0, oi_regime="NEUTRAL", lot_size=lot_size
        )
