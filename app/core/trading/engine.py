import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class TradingEngine:
    def __init__(self, market_client, config: Dict):
        self.market = market_client
        self.config = config
        self.lot_size = config.get("MAX_POSITION_SIZE", 50) # Default to Nifty Lot
        
        # Strategy Parameters
        self.target_delta = 0.25  # Sell 25 Delta (Strangle)
        self.min_dte = 2          # Don't enter if < 2 days to expiry
        self.max_dte = 45         # Don't enter if > 45 days

    async def generate_entry_orders(self, regime: Dict, snapshot: Dict) -> List[Dict]:
        """
        Generates entry orders based on the current market regime.
        """
        orders = []
        regime_name = regime.get("name", "NEUTRAL")
        
        # 1. Filter Regimes: Only trade if Short Volatility is favorable
        if regime_name not in ["AGGRESSIVE_SHORT", "MODERATE_SHORT"]:
            logger.info(f"Regime {regime_name} - No entries generated.")
            return []

        spot = snapshot.get("spot", 0)
        if spot == 0: return []

        # 2. Get Option Chain for nearest valid expiry
        expiry_date, chain_df = await self._get_best_expiry_chain()
        if chain_df.empty:
            return []

        logger.info(f"Generating Entry for Expiry: {expiry_date} (Spot: {spot})")

        # 3. STRATEGY: SHORT STRANGLE
        # Sell OTM Put (Delta ~ -0.25) & Sell OTM Call (Delta ~ 0.25)
        
        pe_leg = self._select_strike_by_delta(chain_df, 'PE', -self.target_delta)
        ce_leg = self._select_strike_by_delta(chain_df, 'CE', self.target_delta)

        if pe_leg:
            orders.append(self._create_order(pe_leg, "SELL", "SHORT_STRANGLE_PUT", regime))
        
        if ce_leg:
            orders.append(self._create_order(ce_leg, "SELL", "SHORT_STRANGLE_CALL", regime))

        return orders

    async def _get_best_expiry_chain(self):
        """Finds the current month or next week expiry."""
        start_date, end_date, lot = await self.market.get_expiries_and_lot()
        if not start_date: 
            return None, pd.DataFrame()
        
        # Validate DTE (Gamma Risk & Liquidity)
        try:
            expiry_dt = datetime.strptime(start_date, "%Y-%m-%d")
            today = datetime.now()
            dte = (expiry_dt - today).days
            
            if dte < self.min_dte:
                logger.warning(f"Expiry {start_date} has {dte} DTE (Min {self.min_dte}). Skipping.")
                return None, pd.DataFrame()
            
            if dte > self.max_dte:
                return None, pd.DataFrame()

        except Exception as e:
            logger.error(f"DTE Calc Error: {e}")
            return None, pd.DataFrame()

        # Update internal lot size from API
        self.lot_size = lot
        
        chain = await self.market.get_option_chain(start_date)
        return start_date, chain

    def _select_strike_by_delta(self, chain: pd.DataFrame, option_type: str, target_delta: float) -> Optional[Dict]:
        """
        Finds the strike with delta closest to target.
        """
        try:
            # Filter for Option Type (CE/PE)
            if option_type == 'CE':
                df = chain[['strike', 'ce_key', 'ce_delta', 'ce_iv']].copy()
                df.columns = ['strike', 'key', 'delta', 'iv']
            else:
                df = chain[['strike', 'pe_key', 'pe_delta', 'pe_iv']].copy()
                df.columns = ['strike', 'key', 'delta', 'iv']

            # Filter valid deltas (clean bad data)
            df = df[(df['delta'] != 0) & (df['delta'].notna())]
            
            if df.empty: return None

            # Find row with minimum absolute difference from target delta
            df['diff'] = (df['delta'] - target_delta).abs()
            best_row = df.loc[df['diff'].idxmin()]
            
            return {
                "instrument_key": best_row['key'],
                "strike": best_row['strike'],
                "delta": best_row['delta'],
                "iv": best_row['iv']
            }
        except Exception as e:
            logger.error(f"Strike Selection Error: {e}")
            return None

    def _create_order(self, leg_data: Dict, side: str, strategy_tag: str, regime: Dict) -> Dict:
        # Scale position by Regime Confidence
        max_lots = regime.get('max_lots', 1)
        qty = self.lot_size * max_lots

        return {
            "action": "ENTRY",
            "instrument_key": leg_data['instrument_key'],
            "quantity": qty, 
            "side": side,
            "strategy": strategy_tag,
            "reason": f"Entry Delta {leg_data['delta']:.2f} (Strike {leg_data['strike']}, {max_lots} Lots)"
        }
