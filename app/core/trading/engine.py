import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
import logging
from app.services.instrument_registry import registry
from app.core.market.data_client import MarketDataClient

logger = logging.getLogger(__name__)

class TradingEngine:
    """
    Converts 'AGGRESSIVE_SHORT' -> 'Sell 21500 CE'.
    """
    def __init__(self, market_client: MarketDataClient, config: Dict):
        self.market_client = market_client
        self.base_capital = config.get("BASE_CAPITAL", 1000000)

    async def generate_entry_orders(self, regime: Dict, market_snapshot: Dict) -> List[Dict]:
        orders = []
        regime_name = regime.get("name", "NEUTRAL")
        
        if regime_name == "AGGRESSIVE_SHORT":
            # Strategy: Sell 25 Delta Strangle
            expiry_date = await self._get_nearest_weekly_expiry()
            if not expiry_date: return []

            # Fetch Chain
            chain = await self.market_client.get_option_chain(expiry_date)
            if chain.empty: return []

            # Select Strikes
            call_leg = self._find_strike_by_delta(chain, 0.25, "CE")
            put_leg = self._find_strike_by_delta(chain, 0.25, "PE")

            if call_leg: orders.append(self._create_order_packet(call_leg, "SELL", "STRANGLE"))
            if put_leg: orders.append(self._create_order_packet(put_leg, "SELL", "STRANGLE"))

        return orders

    def _create_order_packet(self, leg_data: Dict, side: str, strategy_tag: str) -> Dict:
        key = leg_data['instrument_key']
        # Dynamic Lot Size
        details = registry.get_instrument_details(key)
        lot_size = details.get('lot_size', 0)
        if lot_size == 0: lot_size = 25 # Fallback
        
        return {
            "instrument_key": key,
            "quantity": lot_size,
            "side": side,
            "strategy": strategy_tag,
            "strike": leg_data['strike'],
            "reason": f"Delta {leg_data['delta']:.2f}"
        }

    def _find_strike_by_delta(self, chain: pd.DataFrame, target: float, type: str) -> Optional[Dict]:
        try:
            if type == "CE":
                chain['diff'] = abs(chain['ce_delta'].abs() - target)
                best = chain.sort_values('diff').iloc[0]
                return {"instrument_key": best['ce_key'], "strike": best['strike'], "delta": best['ce_delta']}
            else:
                chain['diff'] = abs(chain['pe_delta'].abs() - target)
                best = chain.sort_values('diff').iloc[0]
                return {"instrument_key": best['pe_key'], "strike": best['strike'], "delta": best['pe_delta']}
        except:
            return None

    async def _get_nearest_weekly_expiry(self):
        # Uses Registry to find next expiry date string YYYY-MM-DD
        if registry._data is None: registry.load_master()
        today = datetime.now()
        df = registry._data
        mask = (df['name'] == 'NIFTY') & (df['instrument_type'].isin(['CE', 'PE'])) & (df['expiry'] >= today)
        valid = df.loc[mask].sort_values('expiry')
        if valid.empty: return None
        return valid.iloc[0]['expiry'].strftime("%Y-%m-%d")
