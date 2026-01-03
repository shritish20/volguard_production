import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict

from app.core.market.data_client import MarketDataClient
from app.core.trading.strategy_selector import StrategySelector
from app.core.trading.leg_builder import LegBuilder
from app.schemas.analytics import RegimeResult, VolMetrics

logger = logging.getLogger(__name__)

class TradingEngine:
    def __init__(self, market_client: MarketDataClient, config: Dict):
        self.market = market_client
        self.config = config
        self.base_capital = config.get("BASE_CAPITAL", 1000000)
        self.lot_size = config.get("MAX_POSITION_SIZE", 50) 
        
        # New Modules
        self.selector = StrategySelector()
        self.builder = LegBuilder()
        
        # Safety
        self.min_dte = 2
        self.max_dte = 45

    async def generate_entry_orders(self, 
                                  regime: RegimeResult, 
                                  vol_metrics: VolMetrics,
                                  snapshot: Dict) -> List[Dict]:
        """
        ORCHESTRATOR:
        1. Select Strategy (StrategySelector)
        2. Get Option Chain
        3. Build Legs (LegBuilder)
        """
        
        spot = snapshot.get("spot", 0)
        if spot == 0: return []

        # 1. Strategy Selection
        selected_strategy = self.selector.select_strategy(regime, vol_metrics)
        
        if not selected_strategy:
            return [] # No strategy fits this regime/risk profile

        logger.info(f"Generating orders for: {selected_strategy.name}")

        # 2. Data Fetching (Expiry & Chain)
        expiry_date, chain_df = await self._get_best_expiry_chain()
        if chain_df.empty:
            logger.warning("No valid option chain found.")
            return []

        # 3. Leg Construction
        # Calculate dynamic lot size based on regime allocation
        # Regime max_lots is authoritative
        alloc_lots = regime.max_lots
        if alloc_lots < 1: alloc_lots = 1
        
        trade_qty = self.lot_size * alloc_lots
        
        orders = self.builder.build_legs(
            strategy=selected_strategy,
            chain=chain_df,
            lot_size=trade_qty
        )
        
        # 4. Final Tagging
        for order in orders:
            order['expiry'] = expiry_date
            
        return orders

    async def _get_best_expiry_chain(self):
        """Finds valid expiry and fetches chain"""
        start_date, end_date, lot = await self.market.get_expiries_and_lot()
        if not start_date: return None, pd.DataFrame()
        
        # DTE Check
        try:
            expiry_dt = datetime.strptime(start_date, "%Y-%m-%d")
            dte = (expiry_dt - datetime.now()).days
            
            if dte < self.min_dte:
                logger.warning(f"Expiry {start_date} too close ({dte} DTE). Skipping.")
                return None, pd.DataFrame()
            if dte > self.max_dte:
                return None, pd.DataFrame()
                
        except Exception:
            return None, pd.DataFrame()

        self.lot_size = lot
        chain = await self.market.get_option_chain(start_date)
        return start_date, chain
