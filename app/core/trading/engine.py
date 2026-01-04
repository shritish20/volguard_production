# app/core/trading/engine.py

import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from app.core.market.data_client import MarketDataClient
from app.core.trading.strategy_selector import StrategySelector
from app.core.trading.leg_builder import LegBuilder
from app.schemas.analytics import RegimeResult, VolMetrics

logger = logging.getLogger(__name__)

class TradingEngine:
    """
    VolGuard Smart Trading Engine (VolGuard 3.0)
    
    Orchestrator:
    1. Receives Market Regime & Vol Metrics
    2. Selects Optimal Strategy (StrategySelector)
    3. Fetches Option Chain (MarketDataClient V2)
    4. Builds & Validates Legs (LegBuilder + Liquidity Gate)
    """

    def __init__(self, market_client: MarketDataClient, config: Dict):
        self.market = market_client
        self.config = config
        
        # System constraints
        self.min_dte = 2   # Don't trade if expiry is < 2 days away (Gamma risk)
        self.max_dte = 45  # Don't trade excessively far out
        
        # Default Lot Size (Will be overridden by dynamic contract details)
        self.instrument_lot_size = config.get("DEFAULT_LOT_SIZE", 50)

        # Sub-components
        self.selector = StrategySelector()
        self.builder = LegBuilder()

    async def generate_entry_orders(
        self, 
        regime: RegimeResult, 
        vol_metrics: VolMetrics, 
        snapshot: Dict
    ) -> List[Dict]:
        """
        Main Entry Orchestration Workflow:
        Regime -> Strategy -> Expiry -> Chain -> Legs -> Validation -> Orders
        """
        spot = snapshot.get("spot")
        if not spot or spot <= 0:
            logger.warning("Spot price missing or invalid. Skipping entry generation.")
            return []

        # ==================================================================
        # 1. STRATEGY SELECTION
        # ==================================================================
        strategy = self.selector.select_strategy(regime, vol_metrics)
        if not strategy:
            # Common case: Regime implies CASH or no strategy fits strict filters
            return []
        
        logger.info(f"Selected Strategy: {strategy.name} (Regime: {regime.name})")

        # ==================================================================
        # 2. EXPIRY & CHAIN FETCHING
        # ==================================================================
        expiry, chain = await self._get_best_expiry_chain()
        
        if not expiry or chain is None or chain.empty:
            logger.warning("Failed to fetch valid expiry or option chain.")
            return []

        # ==================================================================
        # 3. POSITION SIZING
        # ==================================================================
        # Dynamic Lot Sizing: 
        # Regime.max_lots is a hard ceiling from the Regime Engine.
        # _suggest_lots() is a conservative starter (usually 1).
        # We take the minimum of constraints.
        
        requested_lots = max(1, min(regime.max_lots, self._suggest_lots()))
        logger.info(f"Building legs for {requested_lots} lots (Expiry: {expiry})")

        # ==================================================================
        # 4. LEG BUILDING (WITH LIQUIDITY CHECKS)
        # ==================================================================
        # CRITICAL: We pass self.market so builder can check Bid-Ask spreads
        orders = await self.builder.build_legs(
            strategy=strategy,
            chain=chain,
            lots=requested_lots,
            market_client=self.market
        )

        if not orders:
            logger.error(f"LegBuilder failed to generate valid legs for {strategy.name}")
            return []

        # ==================================================================
        # 5. ENGINE-LEVEL SAFETY VALIDATION
        # ==================================================================
        if not self._validate_orders(orders, strategy):
            logger.error("Order validation failed at TradingEngine level")
            return []

        # ==================================================================
        # 6. FINAL TAGGING
        # ==================================================================
        for o in orders:
            o["expiry"] = expiry
            o["engine"] = "TradingEngine"
            # Ensure price is 0.0 so Executor calculates Smart Limit
            if "price" not in o:
                o["price"] = 0.0 

        return orders

    # ==================================================================
    # INTERNAL HELPERS
    # ==================================================================

    async def _get_best_expiry_chain(self) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
        """
        Finds the best 'Weekly' expiry and fetches its chain.
        Filters for DTE limits.
        """
        # 1. Get filtered expiries from Market Client (V2)
        weekly, monthly = await self.market.get_expiries()
        
        if not weekly:
            return None, None
            
        target_expiry = weekly
        
        # 2. Validate DTE (Days to Expiry)
        try:
            expiry_dt = datetime.strptime(target_expiry, "%Y-%m-%d")
            dte = (expiry_dt - datetime.now()).days
            
            if dte < self.min_dte:
                logger.info(f"Weekly expiry {target_expiry} too close (DTE={dte} < {self.min_dte}). Skipping.")
                # Optional: Fallback to monthly if weekly is too close? 
                # For now, safe behavior is to stand aside.
                return None, None
                
            if dte > self.max_dte:
                 logger.info(f"Expiry {target_expiry} too far (DTE={dte}). Skipping.")
                 return None, None
                 
        except Exception as e:
            logger.error(f"Expiry DTE check failed: {e}")
            return None, None

        # 3. Fetch Chain
        chain = await self.market.get_option_chain(target_expiry)
        
        return target_expiry, chain

    def _suggest_lots(self) -> int:
        """
        Conservative default sizing.
        CapitalGovernor checks actual margin limits downstream.
        """
        return 1

    def _validate_orders(self, orders: List[Dict], strategy) -> bool:
        """
        Final sanity check before handing off to Supervisor.
        """
        if not orders or len(orders) < 2:
            logger.error("Invalid order set: insufficient legs")
            return False

        sell_legs = [o for o in orders if o["side"] == "SELL"]
        buy_legs = [o for o in orders if o["side"] == "BUY"]

        if not sell_legs:
            logger.error("No SELL legs found - strategy must have premium collection")
            return False

        if strategy.risk_type == "DEFINED" and not buy_legs:
            logger.error("Defined-risk strategy generated without hedges")
            return False

        for o in orders:
            if o["quantity"] <= 0:
                logger.error(f"Invalid quantity: {o['quantity']}")
                return False

        return True
