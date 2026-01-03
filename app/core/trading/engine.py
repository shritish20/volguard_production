# app/core/trading/engine.py

import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

from app.core.market.data_client import MarketDataClient
from app.core.trading.strategy_selector import StrategySelector
from app.core.trading.leg_builder import LegBuilder
from app.schemas.analytics import RegimeResult, VolMetrics

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    TradingEngine is a pure ORCHESTRATOR.
    It never decides strategy logic.
    It never decides strikes.
    It performs final safety & sanity validation before execution.
    """

    def __init__(self, market_client: MarketDataClient, config: Dict):
        self.market = market_client
        self.config = config

        # System parameters
        self.min_dte = 2
        self.max_dte = 45

        # Lot size is instrument-specific and updated dynamically
        self.instrument_lot_size = config.get("DEFAULT_LOT_SIZE", 50)

        # Modules
        self.selector = StrategySelector()
        self.builder = LegBuilder()

    async def generate_entry_orders(
        self,
        regime: RegimeResult,
        vol_metrics: VolMetrics,
        snapshot: Dict
    ) -> List[Dict]:
        """
        Entry orchestration:
        Regime → Strategy → Chain → Legs → Validation
        """

        spot = snapshot.get("spot")
        if not spot:
            logger.warning("Spot price missing")
            return []

        # ----------------------------------------------------------
        # 1️⃣ STRATEGY SELECTION
        # ----------------------------------------------------------
        strategy = self.selector.select_strategy(regime, vol_metrics)
        if not strategy:
            logger.info(f"No strategy eligible for regime={regime.name}")
            return []

        logger.info(f"Selected strategy: {strategy.name}")

        # ----------------------------------------------------------
        # 2️⃣ EXPIRY & OPTION CHAIN
        # ----------------------------------------------------------
        expiry, chain = await self._get_best_expiry_chain()
        if chain is None or chain.empty:
            logger.warning("Option chain invalid or empty")
            return []

        # ----------------------------------------------------------
        # 3️⃣ POSITION SIZING (LOTS ONLY)
        # ----------------------------------------------------------
        # Regime.max_lots is a CEILING, not a command
        requested_lots = max(1, min(regime.max_lots, self._suggest_lots()))
        logger.info(f"Requested lots: {requested_lots}")

        # ----------------------------------------------------------
        # 4️⃣ LEG CONSTRUCTION
        # ----------------------------------------------------------
        orders = self.builder.build_legs(
            strategy=strategy,
            chain=chain,
            lots=requested_lots
        )

        # ----------------------------------------------------------
        # 5️⃣ ENGINE-LEVEL SAFETY VALIDATION
        # ----------------------------------------------------------
        if not self._validate_orders(orders, strategy):
            logger.error("Order validation failed at TradingEngine level")
            return []

        # ----------------------------------------------------------
        # 6️⃣ FINAL TAGGING
        # ----------------------------------------------------------
        for o in orders:
            o["expiry"] = expiry
            o["engine"] = "TradingEngine"

        return orders

    # ==========================================================
    # Internal helpers
    # ==========================================================

    async def _get_best_expiry_chain(self) -> tuple[Optional[str], Optional[pd.DataFrame]]:
        """
        Finds a valid expiry within DTE limits and fetches its option chain.
        """
        expiry, _, lot = await self.market.get_expiries_and_lot()
        if not expiry:
            return None, None

        try:
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
            dte = (expiry_dt - datetime.now()).days

            if dte < self.min_dte or dte > self.max_dte:
                logger.info(f"Expiry {expiry} rejected (DTE={dte})")
                return None, None

        except Exception:
            logger.exception("Expiry parsing failed")
            return None, None

        self.instrument_lot_size = lot
        chain = await self.market.get_option_chain(expiry)
        return expiry, chain

    def _suggest_lots(self) -> int:
        """
        Conservative default sizing logic.
        CapitalGovernor already enforced permission upstream.
        """
        return 1

    def _validate_orders(self, orders: List[Dict], strategy) -> bool:
        """
        Final sanity check before execution.
        This protects against upstream bugs.
        """

        if not orders or len(orders) < 2:
            logger.error("Invalid order set: insufficient legs")
            return False

        sell_legs = [o for o in orders if o["side"] == "SELL"]
        buy_legs = [o for o in orders if o["side"] == "BUY"]

        if not sell_legs:
            logger.error("No SELL legs found — invalid strategy output")
            return False

        if strategy.risk_type == "DEFINED" and not buy_legs:
            logger.error("Defined-risk strategy without hedges")
            return False

        for o in orders:
            if o["quantity"] <= 0:
                logger.error(f"Invalid quantity: {o}")
                return False

        return True
