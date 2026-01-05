# app/core/trading/engine.py

import logging
from typing import Dict, List, Optional
from datetime import datetime, date

from app.config import settings
from app.services.instrument_registry import registry
from app.core.trading.leg_builder import LegBuilder
from app.core.trading.strategy_selector import StrategySelector

# EV & Capital Integration
from app.core.ev import TrueEVEngine, RawEdgeInputs, CapitalBucketEngine
from app.schemas.analytics import RegimeResult, VolMetrics

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    VolGuard Execution Engine (v3.1 + EV Core)
    ------------------------------------------
    Decision Flow:
    Regime Permission
        → Strategy Candidate
            → EV Go / No-Go
                → Capital Bucket Permission
                    → Lot Sizing
                        → Leg Construction
    """

    def __init__(self, market_client, config: Dict):
        self.client = market_client
        self.config = config

        # Core Components
        self.selector = StrategySelector()
        self.builder = LegBuilder()

        # EV & Capital Engines
        self.ev_engine = TrueEVEngine()
        self.bucket_engine = CapitalBucketEngine(
            total_capital=settings.BASE_CAPITAL
        )

    async def generate_entry_orders(
        self,
        regime: RegimeResult,
        vol: VolMetrics,
        snapshot: Dict
    ) -> List[Dict]:
        """
        Main decision pipeline called by Supervisor.
        Returns a list of order dictionaries (or empty list).
        """

        # -------------------------------------------------------------
        # STEP 1: REGIME PERMISSION
        # -------------------------------------------------------------
        self.bucket_engine.enforce_regime(regime.name)

        # Log active buckets for debugging
        active_buckets = {k: b.active for k, b in self.bucket_engine.buckets.items()}
        logger.debug(f"Bucket status: {active_buckets}")

        if regime.name == "CASH" or regime.alloc_pct <= 0:
            return []

        # -------------------------------------------------------------
        # STEP 2: STRATEGY SELECTION (THEORETICAL CANDIDATE)
        # -------------------------------------------------------------
        strategy_def = self.selector.select_strategy(regime, vol)

        if not strategy_def:
            logger.info(f"No strategy candidate for regime {regime.name}")
            return []

        # -------------------------------------------------------------
        # STEP 3: EV VALIDATION (GO / NO-GO)
        # -------------------------------------------------------------
        current_iv = snapshot.get("vix", 0.0)

        ev_inputs = RawEdgeInputs(
            atm_iv=current_iv,
            rv=vol.rv7,
            garch=vol.garch7,
            parkinson=vol.pk7,
            ivp=vol.ivp1y,
            fast_vol=snapshot.get("fast_vol", False)
        )

        ev_results = self.ev_engine.evaluate(
            raw=ev_inputs,
            regime=regime.name,
            expected_theta={
                strategy_def.name: 1000.0  # Conservative placeholder (V1 safe)
            }
        )

        # EV veto check
        if not ev_results:
            logger.info(
                f"⛔ EV blocked trade (negative expectancy) "
                f"for strategy {strategy_def.name}"
            )
            return []

        # Ensure selected strategy is actually in the EV-approved list
        if not any(ev.strategy == strategy_def.name for ev in ev_results):
            logger.info(f"⛔ Strategy {strategy_def.name} rejected by EV ranking")
            return []

        best_ev = ev_results[0]
        logger.info(
            f"✅ EV Passed | Strategy={best_ev.strategy} | "
            f"EV={best_ev.final_ev:.4f} | RawEdge={best_ev.raw_edge:.2f}"
        )

        # -------------------------------------------------------------
        # STEP 4: EXPIRY SELECTION
        # -------------------------------------------------------------
        # CRITICAL FIX: Use MarketClient to get LIVE chain data, not Registry
        chain = await self._get_best_expiry_chain(snapshot.get("symbol", "NIFTY"))

        if chain is None or chain.empty:
            logger.warning("No valid option chain found")
            return []

        # -------------------------------------------------------------
        # STEP 5: CAPITAL SIZING
        # -------------------------------------------------------------
        bucket_name = self._get_strategy_bucket(strategy_def.name)
        lots = self._calculate_bucket_lots(bucket_name, regime)

        if lots <= 0:
            logger.info(f"Insufficient capital in bucket '{bucket_name}'")
            return []

        # -------------------------------------------------------------
        # STEP 6: LEG CONSTRUCTION
        # -------------------------------------------------------------
        # Pass the LIVE chain to the builder so it can find liquid strikes
        legs = await self.builder.build_legs(
            strategy=strategy_def,
            chain=chain,
            lots=lots,
            market_client=self.client
        )

        if not legs:
            return []

        logger.info(
            f"🚀 Generated {len(legs)} legs | "
            f"Strategy={strategy_def.name} | Lots={lots}"
        )

        return legs

    # =============================================================
    # INTERNAL HELPERS
    # =============================================================

    def _get_strategy_bucket(self, strategy_name: str) -> str:
        """Maps strategies to capital buckets"""
        name = strategy_name.upper()

        if "INTRADAY" in name or "SCALP" in name:
            return "INTRADAY"
        if "IRON_CONDOR" in name or "FLY" in name:
            return "WEEKLY"
        if "POSITIONAL" in name or "CALENDAR" in name:
            return "MONTHLY"
        
        return "WEEKLY"

    def _calculate_bucket_lots(
        self,
        bucket_name: str,
        regime: RegimeResult
    ) -> int:
        """
        Conservative lot sizing using bucket + regime constraints.
        """
        try:
            bucket_capital = self.bucket_engine.get_bucket_capital(bucket_name)

            if bucket_capital <= 0:
                return 0

            # Global Limit (from Regime Alloc %)
            global_cap_limit = settings.BASE_CAPITAL * regime.alloc_pct

            # Use the tighter of the two constraints
            usable_capital = min(bucket_capital, global_cap_limit)

            # Conservative static margin (SELLING SAFETY)
            # TODO: Link to CapitalGovernor.predict_margin() in V3.2
            margin_per_lot = 150_000.0

            if margin_per_lot <= 0:
                return 0

            raw_lots = int(usable_capital / margin_per_lot)

            # Apply hard limits
            final_lots = max(
                0,
                min(
                    raw_lots,
                    regime.max_lots,
                    settings.REGIME_MAX_LOTS.get(regime.name, 0)
                )
            )

            return final_lots

        except Exception as e:
            logger.error(f"Lot sizing error: {e}")
            return 0

    async def _get_best_expiry_chain(self, symbol: str) -> Optional[object]:
        """
        Selects nearest valid expiry (2 <= DTE <= 45) and fetches 
        the LIVE option chain with Greeks.
        """
        try:
            # 1. Get Expiry Dates from Market Client (Dynamic)
            # Returns tuple (weekly_expiry_str, monthly_expiry_str)
            weekly, monthly = await self.client.get_expiries()
            
            if not weekly:
                return None

            today = date.today()
            candidates = []

            # Check Weekly
            if weekly:
                w_date = datetime.strptime(weekly, "%Y-%m-%d").date()
                w_dte = (w_date - today).days
                if 2 <= w_dte <= 45:
                    candidates.append((w_dte, weekly))
            
            # Check Monthly
            if monthly:
                m_date = datetime.strptime(monthly, "%Y-%m-%d").date()
                m_dte = (m_date - today).days
                if 2 <= m_dte <= 45:
                    candidates.append((m_dte, monthly))

            if not candidates:
                return None

            # Sort by DTE (Nearest first)
            candidates.sort(key=lambda x: x[0])
            best_expiry_str = candidates[0][1]

            # 2. Fetch the FULL CHAIN for this specific expiry
            # This returns the DataFrame with 'ce_iv', 'ce_delta', etc.
            chain_df = await self.client.get_option_chain(best_expiry_str)
            
            return chain_df

        except Exception as e:
            logger.error(f"Expiry selection/fetch failed: {e}")
            return None
