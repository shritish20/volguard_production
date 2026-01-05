# app/core/trading/engine.py

import logging
from typing import Dict, List, Optional
from datetime import datetime, date

from app.config import settings
from app.core.trading.leg_builder import LegBuilder
from app.core.trading.strategy_selector import StrategySelector

# EV & Capital Integration
from app.core.ev import TrueEVEngine, RawEdgeInputs, CapitalBucketEngine
from app.schemas.analytics import RegimeResult, VolMetrics

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    VolGuard Execution Engine (v3.1 + EV Core)
    Decision Flow: Regime -> Strategy -> EV Check -> Capital Bucket -> Legs
    """

    def __init__(self, market_client, config: Dict):
        self.client = market_client
        self.config = config

        self.selector = StrategySelector()
        self.builder = LegBuilder()

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

        # 1. REGIME PERMISSION
        self.bucket_engine.enforce_regime(regime.name)

        logger.debug(
            f"Bucket status: "
            f"{ {k: b.active for k, b in self.bucket_engine.buckets.items()} }"
        )

        if regime.name == "CASH" or regime.alloc_pct <= 0:
            return []

        # 2. STRATEGY SELECTION
        strategy_def = self.selector.select_strategy(regime, vol)
        if not strategy_def:
            logger.info(f"No strategy candidate for regime {regime.name}")
            return []

        # 3. EV VALIDATION
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
            expected_theta={strategy_def.name: 1000.0}
        )

        if not ev_results:
            logger.info(f"⛔ EV blocked trade for {strategy_def.name}")
            return []

        if not any(ev.strategy == strategy_def.name for ev in ev_results):
            logger.info(f"⛔ Strategy {strategy_def.name} rejected by EV ranking")
            return []

        best_ev = ev_results[0]
        logger.info(f"✅ EV Passed | EV={best_ev.final_ev:.4f}")

        # 4. EXPIRY SELECTION (LIVE)
        chain = await self._get_best_expiry_chain(snapshot.get("symbol", "NIFTY"))
        if chain is None or chain.empty:
            logger.warning("No valid option chain found")
            return []

        # 5. CAPITAL SIZING
        bucket_name = self._get_strategy_bucket(strategy_def.name)
        lots = self._calculate_bucket_lots(bucket_name, regime)

        if lots <= 0:
            logger.info(f"Insufficient capital in bucket '{bucket_name}'")
            return []

        # 6. LEG CONSTRUCTION
        legs = await self.builder.build_legs(
            strategy=strategy_def,
            chain=chain,
            lots=lots,
            market_client=self.client
        )

        if legs:
            logger.info(
                f"🚀 Generated {len(legs)} legs | "
                f"Strategy={strategy_def.name} | Lots={lots}"
            )

        return legs or []

    # =============================================================

    def _get_strategy_bucket(self, strategy_name: str) -> str:
        name = strategy_name.upper()
        if "INTRADAY" in name or "SCALP" in name:
            return "INTRADAY"
        if "IRON_CONDOR" in name or "FLY" in name:
            return "WEEKLY"
        if "POSITIONAL" in name or "CALENDAR" in name:
            return "MONTHLY"
        return "WEEKLY"

    def _calculate_bucket_lots(self, bucket_name: str, regime: RegimeResult) -> int:
        try:
            bucket_capital = self.bucket_engine.get_bucket_capital(bucket_name)
            if bucket_capital <= 0:
                return 0

            # Global cap from Regime
            global_cap_limit = settings.BASE_CAPITAL * regime.alloc_pct
            
            # Use strict minimum of bucket vs global
            usable_capital = min(bucket_capital, global_cap_limit)

            # Conservative margin estimate (approx 1.5L for selling)
            margin_per_lot = 150_000.0
            
            raw_lots = int(usable_capital / margin_per_lot)

            # FIX: Removed settings.REGIME_MAX_LOTS dependency to prevent
            # default=0 from blocking all trades.
            # We rely on regime.max_lots which is calculated dynamically.
            return max(
                0,
                min(raw_lots, regime.max_lots)
            )

        except Exception as e:
            logger.error(f"Lot sizing error: {e}")
            return 0

    async def _get_best_expiry_chain(self, symbol: str) -> Optional[object]:
        try:
            weekly, monthly = await self.client.get_expiries()
            if not weekly:
                return None

            today = date.today()
            candidates = []

            for exp_str in [weekly, monthly]:
                if not exp_str:
                    continue
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                    if 2 <= dte <= 45:
                        candidates.append((dte, exp_str))
                except ValueError:
                    continue

            if not candidates:
                return None

            candidates.sort(key=lambda x: x[0])
            # Fetch LIVE chain data
            return await self.client.get_option_chain(candidates[0][1])

        except Exception as e:
            logger.error(f"Expiry fetch failed: {e}")
            return None
