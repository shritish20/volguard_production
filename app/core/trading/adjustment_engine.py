# app/core/trading/adjustment_engine.py

import logging
import time
from typing import List, Dict
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)


class AdjustmentEngine:
    """
    LAST LINE OF DEFENSE.
    This engine exists ONLY to prevent catastrophic portfolio risk.
    It must NEVER fight strategy intent or regime logic.
    """

    def __init__(self, config: Dict):
        self.max_net_delta = config.get("MAX_NET_DELTA", 0.40)

        # Only hedge at SEVERE breach
        self.severe_multiplier = 1.75

        # Anti-whipsaw
        self.last_adjustment_time = 0
        self.min_adjustment_interval = 300  # seconds

    # --------------------------------------------------
    # PORTFOLIO EVALUATION
    # --------------------------------------------------
    async def evaluate_portfolio(
        self,
        portfolio_risk: Dict,
        market_snapshot: Dict,
        regime: str = "NEUTRAL",
        open_positions: List[Dict] = None,
    ) -> List[Dict]:

        adjustments = []
        open_positions = open_positions or []

        metrics = portfolio_risk.get("aggregate_metrics", {})
        net_delta = metrics.get("delta", 0.0)

        # --------------------------------------------------
        # 1️⃣ NOTHING TO DO IF NO POSITIONS
        # --------------------------------------------------
        if not open_positions:
            return []

        # --------------------------------------------------
        # 2️⃣ REGIME RESPECT
        # --------------------------------------------------
        if regime in ["LONG_VOL", "CASH", "STAY_AWAY"]:
            return []

        # --------------------------------------------------
        # 3️⃣ STRATEGY STRUCTURE CHECK
        # If all positions are defined-risk, DO NOT hedge
        # --------------------------------------------------
        has_undefined_risk = any(
            not p.get("is_hedge", False) and p.get("strategy") in ["SHORT_STRANGLE", "RATIO_PUT_SPREAD"]
            for p in open_positions
        )

        if not has_undefined_risk:
            return []

        # --------------------------------------------------
        # 4️⃣ COOLDOWN CHECK
        # --------------------------------------------------
        if time.time() - self.last_adjustment_time < self.min_adjustment_interval:
            return []

        # --------------------------------------------------
        # 5️⃣ SEVERE DELTA BREACH ONLY
        # --------------------------------------------------
        severe_limit = self.max_net_delta * self.severe_multiplier

        if abs(net_delta) < severe_limit:
            return []

        logger.critical(
            f"SEVERE DELTA BREACH: {net_delta:.2f} | Limit: {severe_limit:.2f}"
        )

        # --------------------------------------------------
        # 6️⃣ FUTURE-BASED EMERGENCY HEDGE
        # --------------------------------------------------
        fut_key = registry.get_current_future("NIFTY")
        if not fut_key:
            logger.error("Emergency hedge failed: No NIFTY future available")
            return []

        details = registry.get_instrument_details(fut_key)
        lot_size = details.get("lot_size", 50)

        # Snap to nearest lot
        target_qty = -net_delta
        lots = round(target_qty / lot_size)
        qty = abs(lots * lot_size)

        if qty <= 0:
            return []

        side = "BUY" if lots > 0 else "SELL"

        adjustments.append(
            {
                "action": "ENTRY",
                "instrument_key": fut_key,
                "quantity": qty,
                "side": side,
                "strategy": "HEDGE",
                "reason": f"Emergency delta hedge | Net Δ={net_delta:.2f}",
                "is_hedge": True,
            }
        )

        self.last_adjustment_time = time.time()
        return adjustments

    # --------------------------------------------------
    # PLACEHOLDER (INTENTIONAL)
    # --------------------------------------------------
    async def evaluate_trade(self, trade, risk, snap):
        return []
