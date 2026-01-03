# app/core/trading/leg_builder.py

import pandas as pd
import logging
from typing import List, Dict, Optional

from app.core.trading.strategies import StrategyDefinition

logger = logging.getLogger(__name__)


class LegBuilder:
    """
    LegBuilder converts an abstract StrategyDefinition
    into concrete option orders.

    HARD RULES:
    - Accepts LOTS, never quantities
    - Converts LOTS -> quantity exactly once
    - Never decides risk, size, or strategy
    """

    def build_legs(
        self,
        strategy: StrategyDefinition,
        chain: pd.DataFrame,
        lots: int
    ) -> List[Dict]:

        if lots <= 0:
            logger.error("Invalid lots passed to LegBuilder")
            return []

        orders: List[Dict] = []

        # ======================================================
        # Helpers
        # ======================================================
        def pick(delta: float, opt_type: str) -> Optional[Dict]:
            return self._select_strike_by_delta(chain, delta, opt_type)

        def add_order(leg, side, ratio=1, is_hedge=False):
            if not leg:
                return
            orders.append({
                "action": "ENTRY",
                "instrument_key": leg["instrument_key"],
                "side": side,
                "quantity": int(lots * ratio),
                "strategy": strategy.name,
                "is_hedge": is_hedge,
                "reason": f"Δ {leg['delta']:.2f} ({'HEDGE' if is_hedge else 'CORE'})"
            })

        structure = strategy.structure

        # ======================================================
        # CONDOR / STRANGLE / FLY
        # ======================================================
        if structure in ("CONDOR", "STRANGLE", "FLY"):

            # --- CORE (Short)
            ce_core = pick(strategy.core_deltas[0], "CE")
            pe_core = pick(strategy.core_deltas[1], "PE")

            add_order(ce_core, "SELL")
            add_order(pe_core, "SELL")

            # --- HEDGES (Long)
            if strategy.risk_type == "DEFINED":
                ce_hedge = pick(strategy.hedge_deltas[0], "CE")
                pe_hedge = pick(strategy.hedge_deltas[1], "PE")

                add_order(ce_hedge, "BUY", is_hedge=True)
                add_order(pe_hedge, "BUY", is_hedge=True)

        # ======================================================
        # BROKEN WING CONDOR (ASYMMETRIC)
        # ======================================================
        elif structure == "CONDOR_BWB":

            # Core shorts
            ce_core = pick(strategy.core_deltas[0], "CE")
            pe_core = pick(strategy.core_deltas[1], "PE")

            add_order(ce_core, "SELL")
            add_order(pe_core, "SELL")

            # Asymmetric wings
            ce_hedge = pick(strategy.hedge_deltas[0], "CE")
            pe_hedge = pick(strategy.hedge_deltas[1], "PE")

            # One wing closer, one further (true BWB)
            add_order(ce_hedge, "BUY", is_hedge=True)
            add_order(pe_hedge, "BUY", is_hedge=True)

        # ======================================================
        # VERTICAL SPREAD
        # ======================================================
        elif structure == "SPREAD":

            core_delta = strategy.core_deltas[0]
            hedge_delta = strategy.hedge_deltas[0]

            opt_type = "PE" if core_delta < 0 else "CE"

            core = pick(core_delta, opt_type)
            hedge = pick(hedge_delta, opt_type)

            add_order(core, "SELL")
            add_order(hedge, "BUY", is_hedge=True)

        # ======================================================
        # RATIO SPREAD (1x2 etc.)
        # ======================================================
        elif structure == "RATIO":

            # Convention:
            # ratios = [long_ratio, short_ratio]
            long_delta, short_delta = strategy.core_deltas
            long_ratio, short_ratio = strategy.ratios

            opt_type = "PE" if long_delta < 0 else "CE"

            long_leg = pick(long_delta, opt_type)
            short_leg = pick(short_delta, opt_type)

            add_order(long_leg, "BUY", ratio=long_ratio)
            add_order(short_leg, "SELL", ratio=short_ratio)

        else:
            logger.error(f"Unknown strategy structure: {structure}")
            return []

        # ======================================================
        # FINAL SANITY CHECK
        # ======================================================
        if not self._validate_structure(orders, strategy):
            logger.error("LegBuilder validation failed")
            return []

        # BUY first for margin benefit
        orders.sort(key=lambda o: 0 if o["side"] == "BUY" else 1)

        return orders

    # ==========================================================
    # Internal helpers
    # ==========================================================

    def _select_strike_by_delta(
        self,
        chain: pd.DataFrame,
        target_delta: float,
        option_type: str
    ) -> Optional[Dict]:

        try:
            if option_type == "CE":
                df = chain[["strike", "ce_key", "ce_delta"]].rename(
                    columns={"ce_key": "key", "ce_delta": "delta"}
                )
            else:
                df = chain[["strike", "pe_key", "pe_delta"]].rename(
                    columns={"pe_key": "key", "pe_delta": "delta"}
                )

            df = df[df["delta"].notna() & (df["delta"] != 0)]
            if df.empty:
                return None

            df["diff"] = (df["delta"] - target_delta).abs()
            best = df.loc[df["diff"].idxmin()]

            return {
                "instrument_key": best["key"],
                "strike": best["strike"],
                "delta": best["delta"]
            }

        except Exception as e:
            logger.exception("Strike selection failed")
            return None

    def _validate_structure(
        self,
        orders: List[Dict],
        strategy: StrategyDefinition
    ) -> bool:

        if len(orders) < 2:
            return False

        sells = [o for o in orders if o["side"] == "SELL"]
        buys = [o for o in orders if o["side"] == "BUY"]

        if not sells:
            return False

        if strategy.risk_type == "DEFINED" and not buys:
            return False

        for o in orders:
            if o["quantity"] <= 0:
                return False

        return True
