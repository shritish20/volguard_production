# app/core/trading/leg_builder.py

import pandas as pd
import logging
import asyncio
from typing import List, Dict, Optional
from app.core.trading.strategies import StrategyDefinition
from app.core.market.data_client import MarketDataClient

logger = logging.getLogger(__name__)

class LegBuilder:
    """
    VolGuard Smart Leg Builder (VolGuard 3.0)
    
    Responsibility:
    - Converts Abstract Strategy (e.g. "Sell 20 Delta Strangle") -> Concrete Orders
    - LIQUIDITY GATE: Checks Bid-Ask spread before selecting a strike.
    - RETRY LOGIC: Falls back to next best delta if primary strike is illiquid.
    """

    async def build_legs(
        self, 
        strategy: StrategyDefinition, 
        chain: pd.DataFrame, 
        lots: int,
        market_client: MarketDataClient
    ) -> List[Dict]:
        """
        Constructs strategy legs with real-time liquidity validation.
        """
        if lots <= 0:
            logger.error("Invalid lots passed to LegBuilder")
            return []

        if chain.empty:
            logger.error("Empty option chain provided to LegBuilder")
            return []

        orders: List[Dict] = []

        # ==================================================================
        # 1. SMART PICKER HELPER
        # ==================================================================
        async def pick(target_delta: float, opt_type: str, strict: bool = True) -> Optional[Dict]:
            """
            Finds best liquid strike near target delta.
            Iterates through top 3 candidates to find one with good spread.
            """
            candidates = self._get_candidates_by_delta(chain, target_delta, opt_type)
            
            if not candidates:
                logger.warning(f"No strikes found for Delta {target_delta} ({opt_type})")
                return None

            # Check top 3 candidates for liquidity
            for i, cand in enumerate(candidates[:3]):
                key = cand["key"]
                
                # Check Liquidity via V2 Quote API (Real-time)
                depth = await market_client.get_quote_depth(key)
                
                if depth.get("liquid", False):
                    # Found a good one!
                    if i > 0:
                        logger.info(f"Skipped illiquid primary strike. Selected candidate #{i+1} for Delta {target_delta}")
                    return cand
                else:
                    logger.debug(f"Strike {cand['strike']} rejected: {depth.get('reason', 'High Spread')} (Spread: {depth.get('spread')})")
            
            # If all candidates fail
            if strict:
                logger.error(f"Failed to find ANY liquid strike for Delta {target_delta}")
                return None
            else:
                # If not strict, return the best delta match anyway (Risky, usually for hedges)
                logger.warning(f"Forced selection of potentially illiquid strike for Delta {target_delta}")
                return candidates[0]

        # ==================================================================
        # 2. ORDER CONSTRUCTION HELPER
        # ==================================================================
        def add_order(leg, side, ratio=1, is_hedge=False):
            if not leg:
                return
            
            # Smart Sizing: lots * ratio
            qty = int(lots * ratio)
            
            orders.append({
                "action": "ENTRY",
                "instrument_key": leg["key"],
                "strike": leg["strike"],
                "option_type": "CE" if "CE" in str(leg["key"]) else "PE",
                "side": side,
                "quantity": qty,
                "strategy": strategy.name,
                "is_hedge": is_hedge,
                "reason": f"Delta {leg['delta']:.2f} ({'HEDGE' if is_hedge else 'CORE'})",
                "price": 0.0 # Will be resolved to Smart Limit in Executor
            })

        # ==================================================================
        # 3. STRATEGY MAPPING
        # ==================================================================
        structure = strategy.structure

        try:
            # --- CONDOR / STRANGLE / FLY ---
            if structure in ("CONDOR", "STRANGLE", "FLY"):
                # A. Core Legs (Short) - STRICT Liquidity
                ce_core = await pick(strategy.core_deltas[0], "CE", strict=True)
                pe_core = await pick(strategy.core_deltas[1], "PE", strict=True)
                
                if not ce_core or not pe_core:
                    logger.error("Aborting Strategy: Core legs illiquid")
                    return []

                add_order(ce_core, "SELL")
                add_order(pe_core, "SELL")

                # B. Hedges (Long) - Can be slightly looser if needed, but safer to be strict
                if strategy.risk_type == "DEFINED":
                    ce_hedge = await pick(strategy.hedge_deltas[0], "CE", strict=True)
                    pe_hedge = await pick(strategy.hedge_deltas[1], "PE", strict=True)
                    
                    if not ce_hedge or not pe_hedge:
                        logger.error("Aborting Strategy: Hedge legs illiquid")
                        return []

                    add_order(ce_hedge, "BUY", is_hedge=True)
                    add_order(pe_hedge, "BUY", is_hedge=True)

            # --- BROKEN WING CONDOR (ASYMMETRIC) ---
            elif structure == "CONDOR_BWB":
                # Core shorts
                ce_core = await pick(strategy.core_deltas[0], "CE")
                pe_core = await pick(strategy.core_deltas[1], "PE")
                
                if not ce_core or not pe_core: return []

                add_order(ce_core, "SELL")
                add_order(pe_core, "SELL")

                # Asymmetric wings
                ce_hedge = await pick(strategy.hedge_deltas[0], "CE")
                pe_hedge = await pick(strategy.hedge_deltas[1], "PE")
                
                if not ce_hedge or not pe_hedge: return []

                add_order(ce_hedge, "BUY", is_hedge=True)
                add_order(pe_hedge, "BUY", is_hedge=True)

            # --- VERTICAL SPREAD ---
            elif structure == "SPREAD":
                core_delta = strategy.core_deltas[0]
                hedge_delta = strategy.hedge_deltas[0]
                opt_type = "PE" if core_delta < 0 else "CE"

                core = await pick(core_delta, opt_type)
                hedge = await pick(hedge_delta, opt_type)
                
                if not core or not hedge: return []

                add_order(core, "SELL")
                add_order(hedge, "BUY", is_hedge=True)

            # --- RATIO SPREAD ---
            elif structure == "RATIO":
                # ratios = [long_ratio, short_ratio]
                long_delta = strategy.core_deltas[0]  # Usually the closer leg (bought)
                short_delta = strategy.core_deltas[1] # Further leg (sold)
                
                long_ratio = strategy.ratios[0]
                short_ratio = strategy.ratios[1]
                
                opt_type = "PE" if long_delta < 0 else "CE"

                long_leg = await pick(long_delta, opt_type)
                short_leg = await pick(short_delta, opt_type)

                if not long_leg or not short_leg: return []

                add_order(long_leg, "BUY", ratio=long_ratio)
                add_order(short_leg, "SELL", ratio=short_ratio)

            else:
                logger.error(f"Unknown strategy structure: {structure}")
                return []

        except Exception as e:
            logger.exception(f"Leg building crashed: {e}")
            return []

        # ==================================================================
        # 4. FINAL STRUCTURAL VALIDATION
        # ==================================================================
        if not self._validate_structure(orders, strategy):
            logger.error("LegBuilder Validation Failed: Incomplete Strategy Structure")
            return []

        # Sort: BUY first (Margin Benefit)
        orders.sort(key=lambda o: 0 if o["side"] == "BUY" else 1)

        return orders

    def _get_candidates_by_delta(self, chain: pd.DataFrame, target_delta: float, option_type: str) -> List[Dict]:
        """
        Returns a list of candidates sorted by closeness to target delta.
        """
        try:
            # 1. Filter by Option Type
            if option_type == "CE":
                df = chain[["strike", "ce_key", "ce_delta"]].rename(
                    columns={"ce_key": "key", "ce_delta": "delta"}
                )
            else:
                df = chain[["strike", "pe_key", "pe_delta"]].rename(
                    columns={"pe_key": "key", "pe_delta": "delta"}
                )

            # 2. Filter Valid Deltas
            df = df[df["delta"].notna() & (df["delta"] != 0)]
            if df.empty:
                return []

            # 3. Calculate Diff and Sort
            df["diff"] = (df["delta"] - target_delta).abs()
            df_sorted = df.sort_values("diff")

            # 4. Convert to List of Dicts
            candidates = []
            for _, row in df_sorted.head(5).iterrows(): # Return top 5 matches
                candidates.append({
                    "key": row["key"],
                    "strike": row["strike"],
                    "delta": row["delta"]
                })
            
            return candidates

        except Exception as e:
            logger.exception("Candidate selection failed")
            return []

    def _validate_structure(self, orders: List[Dict], strategy: StrategyDefinition) -> bool:
        """
        Ensures we haven't built a 'crippled' strategy (e.g., Short legs without Long hedges).
        """
        if len(orders) < 2:
            return False

        sells = [o for o in orders if o["side"] == "SELL"]
        buys = [o for o in orders if o["side"] == "BUY"]

        # Must have shorts (otherwise why trade?)
        if not sells:
            return False

        # If Defined Risk, MUST have buys
        if strategy.risk_type == "DEFINED" and not buys:
            logger.critical("Defined Risk strategy generated without Hedges!")
            return False
            
        # Quantity sanity check
        for o in orders:
            if o["quantity"] <= 0:
                return False

        return True
