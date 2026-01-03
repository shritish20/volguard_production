import pandas as pd
import logging
from typing import List, Dict, Optional
from app.core.trading.strategies import StrategyDefinition

logger = logging.getLogger(__name__)

class LegBuilder:
    """
    Converts StrategyDefinitions into concrete Option Legs.
    Responsible for Strike Selection, Ratios, and Hedge Tagging.
    """
    
    def build_legs(self, 
                   strategy: StrategyDefinition, 
                   chain: pd.DataFrame, 
                   lot_size: int) -> List[Dict]:
        
        orders = []
        structure = strategy.structure
        
        # Helper to find strike by delta
        def find_leg(target_delta, leg_type):
            return self._select_strike_by_delta(chain, leg_type, target_delta)

        # ----------------------------------------
        # LOGIC: CONDOR / FLY / STRANGLE (Symmetrical-ish)
        # ----------------------------------------
        if structure in ["CONDOR", "FLY", "STRANGLE", "CONDOR_BWB"]:
            # 1. CORE LEGS (The Shorts)
            # Expecting [CallDelta, PutDelta] e.g. [0.25, -0.25]
            if len(strategy.core_deltas) >= 1:
                ce_core = find_leg(strategy.core_deltas[0], "CE")
                if ce_core:
                    orders.append(self._make_order(ce_core, "SELL", strategy.name, lot_size, is_hedge=False))
            
            if len(strategy.core_deltas) >= 2:
                pe_core = find_leg(strategy.core_deltas[1], "PE")
                if pe_core:
                    orders.append(self._make_order(pe_core, "SELL", strategy.name, lot_size, is_hedge=False))

            # 2. HEDGE LEGS (The Wings - Longs)
            # Expecting [CallHedge, PutHedge] e.g. [0.05, -0.05]
            if len(strategy.hedge_deltas) >= 1:
                ce_hedge = find_leg(strategy.hedge_deltas[0], "CE")
                if ce_hedge:
                    orders.append(self._make_order(ce_hedge, "BUY", strategy.name, lot_size, is_hedge=True))
            
            if len(strategy.hedge_deltas) >= 2:
                pe_hedge = find_leg(strategy.hedge_deltas[1], "PE")
                if pe_hedge:
                    orders.append(self._make_order(pe_hedge, "BUY", strategy.name, lot_size, is_hedge=True))

        # ----------------------------------------
        # LOGIC: SPREAD (Directional)
        # ----------------------------------------
        elif structure == "SPREAD":
            # Core is Short, Hedge is Long
            # Put Credit Spread: Core=[-0.25], Hedge=[-0.10]
            if strategy.core_deltas:
                core = find_leg(strategy.core_deltas[0], "PE" if strategy.core_deltas[0] < 0 else "CE")
                if core: orders.append(self._make_order(core, "SELL", strategy.name, lot_size, False))
            
            if strategy.hedge_deltas:
                hedge = find_leg(strategy.hedge_deltas[0], "PE" if strategy.hedge_deltas[0] < 0 else "CE")
                if hedge: orders.append(self._make_order(hedge, "BUY", strategy.name, lot_size, True))

        # ----------------------------------------
        # LOGIC: RATIO (Complex)
        # ----------------------------------------
        elif structure == "RATIO":
            # Example: 1x2 Put Spread
            # Core Deltas: [-0.30, -0.15] -> Buy 0.30, Sell 0.15
            # This logic requires specific mapping.
            # Convention for Ratio in this system: 
            # Index 0 = Long (The '1' in 1x2), Index 1 = Short (The '2' in 1x2)
            
            if len(strategy.core_deltas) >= 2:
                # Leg 1: Long (Buy)
                leg1 = find_leg(strategy.core_deltas[0], "PE" if strategy.core_deltas[0] < 0 else "CE")
                qty1 = lot_size * strategy.ratios[0]
                if leg1: orders.append(self._make_order(leg1, "BUY", strategy.name, qty1, False))
                
                # Leg 2: Short (Sell)
                leg2 = find_leg(strategy.core_deltas[1], "PE" if strategy.core_deltas[1] < 0 else "CE")
                qty2 = lot_size * strategy.ratios[1]
                if leg2: orders.append(self._make_order(leg2, "SELL", strategy.name, qty2, False))

        # ----------------------------------------
        # SORTING: HEDGE FIRST
        # ----------------------------------------
        # Ensure Buys (Hedges) come before Sells to margin benefit
        orders.sort(key=lambda x: 0 if x['side'] == 'BUY' else 1)
        
        return orders

    def _select_strike_by_delta(self, chain: pd.DataFrame, option_type: str, target_delta: float) -> Optional[Dict]:
        try:
            if option_type == 'CE':
                df = chain[['strike', 'ce_key', 'ce_delta']].copy()
                df.columns = ['strike', 'key', 'delta']
            else:
                df = chain[['strike', 'pe_key', 'pe_delta']].copy()
                df.columns = ['strike', 'key', 'delta']

            # Clean data
            df = df[(df['delta'] != 0) & (df['delta'].notna())]
            if df.empty: return None

            # Find closest
            df['diff'] = (df['delta'] - target_delta).abs()
            best = df.loc[df['diff'].idxmin()]
            
            return {
                "instrument_key": best['key'],
                "strike": best['strike'],
                "delta": best['delta']
            }
        except Exception as e:
            logger.error(f"Leg Selection Error: {e}")
            return None

    def _make_order(self, leg_data, side, tag, qty, is_hedge):
        return {
            "action": "ENTRY",
            "instrument_key": leg_data['instrument_key'],
            "quantity": int(qty),
            "side": side,
            "strategy": tag,
            "is_hedge": is_hedge,
            "reason": f"Delta {leg_data['delta']:.2f} ({'Hedge' if is_hedge else 'Core'})"
        }
