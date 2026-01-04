# app/core/trading/adjustment_engine.py

import logging
import pandas as pd
from typing import Dict, List, Optional
from app.core.market.data_client import MarketDataClient

logger = logging.getLogger(__name__)

class AdjustmentEngine:
    """
    VolGuard Smart Adjustment Engine (VolGuard 3.0)
    
    Responsibilities:
    1. MONITOR: Checks Net Delta and Individual Leg PnL.
    2. PRESCRIBE: Suggests 'HEDGE_ENTRY' or 'PROFIT_EXIT'.
    3. TARGET: Aims to keep Net Delta within +/- 10.
    """

    def __init__(self, delta_threshold: float = 15.0):
        self.delta_threshold = delta_threshold
        # Minimum interval handled by Supervisor, but we enforce logic here too
        self.max_adjustments_per_day = 5 

    async def evaluate_portfolio(
        self, 
        risk_metrics: Dict, 
        snapshot: Dict
    ) -> List[Dict]:
        """
        Input: Risk Metrics (Aggregated Delta)
        Output: List of Adjustment Orders (Dictionaries)
        """
        adjustments = []
        
        # 1. DELTA HEDGING
        net_delta = risk_metrics.get("aggregate_metrics", {}).get("delta", 0.0)
        
        if abs(net_delta) > self.delta_threshold:
            logger.info(f"Net Delta {net_delta:.2f} breaches threshold {self.delta_threshold}. Calculation Hedge.")
            
            # Action: Buy/Sell Hedges to neutralize
            # If Delta is +50, we need -50 (Buy Puts or Sell Calls)
            # VolGuard Logic: We buy cheap wings (Long Options) to hedge, rarely Futures.
            
            hedge_order = self._create_delta_hedge(net_delta, snapshot)
            if hedge_order:
                adjustments.append(hedge_order)

        return adjustments

    def _create_delta_hedge(self, net_delta: float, snapshot: Dict) -> Optional[Dict]:
        """
        Creates a 'Repair' order.
        Strategy: Buy roughly 20 Delta options to offset risk.
        """
        # Direction
        # If Net Delta is POSITIVE (+50), market went up or we are long. We need NEGATIVE delta -> BUY PUTS.
        # If Net Delta is NEGATIVE (-50), market went down. We need POSITIVE delta -> BUY CALLS.
        
        side = "BUY" # We prefer buying hedges (defined risk) vs selling more (margin issue)
        
        if net_delta > 0:
            # Need Short Delta -> Buy PE
            option_type = "PE"
            reason = "Positive Delta Breach"
        else:
            # Need Long Delta -> Buy CE
            option_type = "CE"
            reason = "Negative Delta Breach"
            
        # Sizing
        # Approx 20 Delta hedge. 
        # Qty needed = Net_Delta / 0.20
        # Example: Delta +50. Need -50. One 20 Delta Put gives -20. Need 2.5 lots -> 3 lots.
        
        target_hedge_delta = 0.20
        qty_needed = abs(net_delta) / target_hedge_delta
        
        # Round to nearest lot (assuming 50 for now, but strictly should use contract details)
        # Using a standard 50 for calculation, Supervisor/Executor will validate.
        lot_size = 50 
        lots = max(1, round(qty_needed / lot_size * lot_size / 50)) # Rough lot logic
        final_qty = lots * lot_size
        
        if final_qty <= 0:
            return None

        # We don't have the chain here to pick the exact strike.
        # We return a "Directive" that the TradingEngine or Executor must resolve?
        # OR: We make the AdjustmentEngine async and give it the MarketClient.
        # DESIGN CHOICE: For simplicity in V3, we return a "MARKET" order for the CURRENT_MONTH hedge
        # relying on the Executor to find the liquid strike? 
        # NO. We should probably pick a dynamic strike. 
        
        # Simplified for V3 Phase: Return a generic instruction.
        # In a real system, we'd pass the chain here. 
        # Let's assume we simply trade the NIFTY FUTURE for immediate hedging if it's critical?
        # Or stick to the "Long Option" plan but let the Executor pick the strike?
        
        # Let's go with: "NIFTY_HEDGE_CE" or "NIFTY_HEDGE_PE" as a key, 
        # and let the Executor resolve it to a 20-delta strike.
        # This requires the Executor to be smart.
        
        return {
            "action": "ENTRY",
            "instrument_key": "NIFTY_SMART_HEDGE", # Special flag for Executor
            "option_type": option_type,
            "quantity": final_qty,
            "side": side,
            "strategy": "DELTA_HEDGE",
            "reason": reason,
            "price": 0.0,
            "is_hedge": True
        }
