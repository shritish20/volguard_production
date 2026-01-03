import logging
import time
from typing import List, Dict
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

class AdjustmentEngine:
    def __init__(self, config: Dict):
        self.max_net_delta = config.get("MAX_NET_DELTA", 0.40)
        self.max_daily_loss = config.get("MAX_DAILY_LOSS", 20000)
        
        # Anti-Whipsaw
        self.last_adjustment_time = 0
        self.min_adjustment_interval = 300 
        self.delta_buffer = 0.05

    async def evaluate_portfolio(self, portfolio_risk: Dict, market_snapshot: Dict) -> List[Dict]:
        adjustments = []
        metrics = portfolio_risk.get("aggregate_metrics", {})
        current_delta = metrics.get("delta", 0.0)

        # 1. Cool-down Check
        time_since_last = time.time() - self.last_adjustment_time
        if time_since_last < self.min_adjustment_interval:
            # Exception: Critical Breach (> 2x limit)
            if abs(current_delta) < (self.max_net_delta * 2):
                return []

        # 2. Delta Threshold Logic
        threshold = self.max_net_delta + self.delta_buffer
        
        if abs(current_delta) > threshold:
            logger.warning(f"Delta Breach: {current_delta:.2f} (Limit: {self.max_net_delta})")

            fut_key = registry.get_current_future("NIFTY")
            if not fut_key:
                logger.error("Cannot Hedge: No Future found")
                return []
            
            details = registry.get_instrument_details(fut_key)
            lot_size = details.get('lot_size', 50)
            
            # 3. SMART HEDGING LOGIC: Snap to nearest lot
            target_qty = -current_delta
            lots_needed = round(target_qty / lot_size)
            qty_needed = abs(lots_needed * lot_size)
            
            if qty_needed == 0:
                return [] 

            side = "BUY" if lots_needed > 0 else "SELL"
            
            # 4. Construct Order
            adjustments.append({
                "action": "DELTA_HEDGE",
                "instrument_key": fut_key,
                "quantity": qty_needed,
                "side": side,
                "strategy": "HEDGE",
                "reason": f"Delta {current_delta:.2f} exceeds limit. Hedging {qty_needed} qty."
            })
            
            self.last_adjustment_time = time.time()

        return adjustments

    async def evaluate_trade(self, trade, risk, snap):
        return []
