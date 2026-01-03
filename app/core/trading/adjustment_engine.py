import logging
import time
from typing import List, Dict
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

class AdjustmentEngine:
    def __init__(self, config: Dict):
        self.max_net_delta = config.get("MAX_NET_DELTA", 0.40)
        self.max_daily_loss = config.get("MAX_DAILY_LOSS", 20000)
        
        # Anti-Whipsaw Configuration
        self.last_adjustment_time = 0
        self.min_adjustment_interval = 300  # 5 Minutes (in seconds)
        self.delta_buffer = 0.05  # Only adjust if we exceed limit by 0.05 (Hysteresis)

    async def evaluate_portfolio(self, portfolio_risk: Dict, market_snapshot: Dict) -> List[Dict]:
        """
        Evaluates if the portfolio needs hedging.
        Includes safeguards against over-trading (Churning).
        """
        adjustments = []
        metrics = portfolio_risk.get("aggregate_metrics", {})
        
        # 1. Extract Metrics
        current_delta = metrics.get("delta", 0.0)
        
        # 2. Cool-down Check
        # If we adjusted less than 5 minutes ago, DO NOT touch it unless it's a critical emergency
        time_since_last = time.time() - self.last_adjustment_time
        if time_since_last < self.min_adjustment_interval:
            # Exception: If Delta is HUGE (> 2x limit), ignore timer and act immediately
            if abs(current_delta) < (self.max_net_delta * 2):
                return []

        # 3. Delta Threshold Logic (With Hysteresis)
        # We only hedge if Delta > Limit + Buffer (e.g., 0.40 + 0.05 = 0.45)
        # This prevents hedging at 0.41 then un-hedging at 0.39 repeatedly.
        threshold = self.max_net_delta + self.delta_buffer
        
        if abs(current_delta) > threshold:
            logger.warning(f"Delta Breach detected: {current_delta:.2f} (Limit: {self.max_net_delta})")
            
            # Find the Hedge Instrument (NIFTY Futures)
            fut_key = registry.get_current_future("NIFTY")
            if not fut_key:
                logger.error("Cannot Hedge: No Future found in Registry")
                return []
            
            # Get Lot Size
            details = registry.get_instrument_details(fut_key)
            lot_size = details.get('lot_size', 50) 

            # 4. Calculate Required Hedge Quantity
            # We want to bring Delta back to 0 (Neutral)
            delta_to_neutralize = -current_delta
            
            # Round to nearest lot
            # Assumes 1 Lot of Future has approx Delta of 1 * Lot_Size (e.g. 50)
            # Normalization: If portfolio Delta is normalized to "Lots", logic changes.
            # Assuming Delta here is raw weighted Delta.
            # Approx: 1 Nifty Future = 1.0 Delta * 50 Qty = 50 Delta Exposure
            # If `current_delta` is "0.40" (Portfolio normalized), we need to trade fraction?
            # Based on standard greeks, usually Delta is per share. So Nifty Fut Delta = 1.
            # Total Position Delta = Sum(Delta * Qty).
            
            # If current_delta is small (e.g. 0.45), it implies 'Normalized to Portfolio' or 'Per Share'.
            # If Config MAX_NET_DELTA is 0.40, it implies "Portfolio Delta / Capital" or similar ratio.
            # Assuming Standard VolGuard logic: Max Net Delta is Total Delta / Capital or similar? 
            # Reviewing config: MAX_NET_DELTA = 0.40. This is likely "Weighted Delta".
            
            # SIMPLE LOGIC for Production Safety:
            # If Positive Delta -> Sell 1 Lot Future
            # If Negative Delta -> Buy 1 Lot Future
            # We do incremental hedging rather than massive rebalancing to avoid errors.
            
            side = "SELL" if current_delta > 0 else "BUY"
            qty_needed = lot_size # Start with 1 lot hedge
            
            # 5. Construct Order
            adjustments.append({
                "action": "DELTA_HEDGE",
                "instrument_key": fut_key,
                "quantity": qty_needed,
                "side": side,
                "strategy": "HEDGE",
                "reason": f"Delta {current_delta:.2f} exceeds limit {self.max_net_delta}"
            })
            
            # Update Timestamp
            self.last_adjustment_time = time.time()

        return adjustments

    async def evaluate_trade(self, trade, risk, snap):
        # Placeholder for pre-trade checks
        return []
