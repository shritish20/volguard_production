import logging
from typing import List, Dict
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)

class AdjustmentEngine:
    """
    The 'Tactician'. 
    Evaluates risk metrics and suggests adjustments (Hedges/Closes).
    """
    def __init__(self, config: Dict):
        self.max_net_delta = config.get("MAX_NET_DELTA", 0.40)
        self.max_daily_loss = config.get("MAX_DAILY_LOSS", 20000)
        self.margin_sell = config.get("MARGIN_SELL", 120000)
        
    async def evaluate_portfolio(self, portfolio_risk: Dict, market_snapshot: Dict) -> List[Dict]:
        """
        Check global portfolio limits (Delta, Vega, Loss) and suggest hedges.
        """
        adjustments = []
        metrics = portfolio_risk.get("aggregate_metrics", {})
        
        # 1. Delta Hedging Logic
        current_delta = metrics.get("delta", 0.0)
        
        # If Delta is too positive (Market exposure too long) -> Sell Futures/Calls
        if current_delta > self.max_net_delta:
            logger.warning(f"Portfolio Delta ({current_delta:.2f}) > Limit ({self.max_net_delta})")
            adjustments.append({
                "action": "DELTA_HEDGE",
                "reason": "Delta Limit Breach (+)",
                "quantity": 50,  # Standard Nifty Lot size
                "side": "SELL",
                "instrument_key": "NIFTY_FUT_CURRENT", # Placeholder, would need logic to find actual key
                "priority": "HIGH"
            })
            
        # If Delta is too negative (Market exposure too short) -> Buy Futures/Calls
        elif current_delta < -self.max_net_delta:
            logger.warning(f"Portfolio Delta ({current_delta:.2f}) < Limit (-{self.max_net_delta})")
            adjustments.append({
                "action": "DELTA_HEDGE",
                "reason": "Delta Limit Breach (-)",
                "quantity": 50,
                "side": "BUY",
                "instrument_key": "NIFTY_FUT_CURRENT",
                "priority": "HIGH"
            })

        # 2. Global Loss Protection
        current_pnl = metrics.get("pnl", 0.0)
        if current_pnl < -self.max_daily_loss:
             adjustments.append({
                "action": "REDUCE_EXPOSURE",
                "reason": "Max Daily Loss Breached",
                "quantity": "ALL", # Signal to close everything
                "priority": "CRITICAL"
            })

        return adjustments

    async def evaluate_trade(self, trade: Dict, portfolio_risk: Dict, market_snapshot: Dict) -> List[Dict]:
        """
        Check individual trade health (Stop Loss, Profit Taking).
        """
        adjustments = []
        
        # 1. Stop Loss Check (e.g., 50% loss on premium)
        entry_price = trade.get("average_price", 0)
        current_price = trade.get("current_price", 0)
        quantity = trade.get("quantity", 0)
        
        if entry_price == 0: return []

        # Calculate PnL percentage roughly
        if quantity > 0: # Long position
            pnl_pct = (current_price - entry_price) / entry_price
        else: # Short position
            pnl_pct = (entry_price - current_price) / entry_price

        # Hard Stop Loss at -50% ROI
        if pnl_pct < -0.50:
            adjustments.append({
                "action": "CLOSE_POSITION",
                "reason": "Stop Loss Hit (-50%)",
                "trade_id": trade.get("position_id"),
                "instrument_key": trade.get("instrument_key"),
                "quantity": quantity, # Close full size
                "priority": "HIGH"
            })
            
        # 2. Profit Taking (e.g., +80% ROI)
        elif pnl_pct > 0.80:
             adjustments.append({
                "action": "CLOSE_POSITION",
                "reason": "Take Profit Hit (+80%)",
                "trade_id": trade.get("position_id"),
                "instrument_key": trade.get("instrument_key"),
                "quantity": quantity,
                "priority": "MEDIUM"
            })
            
        return adjustments
