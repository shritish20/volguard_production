import logging
from typing import List, Dict
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

class AdjustmentEngine:
    def __init__(self, config: Dict):
        self.max_net_delta = config.get("MAX_NET_DELTA", 0.40)
        self.max_daily_loss = config.get("MAX_DAILY_LOSS", 20000)
        
    async def evaluate_portfolio(self, portfolio_risk: Dict, market_snapshot: Dict) -> List[Dict]:
        adjustments = []
        metrics = portfolio_risk.get("aggregate_metrics", {})
        delta = metrics.get("delta", 0.0)
        
        if abs(delta) > self.max_net_delta:
            # FIX: Dynamic Token & Lot Size
            fut_key = registry.get_current_future("NIFTY")
            if not fut_key: return []
            
            details = registry.get_instrument_details(fut_key)
            lot = details.get('lot_size', 25)
            
            # Simple Hedge: 1 Lot
            side = "SELL" if delta > 0 else "BUY"
            
            adjustments.append({
                "action": "DELTA_HEDGE",
                "instrument_key": fut_key,
                "quantity": lot,
                "side": side,
                "strategy": "HEDGE"
            })
            
        return adjustments

    async def evaluate_trade(self, trade, risk, snap): return [] # Keep logic from prev steps
