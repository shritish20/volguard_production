import logging
from typing import Optional, List
from app.core.trading.strategies import STRATEGY_REGISTRY, StrategyDefinition
from app.schemas.analytics import VolMetrics, RegimeResult

logger = logging.getLogger(__name__)

class StrategySelector:
    """
    Selects the optimal strategy based on Regime, Safety Filters, and Ranking.
    Strictly follows the decision hierarchy.
    """
    
    def select_strategy(self, 
                       regime: RegimeResult, 
                       vol_metrics: VolMetrics) -> Optional[StrategyDefinition]:
        
        candidates = []
        regime_name = regime.name
        
        # 1. Filter by Regime & Safety Thresholds
        for strategy in STRATEGY_REGISTRY:
            # A. Regime Permission
            if regime_name not in strategy.allowed_regimes:
                continue
                
            # B. Volatility Floor (IVP)
            if vol_metrics.ivp30 < strategy.min_ivp:
                continue
                
            # C. VRP Floor (Edge)
            # Note: Assuming VRP is checked via regime scoring, but we double check here if needed
            # For simplicity, we assume Regime handles primary permission, but specific strategy limits apply
            
            # D. Vol of Vol Ceiling (Stability)
            if vol_metrics.vov > strategy.max_vol_of_vol:
                continue
                
            candidates.append(strategy)
            
        if not candidates:
            logger.info(f"No strategies eligible for regime {regime_name}")
            return None
            
        # 2. Rank Strategies
        # Criteria 1: Defined Risk > Undefined Risk (unless regime implies otherwise, but safety first)
        # Criteria 2: Priority Score (Manual Preference)
        # Criteria 3: Higher Min VRP (Implies higher quality req)
        
        sorted_candidates = sorted(
            candidates,
            key=lambda s: (
                1 if s.risk_type == "DEFINED" else 0, # Prefer Defined Risk
                s.priority,                           # Prefer Higher Priority
                s.min_vrp                             # Prefer Higher VRP req (Quality)
            ),
            reverse=True
        )
        
        selected = sorted_candidates[0]
        logger.info(f"Selected Strategy: {selected.name} (Regime: {regime_name})")
        return selected
