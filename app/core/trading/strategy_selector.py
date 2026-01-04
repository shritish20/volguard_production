# app/core/trading/strategy_selector.py

import logging
from typing import Optional, List
from app.core.trading.strategies import STRATEGY_REGISTRY, StrategyDefinition
from app.schemas.analytics import VolMetrics, RegimeResult

logger = logging.getLogger(__name__)

class StrategySelector:
    """
    VolGuard Smart Strategy Selector.
    
    Responsibility:
    1. FILTER: Excludes strategies not allowed in the current Regime.
    2. SAFETY: Excludes strategies if Volatility is too low (IVP) or too unstable (VoV).
    3. RANK: Picks the 'Best' strategy based on Risk Type and Priority.
    """
    
    def select_strategy(self, 
                       regime: RegimeResult, 
                       vol_metrics: VolMetrics) -> Optional[StrategyDefinition]:
        """
        Selects the optimal strategy definition for the current market state.
        """
        candidates = []
        regime_name = regime.name
        
        # 1. Filter by Regime & Safety Thresholds
        for strategy in STRATEGY_REGISTRY:
            # A. Regime Permission
            if regime_name not in strategy.allowed_regimes:
                # logger.debug(f"Skipping {strategy.name}: Regime mismatch ({regime_name})")
                continue
                
            # B. Volatility Floor (IVP) check
            # We use IVP 30 (30-day IV Rank) as the baseline for cheap/expensive
            if vol_metrics.ivp30 < strategy.min_ivp:
                logger.debug(f"Skipping {strategy.name}: IVP {vol_metrics.ivp30:.1f} < Min {strategy.min_ivp}")
                continue
                
            # C. Vol of Vol Ceiling (Stability) check
            # High VoV means volatility is changing rapidly -> Risk of Gamma explosion
            if vol_metrics.vov > strategy.max_vol_of_vol:
                logger.debug(f"Skipping {strategy.name}: VoV {vol_metrics.vov:.1f} > Max {strategy.max_vol_of_vol}")
                continue
            
            # Note: We rely on the RegimeEngine to have already validated that a VRP Edge exists.
            # We don't double-check VRP here to keep signatures clean, assuming 'regime' implies edge.

            candidates.append(strategy)
            
        if not candidates:
            logger.info(f"No strategies eligible for regime {regime_name} (IVP: {vol_metrics.ivp30:.1f}, VoV: {vol_metrics.vov:.1f})")
            return None
            
        # 2. Rank Strategies
        # Logic:
        # 1. Prefer DEFINED Risk over UNDEFINED (Safety First)
        # 2. Prefer Higher Priority (Manual Override in Registry)
        # 3. Prefer Higher Min VRP (Implies higher quality requirement)
        
        sorted_candidates = sorted(
            candidates,
            key=lambda s: (
                1 if s.risk_type == "DEFINED" else 0, # Primary Sort: Safety
                s.priority,                           # Secondary Sort: Registry Priority
                s.min_vrp                             # Tertiary Sort: Quality Requirement
            ),
            reverse=True
        )
        
        selected = sorted_candidates[0]
        logger.info(f"Selected Strategy: {selected.name} (Regime: {regime_name})")
        return selected
