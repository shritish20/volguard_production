# app/core/trading/engine.py

import logging
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from app.core.trading.leg_builder import LegBuilder
from app.core.trading.strategy_selector import StrategySelector
from app.services.instrument_registry import registry
from app.config import settings

logger = logging.getLogger(__name__)

class TradingEngine:
    def __init__(self, market_client, config: Dict):
        self.client = market_client
        self.config = config
        self.selector = StrategySelector()
        self.builder = LegBuilder()

    async def analyze_and_select(self, regime_data, snapshot, cap_governor) -> Optional[Dict]:
        """
        Main decision pipeline: Regime -> Strategy -> Strike Selection -> Sizing
        """
        # 1. Select Strategy Class
        strategy_def = self.selector.select_strategy(regime_data, snapshot)
        if not strategy_def:
            return None

        # 2. Select Expiry (Nearest Weekly, 2-45 DTE)
        chain = await self._get_best_expiry_chain(snapshot['symbol'])
        if not chain:
            logger.warning(f"No valid option chain found for {snapshot['symbol']}")
            return None

        # 3. Dynamic Sizing (The Fix)
        lots = await self._suggest_lots(regime_data, cap_governor)
        
        # 4. Build Orders
        legs = self.builder.build_legs(
            strategy_def, 
            chain, 
            snapshot['spot'], 
            lots=lots
        )
        
        if not legs:
            return None

        return {
            "strategy": strategy_def.name,
            "orders": legs,
            "lots": lots,
            "expiry": chain['expiry'],
            "regime_context": regime_data.name
        }

    async def _suggest_lots(self, regime, cap_governor) -> int:
        """
        Calculate safe position size based on Available Margin & Regime.
        """
        try:
            # 1. Get Real Available Funds (Live from Broker)
            available_funds = await cap_governor.get_available_funds()
            
            # 2. Determine Budget based on Regime Ceiling
            # Use the lesser of Target Budget (Regime %) or Real Cash
            target_allocation = settings.BASE_CAPITAL * regime.alloc_pct
            usable_cash = min(available_funds, target_allocation)
            
            # 3. Estimate Margin per Lot (Conservative Estimate)
            est_margin_per_lot = settings.MARGIN_SELL_PER_LOT  # e.g., 120,000
            
            # 4. Raw Lot Calculation
            if est_margin_per_lot <= 0: return 1
            raw_lots = int(usable_cash / est_margin_per_lot)
            
            # 5. Apply Hard Limits (Config + Regime)
            final_lots = max(1, min(
                raw_lots, 
                regime.max_lots, 
                settings.MAX_TOTAL_LOTS
            ))
            
            return final_lots

        except Exception as e:
            logger.error(f"Sizing Error: {e}")
            return 1 # Fallback to minimum

    async def _get_best_expiry_chain(self, symbol: str) -> Optional[Dict]:
        """
        Finds the ideal weekly expiry:
        - > 2 Days (Avoid Gamma Risk)
        - < 45 Days (Avoid Liquidity Risk)
        - Nearest valid one
        """
        try:
            # Fetch all expiries from Master Registry
            all_chains = registry.get_option_chain(symbol)
            if not all_chains:
                return None
            
            valid_expiries = []
            now = datetime.now().date()
            
            # Filter Expiries
            for exp_date_str, chain_data in all_chains.items():
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                dte = (exp_date - now).days
                
                # Filter Logic
                if 2 <= dte <= 45:
                    valid_expiries.append((dte, chain_data))
            
            if not valid_expiries:
                return None
            
            # Sort by DTE (Nearest first)
            valid_expiries.sort(key=lambda x: x[0])
            
            return valid_expiries[0][1] # Return the chain data for nearest valid expiry

        except Exception as e:
            logger.error(f"Expiry Selection Failed: {e}")
            return None
