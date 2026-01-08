import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import pandas as pd
import numpy as np

from app.config import settings
from app.services.instrument_registry import registry
from app.core.trading.leg_builder import LegBuilder # Kept for utility, but logic moved here
from app.schemas.analytics import VolMetrics

# For typing, we use a forward reference or Any if Mandate isn't in schemas yet
from typing import Any 

logger = logging.getLogger(__name__)

class TradingEngine:
    """
    VolGuard 4.1 Execution Engine.
    The "Hands" of the system. Executes the Mandate provided by the Brain (RegimeEngine).
    """

    def __init__(self, market_client, config: Dict):
        self.client = market_client
        self.config = config
        self.builder = LegBuilder() 

    async def generate_entry_orders(
        self,
        mandate: Any, # TradingMandate object
        vol: VolMetrics,
        snapshot: Dict
    ) -> List[Dict]:
        """
        Converts a strategic TradingMandate into specific Order objects.
        """
        try:
            # 1. Validation
            if mandate.regime_name == "CASH" or mandate.allocation_pct <= 0:
                return []

            if mandate.max_lots <= 0:
                logger.warning(f"Mandate allows trade but Max Lots is 0. Check Capital.")
                return []

            # 2. Select Expiry (Weekly vs Monthly)
            expiry_date = await self._resolve_expiry(mandate.expiry_type)
            if not expiry_date:
                logger.error(f"Could not resolve expiry for {mandate.expiry_type}")
                return []

            # 3. Fetch Option Chain
            chain = await self.client.get_option_chain(expiry_date.strftime("%Y-%m-%d"))
            if chain is None or chain.empty:
                logger.error(f"Option chain empty for {expiry_date}")
                return []

            logger.info(f"🏗️ Building {mandate.strategy_type} ({mandate.expiry_type}) | Lots: {mandate.max_lots}")

            # 4. Route to Strategy Builder
            if mandate.strategy_type == "STRANGLE":
                return self._build_strangle(chain, vol, snapshot, mandate)
            
            elif mandate.strategy_type == "IRON_CONDOR":
                return self._build_iron_condor(chain, vol, snapshot, mandate)
            
            elif mandate.strategy_type == "IRON_FLY":
                return self._build_iron_fly(chain, vol, snapshot, mandate)
            
            elif mandate.strategy_type == "CREDIT_SPREAD":
                return self._build_credit_spread(chain, vol, snapshot, mandate)
            
            else:
                logger.warning(f"Unknown strategy type: {mandate.strategy_type}")
                return []

        except Exception as e:
            logger.error(f"Order Generation Failed: {e}", exc_info=True)
            return []

    # =================================================================
    # STRATEGY BUILDERS
    # =================================================================

    def _build_strangle(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        """
        Aggressive Short: Sell OTM Call + Sell OTM Put.
        Target: ~15 Delta or 1 StdDev OTM.
        """
        spot = snapshot['spot']
        
        # Dynamic Width based on Volatility (ATR)
        # Higher Vol = Wider Strangle
        width_mult = 1.0 if vol.ivp_1yr < 50 else 1.2
        range_pts = (vol.atr14 * 2.0) * width_mult
        
        upper_target = spot + range_pts
        lower_target = spot - range_pts

        ce_leg = self._find_strike(chain, "CE", upper_target, "SHORT", mandate.max_lots)
        pe_leg = self._find_strike(chain, "PE", lower_target, "SHORT", mandate.max_lots)

        return [x for x in [ce_leg, pe_leg] if x]

    def _build_iron_condor(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        """
        Moderate Short: Sell OTM Strangle + Buy Wings for protection.
        """
        spot = snapshot['spot']
        range_pts = vol.atr14 * 1.5 # Tighter than strangle
        wing_width = 200 # Fixed width or ATR based
        
        # Short Strikes
        short_ce_target = spot + range_pts
        short_pe_target = spot - range_pts
        
        # Long Strikes (Wings)
        long_ce_target = short_ce_target + wing_width
        long_pe_target = short_pe_target - wing_width

        orders = []
        orders.append(self._find_strike(chain, "CE", short_ce_target, "SHORT", mandate.max_lots))
        orders.append(self._find_strike(chain, "PE", short_pe_target, "SHORT", mandate.max_lots))
        orders.append(self._find_strike(chain, "CE", long_ce_target, "LONG", mandate.max_lots)) # Hedge
        orders.append(self._find_strike(chain, "PE", long_pe_target, "LONG", mandate.max_lots)) # Hedge
        
        return [x for x in orders if x]

    def _build_iron_fly(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        """
        Theta Play: Sell ATM Straddle + Buy Wings.
        Used when DTE is low (Weekly).
        """
        spot = snapshot['spot']
        # Wing width proportional to expected move
        wing_width = vol.atr14 * 1.0 

        orders = []
        # Sell ATM
        orders.append(self._find_strike(chain, "CE", spot, "SHORT", mandate.max_lots))
        orders.append(self._find_strike(chain, "PE", spot, "SHORT", mandate.max_lots))
        # Buy Wings
        orders.append(self._find_strike(chain, "CE", spot + wing_width, "LONG", mandate.max_lots))
        orders.append(self._find_strike(chain, "PE", spot - wing_width, "LONG", mandate.max_lots))

        return [x for x in orders if x]

    def _build_credit_spread(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        """
        Defensive: Directional Credit Spread based on Trend.
        """
        spot = snapshot['spot']
        ma20 = vol.trend_strength * vol.atr14 # Heuristic to recover MA20 proxy logic if needed
        # Or simpler: check mandate warnings/rationale for direction? 
        # For now, we default to Bull Put Spread (neutral/bullish bias) 
        # unless market is crashing.
        
        is_bearish = vol.vov_zscore > 1.0 # Simple filter
        
        orders = []
        if is_bearish:
            # Bear Call Spread
            short = spot + (vol.atr14 * 0.5)
            long_strike = short + 100
            orders.append(self._find_strike(chain, "CE", short, "SHORT", mandate.max_lots))
            orders.append(self._find_strike(chain, "CE", long_strike, "LONG", mandate.max_lots))
        else:
            # Bull Put Spread
            short = spot - (vol.atr14 * 0.5)
            long_strike = short - 100
            orders.append(self._find_strike(chain, "PE", short, "SHORT", mandate.max_lots))
            orders.append(self._find_strike(chain, "PE", long_strike, "LONG", mandate.max_lots))
            
        return [x for x in orders if x]

    # =================================================================
    # UTILITIES
    # =================================================================

    def _find_strike(self, chain: pd.DataFrame, opt_type: str, price_target: float, side: str, lots: int) -> Optional[Dict]:
        """
        Finds the specific contract in the chain closest to the target price.
        """
        try:
            # Filter by type
            subset = chain[chain['instrument_name'].str.endswith(opt_type)].copy()
            if subset.empty: return None

            # Find closest strike
            subset['diff'] = abs(subset['strike'] - price_target)
            best_row = subset.nsmallest(1, 'diff').iloc[0]

            # Build Order Dict (compatible with TradeExecutor)
            return {
                "instrument_key": best_row['instrument_key'],
                "symbol": best_row['trading_symbol'],
                "strike": float(best_row['strike']),
                "option_type": opt_type,
                "expiry_date": best_row['expiry'],
                "side": "SELL" if side == "SHORT" else "BUY",
                "quantity": lots * 50, # Lot size hardcoded or fetch from registry if available
                "order_type": "MARKET",
                "product": "I", # Intraday/Margin
                "strategy": "ALGO_ENTRY",
                "tag": "VolGuard_4.1"
            }
        except Exception as e:
            logger.error(f"Strike selection error: {e}")
            return None

    async def _resolve_expiry(self, expiry_type: str) -> Optional[date]:
        """Gets authoritative expiry from Registry."""
        weekly, monthly = registry.get_nifty_expiries()
        
        if expiry_type == "WEEKLY":
            return weekly
        elif expiry_type == "MONTHLY":
            return monthly
        
        # Fallback
        return weekly

    # Legacy method for compatibility if Supervisor calls it directly
    async def _get_best_expiry_chain(self, symbol="NIFTY") -> Tuple[Optional[date], Optional[pd.DataFrame]]:
        weekly, _ = registry.get_nifty_expiries()
        if not weekly: return None, None
        chain = await self.client.get_option_chain(weekly.strftime("%Y-%m-%d"))
        return weekly, chain
