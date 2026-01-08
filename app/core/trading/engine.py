import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
import pandas as pd
import numpy as np

from app.config import settings
from app.services.instrument_registry import registry
from app.schemas.analytics import VolMetrics

# For typing
from typing import Any 

logger = logging.getLogger(__name__)

class TradingEngine:
    """
    VolGuard 4.1 Execution Engine.
    
    UPGRADES:
    1. Dynamic Wings (ATR Based) for ALL strategies.
    2. Liquidity & Spread Protection (Bid-Ask Check).
    3. Dynamic Lot Sizing.
    """

    def __init__(self, market_client, config: Dict):
        self.client = market_client
        self.config = config
        
        # Configuration for Safety
        self.MIN_OI = 50000          # Minimum Open Interest
        self.MAX_SPREAD_PCT = 0.20   # Max allowed spread (20% of LTP)

    async def generate_entry_orders(
        self,
        mandate: Any, 
        vol: VolMetrics,
        snapshot: Dict
    ) -> List[Dict]:
        try:
            # 1. Validation
            if mandate.regime_name == "CASH" or mandate.allocation_pct <= 0:
                return []
            if mandate.max_lots <= 0:
                return []

            # 2. Select Expiry
            expiry_date = await self._resolve_expiry(mandate.expiry_type)
            if not expiry_date:
                return []

            # 3. Fetch Option Chain
            chain = await self.client.get_option_chain(expiry_date.strftime("%Y-%m-%d"))
            if chain is None or chain.empty:
                return []

            logger.info(f"🏗️ Building {mandate.strategy_type} | ATR: {vol.atr14:.1f} | Lots: {mandate.max_lots}")

            # 4. Strategy Routing
            if mandate.strategy_type == "STRANGLE":
                return self._build_strangle(chain, vol, snapshot, mandate)
            elif mandate.strategy_type == "IRON_CONDOR":
                return self._build_iron_condor(chain, vol, snapshot, mandate)
            elif mandate.strategy_type == "IRON_FLY":
                return self._build_iron_fly(chain, vol, snapshot, mandate)
            elif mandate.strategy_type == "CREDIT_SPREAD":
                return self._build_credit_spread(chain, vol, snapshot, mandate)
            else:
                return []

        except Exception as e:
            logger.error(f"Order Generation Failed: {e}", exc_info=True)
            return []

    # =================================================================
    # STRATEGY BUILDERS (NOW WITH 100% DYNAMIC WINGS)
    # =================================================================

    def _build_strangle(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        spot = snapshot['spot']
        # Dynamic Width: 2.0x ATR (Safe distance)
        width_mult = 1.0 if vol.ivp_1yr < 50 else 1.2
        range_pts = (vol.atr14 * 2.0) * width_mult
        
        return [
            self._find_strike(chain, "CE", spot + range_pts, "SHORT", mandate.max_lots),
            self._find_strike(chain, "PE", spot - range_pts, "SHORT", mandate.max_lots)
        ]

    def _build_iron_condor(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        spot = snapshot['spot']
        
        # 1. Short Strikes (1.5x ATR)
        range_pts = vol.atr14 * 1.5
        short_ce = spot + range_pts
        short_pe = spot - range_pts
        
        # 2. Wing Width (1.0x ATR) - DYNAMIC NOW
        wing_width = max(100, vol.atr14 * 1.0) 
        
        return [
            self._find_strike(chain, "CE", short_ce, "SHORT", mandate.max_lots),
            self._find_strike(chain, "PE", short_pe, "SHORT", mandate.max_lots),
            self._find_strike(chain, "CE", short_ce + wing_width, "LONG", mandate.max_lots), # Protection
            self._find_strike(chain, "PE", short_pe - wing_width, "LONG", mandate.max_lots)  # Protection
        ]

    def _build_iron_fly(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        spot = snapshot['spot']
        
        # Wing Width (1.0x ATR)
        wing_width = max(100, vol.atr14 * 1.0)

        return [
            self._find_strike(chain, "CE", spot, "SHORT", mandate.max_lots),
            self._find_strike(chain, "PE", spot, "SHORT", mandate.max_lots),
            self._find_strike(chain, "CE", spot + wing_width, "LONG", mandate.max_lots),
            self._find_strike(chain, "PE", spot - wing_width, "LONG", mandate.max_lots)
        ]

    def _build_credit_spread(self, chain: pd.DataFrame, vol: VolMetrics, snapshot: Dict, mandate: Any) -> List[Dict]:
        spot = snapshot['spot']
        is_bearish = vol.vov_zscore > 1.0
        
        # Width (0.5x ATR) - DYNAMIC NOW
        width = max(50, vol.atr14 * 0.5)

        orders = []
        if is_bearish:
            # Bear Call Spread
            short = spot + (vol.atr14 * 0.5)
            orders.append(self._find_strike(chain, "CE", short, "SHORT", mandate.max_lots))
            orders.append(self._find_strike(chain, "CE", short + width, "LONG", mandate.max_lots))
        else:
            # Bull Put Spread
            short = spot - (vol.atr14 * 0.5)
            orders.append(self._find_strike(chain, "PE", short, "SHORT", mandate.max_lots))
            orders.append(self._find_strike(chain, "PE", short - width, "LONG", mandate.max_lots))
            
        return orders

    # =================================================================
    # SMART STRIKE SELECTION & LIQUIDITY CHECK
    # =================================================================

    def _find_strike(self, chain: pd.DataFrame, opt_type: str, price_target: float, side: str, lots: int) -> Optional[Dict]:
        try:
            # 1. Filter by Option Type
            subset = chain[chain['instrument_name'].str.endswith(opt_type)].copy()
            if subset.empty: return None

            # 2. Find closest strike
            subset['diff'] = abs(subset['strike'] - price_target)
            
            # Sort by distance
            candidates = subset.nsmallest(5, 'diff') # Look at top 5 candidates

            # 3. LIQUIDITY & SPREAD CHECK
            best_row = None
            for _, row in candidates.iterrows():
                if self._check_liquidity(row):
                    best_row = row
                    break
            
            # If all fail liquidity check, fallback to the absolute closest but warn
            if best_row is None:
                logger.warning(f"⚠️ No liquid strikes found near {price_target}. Using closest (Risky).")
                best_row = candidates.iloc[0]

            # 4. Get Authoritative Lot Size
            expiry_date = best_row['expiry']
            specs = registry.get_nifty_contract_specs(expiry_date)
            lot_size = specs.get("lot_size", 50)
            quantity = lots * lot_size

            return {
                "instrument_key": best_row['instrument_key'],
                "symbol": best_row['trading_symbol'],
                "strike": float(best_row['strike']),
                "option_type": opt_type,
                "expiry_date": best_row['expiry'],
                "side": "SELL" if side == "SHORT" else "BUY",
                "quantity": quantity,
                "order_type": "MARKET",
                "product": "I",
                "strategy": "ALGO_ENTRY",
                "tag": "VolGuard_4.1"
            }
        except Exception as e:
            logger.error(f"Strike selection error: {e}")
            return None

    def _check_liquidity(self, row: pd.Series) -> bool:
        """
        Validates if the strike is safe to trade.
        Checks: Volume, OI, and Spread (if available).
        """
        # A. Check Columns Existence (Upstox API varies)
        col_oi = 'oi' if 'oi' in row else 'ce_oi' if 'ce_oi' in row else None
        col_vol = 'volume' if 'volume' in row else None
        
        # B. Check OI (Skip if OI < 50k)
        if col_oi and row[col_oi] < self.MIN_OI:
            return False
            
        # C. Check Volume (Skip if 0)
        if col_vol and row[col_vol] <= 0:
            return False

        # D. Check Bid-Ask Spread (If columns available)
        # Note: 'depth' usually requires a separate call, but sometimes chain has 'ask_price'
        if 'ask_price' in row and 'bid_price' in row:
            ask = row['ask_price']
            bid = row['bid_price']
            ltp = row.get('last_price', (ask+bid)/2)
            
            if ltp > 0:
                spread = ask - bid
                spread_pct = spread / ltp
                if spread_pct > self.MAX_SPREAD_PCT:
                    return False # Spread too wide

        return True

    async def _resolve_expiry(self, expiry_type: str) -> Optional[date]:
        weekly, monthly = registry.get_nifty_expiries()
        return monthly if expiry_type == "MONTHLY" else weekly

    async def _get_best_expiry_chain(self, symbol="NIFTY") -> Tuple[Optional[date], Optional[pd.DataFrame]]:
        weekly, _ = registry.get_nifty_expiries()
        if not weekly: return None, None
        chain = await self.client.get_option_chain(weekly.strftime("%Y-%m-%d"))
        return weekly, chain
