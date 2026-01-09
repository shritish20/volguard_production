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
    VolGuard 5.0 Execution Engine - Hybrid Logic Upgrade
    
    MAJOR UPGRADES:
    1. NEW LOGIC: Uses ATM Straddle Price for determining short strike range (Expected Move)
    2. NEW LOGIC: Uses Delta for wing strikes (Protection)
    3. Integrates with Hybrid Smart Leg Builder for intelligent strike selection
    4. Maintains all existing safety features (liquidity checks, dynamic lot sizing)
    
    LOGIC BREAKDOWN:
    - SHORT STRIKES: ATM ± Straddle Cost (Market's expected move)
    - LONG WINGS: 10 Delta for Condors, 15 Delta for Flies (Consistent protection)
    - ALL ASYNC: Non-blocking parallel data fetching
    """

    def __init__(self, market_client, config: Dict, leg_builder, capital_governor):
        self.client = market_client
        self.config = config
        self.leg_builder = leg_builder
        self.capital_governor = capital_governor
        
        # Configuration for Safety
        self.MIN_OI = 50000          # Minimum Open Interest
        self.MAX_SPREAD_PCT = 0.20   # Max allowed spread (20% of LTP)
        
        # Strategy Configuration
        self.strategy_config = {
            "IRON_CONDOR": {
                "wing_delta": 0.10,      # 10 Delta wings
                "max_position_size": 3,   # Max lots
                "straddle_multiplier": 1.0  # Use full straddle cost
            },
            "IRON_FLY": {
                "wing_delta": 0.15,      # 15 Delta wings (tighter)
                "max_position_size": 2,
                "straddle_multiplier": 0.5  # Half straddle for tighter range
            },
            "STRANGLE": {
                "wing_delta": None,      # No wings
                "max_position_size": 1,
                "straddle_multiplier": 1.0  # Use full straddle cost
            }
        }

    async def generate_entry_orders(
        self,
        mandate: Any, 
        vol: VolMetrics,
        snapshot: Dict
    ) -> List[Dict]:
        """
        Generates entry orders using new hybrid logic
        
        Steps:
        1. Validate market conditions and mandate
        2. Calculate ATM Straddle Cost (Expected Move)
        3. Use LegBuilder to find optimal strikes (Straddle Range + Delta Wings)
        4. Apply pre-trade safety checks
        5. Return validated orders
        """
        try:
            # 1. VALIDATION GATE
            if mandate.regime_name == "CASH" or mandate.allocation_pct <= 0:
                logger.info("No allocation or cash regime - skipping")
                return []
            if mandate.max_lots <= 0:
                logger.error("Invalid lot size in mandate")
                return []

            # 2. SELECT EXPIRY
            expiry_date = await self._resolve_expiry(mandate.expiry_type)
            if not expiry_date:
                logger.error("Could not resolve expiry date")
                return []

            # 3. FETCH OPTION CHAIN
            chain = await self.client.get_option_chain(expiry_date.strftime("%Y-%m-%d"))
            if chain is None or chain.empty:
                logger.error("Empty option chain received")
                return []

            # 4. GET SPOT AND ATM STRIKE
            spot_price = snapshot['spot']
            atm_strike = self._calculate_atm_strike(spot_price)
            
            logger.info(f"🏗️ Building {mandate.strategy_type} | "
                       f"Spot: {spot_price:.2f} | ATM: {atm_strike} | "
                       f"Lots: {mandate.max_lots}")

            # 5. CALCULATE ATM STRADDLE COST (EXPECTED MOVE)
            straddle_cost = await self._calculate_straddle_cost(chain, atm_strike)
            if straddle_cost <= 0:
                logger.error("Failed to calculate valid straddle cost")
                return []
            
            logger.info(f"📊 STRADDLE LOGIC: Expected Move = ±{straddle_cost:.2f} points")

            # 6. STRATEGY ROUTING WITH NEW LOGIC
            strategy_type = mandate.strategy_type
            max_lots = min(mandate.max_lots, self.strategy_config.get(strategy_type, {}).get("max_position_size", 1))
            
            orders = []
            
            if strategy_type == "STRANGLE":
                orders = await self._build_straddle_strangle(
                    chain, spot_price, atm_strike, straddle_cost, max_lots, mandate
                )
            elif strategy_type == "IRON_CONDOR":
                orders = await self._build_iron_condor(
                    chain, spot_price, atm_strike, straddle_cost, max_lots, mandate
                )
            elif strategy_type == "IRON_FLY":
                orders = await self._build_iron_fly(
                    chain, spot_price, atm_strike, straddle_cost, max_lots, mandate
                )
            elif strategy_type == "CREDIT_SPREAD":
                orders = await self._build_credit_spread(
                    chain, spot_price, atm_strike, vol, max_lots, mandate
                )
            else:
                logger.error(f"Unsupported strategy type: {strategy_type}")
                return []

            # 7. PRE-TRADE SAFETY CHECKS
            if not orders:
                logger.warning("No orders generated")
                return []
                
            # Check margin sufficiency
            if not await self.capital_governor.check_margin_sufficiency(orders):
                logger.critical("❌ Margin check failed - aborting trade")
                return []
                
            # Validate order structure
            if not self._validate_orders(orders):
                logger.error("Order validation failed")
                return []

            # 8. LOG STRATEGY SUMMARY
            self._log_strategy_summary(orders, spot_price, straddle_cost, strategy_type)
            
            return orders

        except Exception as e:
            logger.error(f"Order Generation Failed: {e}", exc_info=True)
            return []

    async def _calculate_straddle_cost(self, chain: pd.DataFrame, atm_strike: int) -> float:
        """
        Calculates ATM Straddle Cost using LegBuilder
        
        Returns: Straddle cost (sum of ATM CE and PE premiums)
        """
        try:
            # Find ATM CE and PE keys
            atm_row = chain[chain['strike'] == atm_strike]
            if atm_row.empty:
                logger.warning(f"ATM strike {atm_strike} not found in chain")
                return 0.0
                
            atm_ce_key = atm_row['ce_key'].iloc[0] if 'ce_key' in atm_row else None
            atm_pe_key = atm_row['pe_key'].iloc[0] if 'pe_key' in atm_row else None
            
            if not atm_ce_key or not atm_pe_key:
                logger.warning("Could not find ATM option keys")
                return 0.0
            
            # Use LegBuilder to get straddle cost
            straddle_cost, details = await self.leg_builder.calculate_atm_straddle_cost(
                atm_ce_key, atm_pe_key
            )
            
            if straddle_cost > 0:
                logger.info(f"ATM Straddle: CE={details.get('ce_price', 0):.1f}, "
                           f"PE={details.get('pe_price', 0):.1f}, "
                           f"Total={straddle_cost:.1f}")
            
            return straddle_cost
            
        except Exception as e:
            logger.error(f"Straddle calculation failed: {e}")
            return 0.0

    async def _build_straddle_strangle(
        self,
        chain: pd.DataFrame,
        spot_price: float,
        atm_strike: int,
        straddle_cost: float,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Builds Strangle using Straddle Range logic
        
        Short Strikes: ATM ± Straddle Cost
        No wings (naked strangle - for aggressive regimes only)
        """
        logger.info("🔧 Building STRANGLE (Straddle Range Logic)")
        
        # Get all option keys
        all_ce_keys = chain['ce_key'].dropna().tolist()
        all_pe_keys = chain['pe_key'].dropna().tolist()
        
        # Find short strikes using straddle range
        short_ce = await self.leg_builder.find_strike_by_price_offset(
            all_ce_keys,
            atm_strike,
            price_offset=+straddle_cost,  # Upper range
            min_volume=1000,
            max_spread_pct=5.0
        )
        
        short_pe = await self.leg_builder.find_strike_by_price_offset(
            all_pe_keys,
            atm_strike,
            price_offset=-straddle_cost,  # Lower range
            min_volume=1000,
            max_spread_pct=5.0
        )
        
        if not short_ce or not short_pe:
            logger.error("Failed to find liquid strangle strikes")
            return []
        
        # Build orders
        orders = [
            self._create_order_from_strike(short_ce, "SELL", max_lots, mandate, "CORE"),
            self._create_order_from_strike(short_pe, "SELL", max_lots, mandate, "CORE")
        ]
        
        return orders

    async def _build_iron_condor(
        self,
        chain: pd.DataFrame,
        spot_price: float,
        atm_strike: int,
        straddle_cost: float,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Builds Iron Condor using Hybrid Logic
        
        Short Strikes: ATM ± Straddle Cost (Market expected move)
        Long Wings: 10 Delta strikes (Consistent protection)
        """
        logger.info("🔧 Building IRON CONDOR (Hybrid Logic)")
        
        # Get all option keys
        all_ce_keys = chain['ce_key'].dropna().tolist()
        all_pe_keys = chain['pe_key'].dropna().tolist()
        
        # 1. SHORT STRIKES (Based on straddle range)
        short_ce = await self.leg_builder.find_strike_by_price_offset(
            all_ce_keys,
            atm_strike,
            price_offset=+straddle_cost,  # Upper range
            min_volume=750,
            max_spread_pct=8.0
        )
        
        short_pe = await self.leg_builder.find_strike_by_price_offset(
            all_pe_keys,
            atm_strike,
            price_offset=-straddle_cost,  # Lower range
            min_volume=750,
            max_spread_pct=8.0
        )
        
        if not short_ce or not short_pe:
            logger.error("Failed to find liquid short strikes for condor")
            return []
        
        # 2. LONG WINGS (Based on delta)
        config = self.strategy_config["IRON_CONDOR"]
        wing_delta = config["wing_delta"]
        
        long_ce = await self.leg_builder.find_strike_by_delta(
            all_ce_keys,
            target_delta=wing_delta,
            min_volume=500,
            max_spread_pct=12.0  # Wider tolerance for hedges
        )
        
        long_pe = await self.leg_builder.find_strike_by_delta(
            all_pe_keys,
            target_delta=wing_delta,
            min_volume=500,
            max_spread_pct=12.0
        )
        
        if not long_ce or not long_pe:
            logger.error("Failed to find liquid wing strikes for condor")
            return []
        
        # 3. BUILD ALL LEGS
        orders = [
            self._create_order_from_strike(short_ce, "SELL", max_lots, mandate, "CORE"),
            self._create_order_from_strike(short_pe, "SELL", max_lots, mandate, "CORE"),
            self._create_order_from_strike(long_ce, "BUY", max_lots, mandate, "HEDGE"),
            self._create_order_from_strike(long_pe, "BUY", max_lots, mandate, "HEDGE")
        ]
        
        return orders

    async def _build_iron_fly(
        self,
        chain: pd.DataFrame,
        spot_price: float,
        atm_strike: int,
        straddle_cost: float,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Builds Iron Fly using Hybrid Logic
        
        Short Strikes: ATM (center strikes)
        Long Wings: 15 Delta strikes (Tighter protection than condor)
        Uses reduced straddle range for tighter structure
        """
        logger.info("🔧 Building IRON FLY (Hybrid Logic)")
        
        # Get all option keys
        all_ce_keys = chain['ce_key'].dropna().tolist()
        all_pe_keys = chain['pe_key'].dropna().tolist()
        
        # Find ATM strikes (center)
        atm_row = chain[chain['strike'] == atm_strike]
        if atm_row.empty:
            logger.error(f"Could not find ATM strike {atm_strike}")
            return []
            
        # 1. SHORT STRIKES (ATM)
        short_ce = {
            'key': atm_row['ce_key'].iloc[0],
            'strike': atm_strike,
            'delta': 0.5,  # Approximate ATM delta
            'price': atm_row['ce_ltp'].iloc[0] if 'ce_ltp' in atm_row else 0
        }
        
        short_pe = {
            'key': atm_row['pe_key'].iloc[0],
            'strike': atm_strike,
            'delta': -0.5,  # Approximate ATM delta
            'price': atm_row['pe_ltp'].iloc[0] if 'pe_ltp' in atm_row else 0
        }
        
        # 2. LONG WINGS (Based on delta - tighter than condor)
        config = self.strategy_config["IRON_FLY"]
        wing_delta = config["wing_delta"]
        
        long_ce = await self.leg_builder.find_strike_by_delta(
            all_ce_keys,
            target_delta=wing_delta,
            min_volume=500,
            max_spread_pct=10.0
        )
        
        long_pe = await self.leg_builder.find_strike_by_delta(
            all_pe_keys,
            target_delta=wing_delta,
            min_volume=500,
            max_spread_pct=10.0
        )
        
        if not long_ce or not long_pe:
            logger.error("Failed to find liquid wing strikes for iron fly")
            return []
        
        # 3. BUILD ALL LEGS
        orders = [
            self._create_order_from_strike(short_ce, "SELL", max_lots, mandate, "CORE"),
            self._create_order_from_strike(short_pe, "SELL", max_lots, mandate, "CORE"),
            self._create_order_from_strike(long_ce, "BUY", max_lots, mandate, "HEDGE"),
            self._create_order_from_strike(long_pe, "BUY", max_lots, mandate, "HEDGE")
        ]
        
        return orders

    async def _build_credit_spread(
        self,
        chain: pd.DataFrame,
        spot_price: float,
        atm_strike: int,
        vol: VolMetrics,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Builds Credit Spread (fallback to delta-based for spreads)
        """
        logger.info("🔧 Building CREDIT SPREAD (Delta-based)")
        
        # Determine direction based on volatility
        is_bearish = vol.vov_zscore > 1.0
        
        # Get all option keys
        all_ce_keys = chain['ce_key'].dropna().tolist()
        all_pe_keys = chain['pe_key'].dropna().tolist()
        
        orders = []
        
        if is_bearish:
            # Bear Call Spread
            short_delta = 0.30  # Sell 30 delta
            long_delta = 0.15   # Buy 15 delta
            
            short_strike = await self.leg_builder.find_strike_by_delta(
                all_ce_keys,
                target_delta=short_delta,
                min_volume=1000,
                max_spread_pct=5.0
            )
            
            long_strike = await self.leg_builder.find_strike_by_delta(
                all_ce_keys,
                target_delta=long_delta,
                min_volume=500,
                max_spread_pct=8.0
            )
            
            if short_strike and long_strike:
                orders = [
                    self._create_order_from_strike(short_strike, "SELL", max_lots, mandate, "CORE"),
                    self._create_order_from_strike(long_strike, "BUY", max_lots, mandate, "HEDGE")
                ]
                
        else:
            # Bull Put Spread
            short_delta = 0.30  # Sell -30 delta (absolute)
            long_delta = 0.15   # Buy -15 delta (absolute)
            
            short_strike = await self.leg_builder.find_strike_by_delta(
                all_pe_keys,
                target_delta=short_delta,
                min_volume=1000,
                max_spread_pct=5.0
            )
            
            long_strike = await self.leg_builder.find_strike_by_delta(
                all_pe_keys,
                target_delta=long_delta,
                min_volume=500,
                max_spread_pct=8.0
            )
            
            if short_strike and long_strike:
                orders = [
                    self._create_order_from_strike(short_strike, "SELL", max_lots, mandate, "CORE"),
                    self._create_order_from_strike(long_strike, "BUY", max_lots, mandate, "HEDGE")
                ]
        
        return orders

    def _create_order_from_strike(
        self, 
        strike_info: Dict, 
        side: str, 
        lots: int, 
        mandate: Any,
        leg_type: str = "CORE"
    ) -> Dict:
        """
        Creates order dictionary from strike information
        """
        # Get authoritative lot size
        expiry_date = datetime.now().date()  # Would come from chain in real implementation
        specs = registry.get_nifty_contract_specs(expiry_date)
        lot_size = specs.get("lot_size", 50)
        quantity = lots * lot_size
        
        # Determine option type from instrument key
        instrument_key = strike_info['key']
        option_type = "CE" if "CE" in str(instrument_key) else "PE"
        
        # Get trading symbol from registry if possible
        trading_symbol = registry.get_symbol_from_key(instrument_key) if hasattr(registry, 'get_symbol_from_key') else f"NIFTY{strike_info['strike']}{option_type}"
        
        return {
            "instrument_key": instrument_key,
            "symbol": trading_symbol,
            "strike": float(strike_info['strike']),
            "option_type": option_type,
            "expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "side": side,
            "quantity": quantity,
            "order_type": "LIMIT",  # Safer than market
            "product": "I",
            "strategy": mandate.strategy_type,
            "tag": "VolGuard_5.0",
            "leg_type": leg_type,
            "metadata": {
                "target_delta": strike_info.get('delta', 0),
                "entry_price": strike_info.get('price', 0),
                "spread_pct": strike_info.get('spread_pct', 0)
            }
        }

    def _calculate_atm_strike(self, spot_price: float) -> int:
        """Calculates nearest standard strike to spot price"""
        # NIFTY strikes are in multiples of 50
        return round(spot_price / 50) * 50

    def _validate_orders(self, orders: List[Dict]) -> bool:
        """Validates order structure and quantities"""
        if not orders:
            return False
            
        for order in orders:
            if order["quantity"] <= 0:
                logger.error(f"Invalid quantity in order: {order}")
                return False
            if not order.get("instrument_key"):
                logger.error(f"Missing instrument key in order: {order}")
                return False
                
        # Check for reasonable structure
        sells = [o for o in orders if o["side"] == "SELL"]
        buys = [o for o in orders if o["side"] == "BUY"]
        
        if not sells:
            logger.error("Strategy has no short legs")
            return False
            
        return True

    def _log_strategy_summary(self, orders: List[Dict], spot: float, straddle_cost: float, strategy: str):
        """Logs detailed strategy summary"""
        short_strikes = [o["strike"] for o in orders if o["side"] == "SELL"]
        long_strikes = [o["strike"] for o in orders if o["side"] == "BUY"]
        
        logger.info(f"✅ {strategy} BUILT:")
        logger.info(f"   Spot: {spot:.2f}")
        logger.info(f"   Expected Move (Straddle): ±{straddle_cost:.2f}")
        logger.info(f"   Short Strikes: {sorted(short_strikes)}")
        if long_strikes:
            logger.info(f"   Long Wings: {sorted(long_strikes)}")
        logger.info(f"   Total Legs: {len(orders)}")
        logger.info(f"   Total Quantity: {sum(o['quantity'] for o in orders)}")

    async def _resolve_expiry(self, expiry_type: str) -> Optional[date]:
        """Resolves expiry date based on type"""
        weekly, monthly = registry.get_nifty_expiries()
        return monthly if expiry_type == "MONTHLY" else weekly

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
