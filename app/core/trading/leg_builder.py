# app/core/trading/leg_builder.py

import logging
import asyncio
import time
from typing import List, Dict, Optional, Tuple
import httpx
import pandas as pd
from app.core.trading.strategies import StrategyDefinition
from app.core.market.data_client import MarketDataClient

logger = logging.getLogger(__name__)

class LegBuilder:
    """
    VolGuard Hybrid Smart Leg Builder (VolGuard 4.0)
    
    NEW LOGIC:
    - Uses ATM Straddle Price for determining short strike range (Expected Move)
    - Uses Delta for wing strikes (Protection)
    - Hybrid Data: Fetches V3 Greeks + V2 Quotes in parallel
    - Async-first design with proper non-blocking patterns
    
    Responsibility:
    - Converts Abstract Strategy -> Concrete Orders using new hybrid logic
    - LIQUIDITY GATE: Checks Bid-Ask spread before selecting a strike
    - RETRY LOGIC: Falls back if primary strike is illiquid
    """

    def __init__(self, token_manager):
        """
        Initialize with token manager for API authentication
        """
        self.token_manager = token_manager
        self.headers = None
        self._refresh_headers()

    def _refresh_headers(self):
        """Refresh headers from token manager"""
        self.headers = self.token_manager.get_headers()

    async def get_hybrid_data(self, instrument_keys: List[str]) -> Dict[str, Dict]:
        """
        Fetches Greeks (V3) and Quotes (V2) in PARALLEL without blocking
        
        Returns: {
            "instrument_key": {
                "delta": 0.45,
                "price": 120.50,
                "bid": 120.00,
                "ask": 121.00,
                "volume": 1500,
                "oi": 25000,
                "timestamp": 1634567890.123
            }
        }
        """
        if not instrument_keys:
            return {}

        # Ensure fresh headers
        self._refresh_headers()
        keys_str = ",".join(instrument_keys)
        
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Fire both requests simultaneously
            greeks_task = client.get(
                "https://api-v2.upstox.com/v3/market-quote/option-greek",
                params={'instrument_key': keys_str},
                headers=self.headers
            )
            
            quotes_task = client.get(
                "https://api-v2.upstox.com/v2/market-quote/quotes",
                params={'instrument_key': keys_str},
                headers=self.headers
            )
            
            # Await them together
            try:
                greeks_resp, quotes_resp = await asyncio.gather(greeks_task, quotes_task)
            except Exception as e:
                logger.error(f"Hybrid data fetch failed: {e}")
                return {}

        # Parse responses
        greeks_data = greeks_resp.json().get('data', {})
        quotes_data = quotes_resp.json().get('data', {})
        
        # Merge data
        merged = {}
        timestamp_now = time.time()
        
        for key in instrument_keys:
            if key in quotes_data:
                greek_info = greeks_data.get(key, {})
                quote_info = quotes_data.get(key, {})
                
                # Extract depth (bid/ask)
                depth = quote_info.get('depth', {})
                buy_depth = depth.get('buy', [{}])
                sell_depth = depth.get('sell', [{}])
                
                bid_price = buy_depth[0].get('price', 0.0) if buy_depth else 0.0
                ask_price = sell_depth[0].get('price', 0.0) if sell_depth else 0.0
                
                merged[key] = {
                    "delta": abs(greek_info.get('delta', 0.0)),  # Absolute delta
                    "price": quote_info.get('last_price', 0.0),
                    "bid": bid_price,
                    "ask": ask_price,
                    "volume": quote_info.get('volume', 0),
                    "oi": quote_info.get('oi', 0),
                    "timestamp": timestamp_now,
                    "spread_pct": ((ask_price - bid_price) / ask_price * 100) if ask_price > 0 else 100.0
                }
        
        return merged

    async def calculate_atm_straddle_cost(
        self, 
        atm_ce_key: str, 
        atm_pe_key: str
    ) -> Tuple[float, Dict]:
        """
        Calculates ATM Straddle Cost (Expected Move) and returns detailed data
        
        Returns: (straddle_cost, details_dict)
        """
        data = await self.get_hybrid_data([atm_ce_key, atm_pe_key])
        
        ce_data = data.get(atm_ce_key, {})
        pe_data = data.get(atm_pe_key, {})
        
        ce_price = ce_data.get('price', 0.0)
        pe_price = pe_data.get('price', 0.0)
        straddle_cost = ce_price + pe_price
        
        # Validation
        if ce_price <= 0 or pe_price <= 0:
            logger.warning(f"Invalid straddle prices: CE={ce_price}, PE={pe_price}")
            return 0.0, {}
        
        # Check liquidity
        ce_spread = ce_data.get('spread_pct', 100.0)
        pe_spread = pe_data.get('spread_pct', 100.0)
        
        if ce_spread > 10.0 or pe_spread > 10.0:  # 10% spread tolerance
            logger.warning(f"Wide spreads in ATM straddle: CE={ce_spread:.1f}%, PE={pe_spread:.1f}%")
        
        details = {
            'ce_price': ce_price,
            'pe_price': pe_price,
            'ce_delta': ce_data.get('delta', 0.0),
            'pe_delta': pe_data.get('delta', 0.0),
            'ce_spread': ce_spread,
            'pe_spread': pe_spread,
            'timestamp': time.time()
        }
        
        logger.info(f"ATM Straddle Cost: {straddle_cost:.2f} (CE: {ce_price:.2f}, PE: {pe_price:.2f})")
        return straddle_cost, details

    async def find_strike_by_delta(
        self,
        chain_keys: List[str],
        target_delta: float,
        min_volume: int = 500,
        max_spread_pct: float = 10.0
    ) -> Optional[Dict]:
        """
        Finds best liquid strike matching target delta
        
        Returns: {
            'key': instrument_key,
            'strike': strike_price,
            'delta': actual_delta,
            'price': option_price,
            'bid': bid_price,
            'ask': ask_price
        }
        """
        if not chain_keys:
            return None
        
        data = await self.get_hybrid_data(chain_keys)
        
        best_match = None
        min_delta_diff = float('inf')
        
        for key, info in data.items():
            # LIQUIDITY GATES
            if info['ask'] <= 0 or info['volume'] < min_volume:
                continue
                
            if info['spread_pct'] > max_spread_pct:
                continue
            
            # Delta matching logic
            delta_diff = abs(info['delta'] - target_delta)
            
            if delta_diff < min_delta_diff:
                min_delta_diff = delta_diff
                best_match = {
                    'key': key,
                    'strike': self._extract_strike_from_key(key),  # Helper method
                    'delta': info['delta'],
                    'price': info['price'],
                    'bid': info['bid'],
                    'ask': info['ask'],
                    'spread_pct': info['spread_pct'],
                    'volume': info['volume']
                }
        
        if best_match:
            logger.debug(f"Delta {target_delta:.2f} -> Strike {best_match['strike']} "
                        f"(Actual Delta: {best_match['delta']:.3f}, Spread: {best_match['spread_pct']:.1f}%)")
        
        return best_match

    async def find_strike_by_price_offset(
        self,
        chain_keys: List[str],
        spot_price: float,
        price_offset: float,  # e.g., +300 or -300 from ATM
        min_volume: int = 500,
        max_spread_pct: float = 10.0
    ) -> Optional[Dict]:
        """
        Finds strike based on price offset from spot (for short strikes)
        
        Uses: ATM ± Straddle_Cost logic
        """
        if not chain_keys:
            return None
        
        data = await self.get_hybrid_data(chain_keys)
        
        best_match = None
        min_price_diff = float('inf')
        target_strike_approx = spot_price + price_offset
        
        for key, info in data.items():
            # LIQUIDITY GATES
            if info['ask'] <= 0 or info['volume'] < min_volume:
                continue
                
            if info['spread_pct'] > max_spread_pct:
                continue
            
            # Extract strike price from key
            strike = self._extract_strike_from_key(key)
            if not strike:
                continue
            
            # Find strike closest to target
            price_diff = abs(strike - target_strike_approx)
            
            if price_diff < min_price_diff:
                min_price_diff = price_diff
                best_match = {
                    'key': key,
                    'strike': strike,
                    'delta': info['delta'],
                    'price': info['price'],
                    'bid': info['bid'],
                    'ask': info['ask'],
                    'spread_pct': info['spread_pct'],
                    'volume': info['volume']
                }
        
        if best_match:
            logger.debug(f"Price Offset {price_offset:+.0f} -> Strike {best_match['strike']} "
                        f"(Delta: {best_match['delta']:.3f}, Spread: {best_match['spread_pct']:.1f}%)")
        
        return best_match

    def _extract_strike_from_key(self, instrument_key: str) -> Optional[int]:
        """
        Extracts strike price from Upstox instrument key
        Format: NSE_FO|{Expiry}{Month}-NIFTY-{Strike}-{Type}
        Example: NSE_FO|25JAN-NIFTY-21500-CE -> 21500
        """
        try:
            # Split by dash and find the strike part
            parts = instrument_key.split('-')
            for part in parts:
                if part.isdigit():
                    return int(part)
            
            # Alternative: Look for strike pattern
            import re
            match = re.search(r'-(\d{4,5})-', instrument_key)
            if match:
                return int(match.group(1))
                
            return None
        except Exception as e:
            logger.debug(f"Could not extract strike from key {instrument_key}: {e}")
            return None

    async def build_legs(
        self, 
        strategy: StrategyDefinition, 
        chain: pd.DataFrame, 
        lots: int,
        market_client: MarketDataClient,
        spot_price: float,
        atm_strike: int
    ) -> List[Dict]:
        """
        NEW LOGIC: Builds strategy legs using hybrid approach:
        - Short strikes based on ATM Straddle Price (Expected Move)
        - Long wings based on Delta (Protection)
        """
        if lots <= 0:
            logger.error("Invalid lots passed to LegBuilder")
            return []

        if chain.empty:
            logger.error("Empty option chain provided to LegBuilder")
            return []

        # Get chain keys
        all_ce_keys = chain['ce_key'].dropna().tolist()
        all_pe_keys = chain['pe_key'].dropna().tolist()
        all_keys = all_ce_keys + all_pe_keys

        orders: List[Dict] = []

        try:
            # ==================================================================
            # 1. CALCULATE ATM STRADDLE COST (Expected Move)
            # ==================================================================
            atm_ce_key = chain.loc[chain['strike'] == atm_strike, 'ce_key'].iloc[0] \
                        if not chain[chain['strike'] == atm_strike].empty else None
            atm_pe_key = chain.loc[chain['strike'] == atm_strike, 'pe_key'].iloc[0] \
                        if not chain[chain['strike'] == atm_strike].empty else None
            
            if not atm_ce_key or not atm_pe_key:
                logger.error(f"Could not find ATM keys for strike {atm_strike}")
                return []
            
            straddle_cost, straddle_details = await self.calculate_atm_straddle_cost(
                atm_ce_key, atm_pe_key
            )
            
            if straddle_cost <= 0:
                logger.error("Invalid straddle cost calculated")
                return []
            
            logger.info(f"STRADDLE LOGIC: Spot={spot_price:.2f}, "
                       f"ATM={atm_strike}, Expected Move=±{straddle_cost:.2f}")
            
            # ==================================================================
            # 2. STRATEGY-SPECIFIC LEG BUILDING
            # ==================================================================
            structure = strategy.structure
            
            if structure in ("STRANGLE", "CONDOR", "FLY"):
                # SHORT STRIKES: Based on straddle cost (Expected Move)
                short_ce_strike = await self.find_strike_by_price_offset(
                    all_ce_keys,
                    atm_strike,
                    price_offset=+straddle_cost,  # Upper range
                    min_volume=750,
                    max_spread_pct=8.0
                )
                
                short_pe_strike = await self.find_strike_by_price_offset(
                    all_pe_keys,
                    atm_strike,
                    price_offset=-straddle_cost,  # Lower range
                    min_volume=750,
                    max_spread_pct=8.0
                )
                
                if not short_ce_strike or not short_pe_strike:
                    logger.error("Failed to find liquid short strikes")
                    return []
                
                # Add short legs
                self._add_order_to_list(orders, short_ce_strike, "SELL", lots, strategy.name)
                self._add_order_to_list(orders, short_pe_strike, "SELL", lots, strategy.name)
                
                # LONG WINGS: Based on delta (Protection)
                if structure in ("CONDOR", "FLY"):
                    # Different delta targets for different strategies
                    if structure == "CONDOR":
                        wing_delta = 0.10  # 10 Delta wings
                    else:  # IRON FLY
                        wing_delta = 0.15  # 15 Delta wings
                    
                    long_ce_strike = await self.find_strike_by_delta(
                        all_ce_keys,
                        target_delta=wing_delta,
                        min_volume=500,
                        max_spread_pct=12.0  # Slightly wider tolerance for hedges
                    )
                    
                    long_pe_strike = await self.find_strike_by_delta(
                        all_pe_keys,
                        target_delta=wing_delta,
                        min_volume=500,
                        max_spread_pct=12.0
                    )
                    
                    if not long_ce_strike or not long_pe_strike:
                        logger.error("Failed to find liquid wing strikes")
                        return []
                    
                    # Add long hedge legs
                    self._add_order_to_list(orders, long_ce_strike, "BUY", lots, strategy.name, is_hedge=True)
                    self._add_order_to_list(orders, long_pe_strike, "BUY", lots, strategy.name, is_hedge=True)
            
            elif structure == "SPREAD":
                # For spreads, use delta-based selection
                core_delta = strategy.core_deltas[0]
                hedge_delta = strategy.hedge_deltas[0]
                opt_type = "PE" if core_delta < 0 else "CE"
                
                chain_keys = all_pe_keys if opt_type == "PE" else all_ce_keys
                
                core_strike = await self.find_strike_by_delta(
                    chain_keys,
                    target_delta=abs(core_delta),
                    min_volume=1000,
                    max_spread_pct=5.0
                )
                
                hedge_strike = await self.find_strike_by_delta(
                    chain_keys,
                    target_delta=abs(hedge_delta),
                    min_volume=500,
                    max_spread_pct=8.0
                )
                
                if not core_strike or not hedge_strike:
                    return []
                
                self._add_order_to_list(orders, core_strike, "SELL", lots, strategy.name)
                self._add_order_to_list(orders, hedge_strike, "BUY", lots, strategy.name, is_hedge=True)
            
            else:
                logger.error(f"Unsupported strategy structure: {structure}")
                return []
            
            # ==================================================================
            # 3. VALIDATION AND FINAL PREPARATION
            # ==================================================================
            if not self._validate_legs(orders, strategy):
                logger.error("Leg validation failed")
                return []
            
            # Sort: BUY first for margin benefit
            orders.sort(key=lambda o: 0 if o["side"] == "BUY" else 1)
            
            # Log summary
            short_count = sum(1 for o in orders if o["side"] == "SELL")
            long_count = sum(1 for o in orders if o["side"] == "BUY")
            logger.info(f"Built {len(orders)} legs: {short_count} Short, {long_count} Long")
            
            return orders
            
        except Exception as e:
            logger.exception(f"Leg building failed: {e}")
            return []

    def _add_order_to_list(
        self,
        orders_list: List[Dict],
        strike_info: Dict,
        side: str,
        lots: int,
        strategy_name: str,
        is_hedge: bool = False
    ):
        """
        Helper to add order to list with consistent structure
        """
        orders_list.append({
            "action": "ENTRY",
            "instrument_key": strike_info['key'],
            "strike": strike_info['strike'],
            "option_type": "CE" if "CE" in str(strike_info['key']) else "PE",
            "side": side,
            "quantity": lots * 25,  # NIFTY lot size
            "strategy": strategy_name,
            "is_hedge": is_hedge,
            "reason": f"{'HEDGE' if is_hedge else 'CORE'} - "
                     f"Delta: {strike_info['delta']:.2f}, "
                     f"Price: {strike_info['price']:.1f}",
            "price": 0.0,  # Will be resolved to Smart Limit in Executor
            "market_data": {  # Include for reference
                'bid': strike_info.get('bid', 0),
                'ask': strike_info.get('ask', 0),
                'spread_pct': strike_info.get('spread_pct', 0)
            }
        })

    def _validate_legs(self, orders: List[Dict], strategy: StrategyDefinition) -> bool:
        """
        Validates the built legs for consistency and safety
        """
        if not orders:
            return False
        
        # Check quantity
        for order in orders:
            if order["quantity"] <= 0:
                logger.error(f"Invalid quantity in order: {order}")
                return False
        
        # Check for required legs based on strategy
        sells = [o for o in orders if o["side"] == "SELL"]
        buys = [o for o in orders if o["side"] == "BUY"]
        
        if not sells:
            logger.error("No short legs in strategy")
            return False
        
        # For defined risk strategies, must have hedge legs
        if strategy.risk_type == "DEFINED" and not buys:
            logger.error("Defined risk strategy has no hedge legs")
            return False
        
        # Check for duplicate strikes (shouldn't happen)
        strikes = [o["strike"] for o in orders if o["strike"]]
        if len(strikes) != len(set(strikes)):
            logger.warning("Duplicate strikes detected in strategy")
        
        return True
