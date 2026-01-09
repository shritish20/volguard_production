# app/core/trading/engine.py

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
import pandas as pd
import numpy as np

from app.config import settings
from app.services.instrument_registry import registry
from app.schemas.analytics import VolMetrics

# NEW: Import hybrid strategies
from app.core.trading.strategies import (
    HybridStrategyDefinition,
    get_strategies_for_regime,
    get_default_strategy_for_regime,
    validate_strategy_for_market,
    get_strategy_type_for_regime,
    StrategyType
)

logger = logging.getLogger(__name__)

class TradingEngine:
    """
    VolGuard 5.0 Execution Engine - Integrated Hybrid Logic
    
    UPDATES:
    1. ✅ Uses HybridStrategyDefinition from strategies.py
    2. ✅ Dynamic strategy selection based on regime and market conditions
    3. ✅ Proper integration with LegBuilder hybrid methods
    4. ✅ Enhanced validation using strategy-specific parameters
    """

    def __init__(self, market_client, config: Dict, leg_builder, capital_governor):
        self.client = market_client
        self.config = config
        self.leg_builder = leg_builder
        self.capital_governor = capital_governor
        
        # Safety Configuration
        self.MIN_OI = 50000
        self.MAX_SPREAD_PCT = 0.20
        
        # Strategy cache
        self._strategy_cache: Dict[str, List[HybridStrategyDefinition]] = {}
        
        # Performance tracking
        self.strategy_performance: Dict[str, Dict] = {}

    async def generate_entry_orders(
        self,
        mandate: Any, 
        vol: VolMetrics,
        snapshot: Dict
    ) -> List[Dict]:
        """
        Enhanced entry order generation with hybrid strategy selection
        
        Steps:
        1. Validate mandate and market conditions
        2. Select optimal hybrid strategy for regime
        3. Calculate ATM straddle cost (expected move)
        4. Build orders using hybrid logic
        5. Apply strategy-specific safety checks
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

            # 4. GET MARKET DATA
            spot_price = snapshot['spot']
            atm_strike = self._calculate_atm_strike(spot_price)
            
            # 5. SELECT HYBRID STRATEGY
            strategy = await self._select_hybrid_strategy(
                mandate, vol, snapshot, chain, spot_price
            )
            
            if not strategy:
                logger.warning("No suitable hybrid strategy found")
                return []
            
            logger.info(f"🏗️ Selected Strategy: {strategy.name} | "
                       f"Type: {strategy.type.value} | "
                       f"Spot: {spot_price:.2f} | ATM: {atm_strike}")

            # 6. CALCULATE ATM STRADDLE COST
            straddle_cost = await self._calculate_straddle_cost(chain, atm_strike)
            if straddle_cost <= 0:
                logger.error("Failed to calculate valid straddle cost")
                return []
            
            # Apply strategy-specific multiplier
            effective_straddle_cost = straddle_cost * strategy.straddle_multiplier
            logger.info(f"📊 Hybrid Logic: Base Straddle={straddle_cost:.2f}, "
                       f"Multiplier={strategy.straddle_multiplier:.2f}, "
                       f"Effective={effective_straddle_cost:.2f}")

            # 7. BUILD ORDERS USING HYBRID LOGIC
            max_lots = min(mandate.max_lots, strategy.max_position_size)
            
            orders = await self._build_hybrid_strategy_orders(
                strategy, chain, spot_price, atm_strike, 
                effective_straddle_cost, max_lots, mandate, vol
            )
            
            if not orders:
                logger.warning("No orders generated from hybrid strategy")
                return []

            # 8. STRATEGY-SPECIFIC SAFETY CHECKS
            validation_result = await self._validate_strategy_orders(
                strategy, orders, snapshot, vol
            )
            
            if not validation_result["valid"]:
                logger.error(f"Strategy validation failed: {validation_result['reasons']}")
                return []

            # 9. CAPITAL GOVERNOR CHECK
            if not await self.capital_governor.check_margin_sufficiency(orders):
                logger.critical("❌ Margin check failed - aborting trade")
                return []

            # 10. LOG STRATEGY EXECUTION
            self._log_hybrid_strategy_execution(
                strategy, orders, spot_price, effective_straddle_cost, 
                straddle_cost, validation_result
            )
            
            # Track performance
            self._track_strategy_performance(strategy.name, len(orders))
            
            return orders

        except Exception as e:
            logger.error(f"Order Generation Failed: {e}", exc_info=True)
            return []

    async def _select_hybrid_strategy(
        self,
        mandate: Any,
        vol: VolMetrics,
        snapshot: Dict,
        chain: pd.DataFrame,
        spot_price: float
    ) -> Optional[HybridStrategyDefinition]:
        """
        Select optimal hybrid strategy based on regime and market conditions
        """
        regime_name = mandate.regime_name
        
        # Get all strategies for this regime
        strategies = get_strategies_for_regime(regime_name)
        if not strategies:
            logger.warning(f"No hybrid strategies defined for regime: {regime_name}")
            return None
        
        # Prepare market metrics for validation
        market_metrics = {
            "ivp_1yr": vol.ivp_1yr,
            "vrp_weighted_weekly": getattr(vol, 'vrp_weighted_weekly', 0),
            "vov_zscore": vol.vov_zscore,
            "weekly_gex_cr": snapshot.get('weekly_gex', 0)  # Would come from structure engine
        }
        
        # Filter and rank strategies
        valid_strategies = []
        for strategy in strategies:
            # Check basic filters
            if strategy.max_vov_zscore < vol.vov_zscore:
                continue
                
            if vol.ivp_1yr < strategy.min_ivp:
                continue
                
            # Detailed validation
            validation = validate_strategy_for_market(strategy, market_metrics)
            if validation["valid"]:
                valid_strategies.append((strategy, validation))
        
        if not valid_strategies:
            logger.warning(f"No valid hybrid strategies for current market conditions")
            return None
        
        # Select best strategy (highest priority first)
        valid_strategies.sort(key=lambda x: (x[0].priority, len(x[1]["warnings"])), reverse=True)
        
        selected_strategy, validation = valid_strategies[0]
        
        logger.info(f"✅ Selected {selected_strategy.name} "
                   f"(Priority: {selected_strategy.priority}, "
                   f"Warnings: {len(validation['warnings'])})")
        
        return selected_strategy

    async def _build_hybrid_strategy_orders(
        self,
        strategy: HybridStrategyDefinition,
        chain: pd.DataFrame,
        spot_price: float,
        atm_strike: int,
        effective_straddle_cost: float,
        max_lots: int,
        mandate: Any,
        vol: VolMetrics
    ) -> List[Dict]:
        """
        Build orders based on hybrid strategy type
        """
        # Get option keys
        all_ce_keys = chain['ce_key'].dropna().tolist()
        all_pe_keys = chain['pe_key'].dropna().tolist()
        
        if strategy.type == StrategyType.SHORT_STRANGLE:
            return await self._build_hybrid_strangle(
                strategy, all_ce_keys, all_pe_keys, atm_strike, 
                effective_straddle_cost, max_lots, mandate
            )
            
        elif strategy.type == StrategyType.IRON_CONDOR:
            return await self._build_hybrid_condor(
                strategy, all_ce_keys, all_pe_keys, atm_strike,
                effective_straddle_cost, max_lots, mandate
            )
            
        elif strategy.type == StrategyType.IRON_FLY:
            return await self._build_hybrid_fly(
                strategy, all_ce_keys, all_pe_keys, atm_strike,
                effective_straddle_cost, max_lots, mandate
            )
            
        elif strategy.type == StrategyType.CREDIT_SPREAD:
            return await self._build_hybrid_credit_spread(
                strategy, all_ce_keys, all_pe_keys, atm_strike,
                vol, max_lots, mandate
            )
            
        elif strategy.type == StrategyType.RATIO_SPREAD:
            return await self._build_hybrid_ratio_spread(
                strategy, all_ce_keys, all_pe_keys, atm_strike,
                effective_straddle_cost, max_lots, mandate
            )
            
        else:
            logger.error(f"Unsupported strategy type: {strategy.type}")
            return []

    async def _build_hybrid_strangle(
        self,
        strategy: HybridStrategyDefinition,
        ce_keys: List[str],
        pe_keys: List[str],
        atm_strike: int,
        straddle_cost: float,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Build hybrid strangle (naked or with wings)
        """
        logger.info(f"🔧 Building {strategy.name} (Hybrid Strangle)")
        
        # Find short strikes using straddle range
        short_ce = await self.leg_builder.find_strike_by_price_offset(
            ce_keys,
            atm_strike,
            price_offset=+straddle_cost,
            min_volume=1000,
            max_spread_pct=5.0
        )
        
        short_pe = await self.leg_builder.find_strike_by_price_offset(
            pe_keys,
            atm_strike,
            price_offset=-straddle_cost,
            min_volume=1000,
            max_spread_pct=5.0
        )
        
        if not short_ce or not short_pe:
            logger.error("Failed to find liquid strangle strikes")
            return []
        
        orders = [
            self._create_order_from_strike(short_ce, "SELL", max_lots, mandate, strategy, "CORE"),
            self._create_order_from_strike(short_pe, "SELL", max_lots, mandate, strategy, "CORE")
        ]
        
        # Add wings if defined
        if strategy.wing_delta is not None:
            wing_ce = await self.leg_builder.find_strike_by_delta(
                ce_keys,
                target_delta=strategy.wing_delta,
                min_volume=500,
                max_spread_pct=10.0
            )
            
            wing_pe = await self.leg_builder.find_strike_by_delta(
                pe_keys,
                target_delta=strategy.wing_delta,
                min_volume=500,
                max_spread_pct=10.0
            )
            
            if wing_ce and wing_pe:
                orders.extend([
                    self._create_order_from_strike(wing_ce, "BUY", max_lots, mandate, strategy, "HEDGE"),
                    self._create_order_from_strike(wing_pe, "BUY", max_lots, mandate, strategy, "HEDGE")
                ])
        
        return orders

    async def _build_hybrid_condor(
        self,
        strategy: HybridStrategyDefinition,
        ce_keys: List[str],
        pe_keys: List[str],
        atm_strike: int,
        straddle_cost: float,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Build hybrid iron condor
        """
        logger.info(f"🔧 Building {strategy.name} (Hybrid Condor)")
        
        # Short strikes based on straddle range
        short_ce = await self.leg_builder.find_strike_by_price_offset(
            ce_keys,
            atm_strike,
            price_offset=+straddle_cost,
            min_volume=750,
            max_spread_pct=8.0
        )
        
        short_pe = await self.leg_builder.find_strike_by_price_offset(
            pe_keys,
            atm_strike,
            price_offset=-straddle_cost,
            min_volume=750,
            max_spread_pct=8.0
        )
        
        if not short_ce or not short_pe:
            logger.error("Failed to find liquid short strikes for condor")
            return []
        
        # Long wings based on delta
        if strategy.wing_delta is None:
            logger.error("Condor strategy requires wing delta")
            return []
        
        long_ce = await self.leg_builder.find_strike_by_delta(
            ce_keys,
            target_delta=strategy.wing_delta,
            min_volume=500,
            max_spread_pct=12.0
        )
        
        long_pe = await self.leg_builder.find_strike_by_delta(
            pe_keys,
            target_delta=strategy.wing_delta,
            min_volume=500,
            max_spread_pct=12.0
        )
        
        if not long_ce or not long_pe:
            logger.error("Failed to find liquid wing strikes for condor")
            return []
        
        return [
            self._create_order_from_strike(short_ce, "SELL", max_lots, mandate, strategy, "CORE"),
            self._create_order_from_strike(short_pe, "SELL", max_lots, mandate, strategy, "CORE"),
            self._create_order_from_strike(long_ce, "BUY", max_lots, mandate, strategy, "HEDGE"),
            self._create_order_from_strike(long_pe, "BUY", max_lots, mandate, strategy, "HEDGE")
        ]

    async def _build_hybrid_fly(
        self,
        strategy: HybridStrategyDefinition,
        ce_keys: List[str],
        pe_keys: List[str],
        atm_strike: int,
        straddle_cost: float,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Build hybrid iron fly
        """
        logger.info(f"🔧 Building {strategy.name} (Hybrid Fly)")
        
        # Short strikes are ATM
        short_ce = await self.leg_builder.find_strike_by_price_offset(
            ce_keys,
            atm_strike,
            price_offset=0,  # ATM
            min_volume=1000,
            max_spread_pct=5.0
        )
        
        short_pe = await self.leg_builder.find_strike_by_price_offset(
            pe_keys,
            atm_strike,
            price_offset=0,  # ATM
            min_volume=1000,
            max_spread_pct=5.0
        )
        
        if not short_ce or not short_pe:
            logger.error("Failed to find ATM strikes for iron fly")
            return []
        
        # Long wings based on delta or fixed distance
        if strategy.wing_delta is not None:
            # Delta-based wings
            long_ce = await self.leg_builder.find_strike_by_delta(
                ce_keys,
                target_delta=strategy.wing_delta,
                min_volume=500,
                max_spread_pct=10.0
            )
            
            long_pe = await self.leg_builder.find_strike_by_delta(
                pe_keys,
                target_delta=strategy.wing_delta,
                min_volume=500,
                max_spread_pct=10.0
            )
        elif strategy.wing_distance_points is not None:
            # Fixed distance wings
            long_ce = await self.leg_builder.find_strike_by_price_offset(
                ce_keys,
                atm_strike,
                price_offset=+strategy.wing_distance_points,
                min_volume=500,
                max_spread_pct=10.0
            )
            
            long_pe = await self.leg_builder.find_strike_by_price_offset(
                pe_keys,
                atm_strike,
                price_offset=-strategy.wing_distance_points,
                min_volume=500,
                max_spread_pct=10.0
            )
        else:
            logger.error("Iron fly requires either wing_delta or wing_distance_points")
            return []
        
        if not long_ce or not long_pe:
            logger.error("Failed to find wing strikes for iron fly")
            return []
        
        return [
            self._create_order_from_strike(short_ce, "SELL", max_lots, mandate, strategy, "CORE"),
            self._create_order_from_strike(short_pe, "SELL", max_lots, mandate, strategy, "CORE"),
            self._create_order_from_strike(long_ce, "BUY", max_lots, mandate, strategy, "HEDGE"),
            self._create_order_from_strike(long_pe, "BUY", max_lots, mandate, strategy, "HEDGE")
        ]

    async def _build_hybrid_credit_spread(
        self,
        strategy: HybridStrategyDefinition,
        ce_keys: List[str],
        pe_keys: List[str],
        atm_strike: int,
        vol: VolMetrics,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Build hybrid credit spread
        """
        logger.info(f"🔧 Building {strategy.name} (Hybrid Credit Spread)")
        
        # Determine direction based on VoV
        is_bearish = vol.vov_zscore > 1.0
        
        if is_bearish:
            # Bear Call Spread
            keys_to_use = ce_keys
            short_offset = +strategy.straddle_multiplier * 100  # Simplified
            long_offset = short_offset + (strategy.wing_distance_points or 100)
        else:
            # Bull Put Spread
            keys_to_use = pe_keys
            short_offset = -strategy.straddle_multiplier * 100
            long_offset = short_offset - (strategy.wing_distance_points or 100)
        
        # Find strikes
        short_strike = await self.leg_builder.find_strike_by_price_offset(
            keys_to_use,
            atm_strike,
            price_offset=short_offset,
            min_volume=1000,
            max_spread_pct=5.0
        )
        
        long_strike = await self.leg_builder.find_strike_by_price_offset(
            keys_to_use,
            atm_strike,
            price_offset=long_offset,
            min_volume=500,
            max_spread_pct=8.0
        )
        
        if not short_strike or not long_strike:
            logger.error("Failed to find strikes for credit spread")
            return []
        
        return [
            self._create_order_from_strike(short_strike, "SELL", max_lots, mandate, strategy, "CORE"),
            self._create_order_from_strike(long_strike, "BUY", max_lots, mandate, strategy, "HEDGE")
        ]

    async def _build_hybrid_ratio_spread(
        self,
        strategy: HybridStrategyDefinition,
        ce_keys: List[str],
        pe_keys: List[str],
        atm_strike: int,
        straddle_cost: float,
        max_lots: int,
        mandate: Any
    ) -> List[Dict]:
        """
        Build hybrid ratio spread
        """
        logger.info(f"🔧 Building {strategy.name} (Hybrid Ratio Spread)")
        
        # Ratio spreads are more complex - simplified implementation
        # For now, fall back to basic implementation
        
        logger.warning("Ratio spread implementation pending - using fallback")
        return []

    async def _validate_strategy_orders(
        self,
        strategy: HybridStrategyDefinition,
        orders: List[Dict],
        snapshot: Dict,
        vol: VolMetrics
    ) -> Dict[str, Any]:
        """
        Validate orders against strategy-specific constraints
        """
        validation = {
            "strategy": strategy.name,
            "valid": True,
            "reasons": [],
            "warnings": []
        }
        
        # Basic order validation
        if not self._validate_orders(orders):
            validation["valid"] = False
            validation["reasons"].append("Basic order validation failed")
            return validation
        
        # Check position count
        sells = [o for o in orders if o["side"] == "SELL"]
        buys = [o for o in orders if o["side"] == "BUY"]
        
        if strategy.risk_type == "DEFINED" and not buys:
            validation["valid"] = False
            validation["reasons"].append("Defined risk strategy has no hedge legs")
        
        # Check delta exposure if we have greeks
        if snapshot.get('live_greeks'):
            total_delta = sum(
                o.get('metadata', {}).get('target_delta', 0) * 
                (1 if o["side"] == "BUY" else -1)
                for o in orders
            )
            
            if abs(total_delta) > strategy.max_delta_exposure:
                validation["warnings"].append(
                    f"Delta exposure {total_delta:.2f} exceeds preferred {strategy.max_delta_exposure}"
                )
        
        return validation

    async def _calculate_straddle_cost(self, chain: pd.DataFrame, atm_strike: int) -> float:
        """
        Calculate ATM straddle cost using LegBuilder
        """
        try:
            atm_row = chain[chain['strike'] == atm_strike]
            if atm_row.empty:
                logger.warning(f"ATM strike {atm_strike} not found in chain")
                return 0.0
                
            atm_ce_key = atm_row['ce_key'].iloc[0] if 'ce_key' in atm_row else None
            atm_pe_key = atm_row['pe_key'].iloc[0] if 'pe_key' in atm_row else None
            
            if not atm_ce_key or not atm_pe_key:
                logger.warning("Could not find ATM option keys")
                return 0.0
            
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

    def _create_order_from_strike(
        self, 
        strike_info: Dict, 
        side: str, 
        lots: int, 
        mandate: Any,
        strategy: HybridStrategyDefinition,
        leg_type: str = "CORE"
    ) -> Dict:
        """
        Enhanced order creation with strategy metadata
        """
        # Get authoritative lot size
        expiry_date = datetime.now().date()
        specs = registry.get_nifty_contract_specs(expiry_date)
        lot_size = specs.get("lot_size", 50)
        quantity = lots * lot_size
        
        # Determine option type
        instrument_key = strike_info['key']
        option_type = "CE" if "CE" in str(instrument_key) else "PE"
        
        # Get trading symbol
        trading_symbol = registry.get_symbol_from_key(instrument_key) if hasattr(registry, 'get_symbol_from_key') else f"NIFTY{strike_info['strike']}{option_type}"
        
        # Calculate limit price with strategy buffer
        base_price = strike_info.get('price', 0)
        if side == "BUY":
            limit_price = base_price * (1 + strategy.buffer_pct)
        else:
            limit_price = base_price * (1 - strategy.buffer_pct)
        
        return {
            "instrument_key": instrument_key,
            "symbol": trading_symbol,
            "strike": float(strike_info['strike']),
            "option_type": option_type,
            "expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "side": side,
            "quantity": quantity,
            "order_type": "LIMIT",
            "price": round(limit_price, 1),
            "product": "I",
            "strategy": strategy.name,
            "strategy_type": strategy.type.value,
            "tag": f"VolGuard_5.0_{strategy.name}",
            "leg_type": leg_type,
            "metadata": {
                "target_delta": strike_info.get('delta', 0),
                "entry_price": base_price,
                "spread_pct": strike_info.get('spread_pct', 0),
                "strategy_config": {
                    "straddle_multiplier": strategy.straddle_multiplier,
                    "wing_delta": strategy.wing_delta,
                    "wing_distance": strategy.wing_distance_points,
                    "buffer_pct": strategy.buffer_pct,
                    "max_loss_pct": strategy.max_loss_pct
                }
            }
        }

    def _calculate_atm_strike(self, spot_price: float) -> int:
        """Calculate nearest standard strike"""
        return round(spot_price / 50) * 50

    def _validate_orders(self, orders: List[Dict]) -> bool:
        """Basic order validation"""
        if not orders:
            return False
            
        for order in orders:
            if order["quantity"] <= 0:
                logger.error(f"Invalid quantity in order: {order}")
                return False
            if not order.get("instrument_key"):
                logger.error(f"Missing instrument key in order: {order}")
                return False
                
        return True

    def _log_hybrid_strategy_execution(
        self,
        strategy: HybridStrategyDefinition,
        orders: List[Dict],
        spot: float,
        effective_straddle_cost: float,
        base_straddle_cost: float,
        validation_result: Dict
    ):
        """Log detailed hybrid strategy execution"""
        short_strikes = [o["strike"] for o in orders if o["side"] == "SELL"]
        long_strikes = [o["strike"] for o in orders if o["side"] == "BUY"]
        
        logger.info(f"✅ HYBRID STRATEGY EXECUTED: {strategy.name}")
        logger.info(f"   Type: {strategy.type.value}")
        logger.info(f"   Spot: {spot:.2f}")
        logger.info(f"   Base Straddle: ±{base_straddle_cost:.2f}")
        logger.info(f"   Effective Range: ±{effective_straddle_cost:.2f} (x{strategy.straddle_multiplier:.2f})")
        logger.info(f"   Short Strikes: {sorted(short_strikes)}")
        if long_strikes:
            logger.info(f"   Long Wings: {sorted(long_strikes)} (Δ={strategy.wing_delta or 'N/A'})")
        logger.info(f"   Total Legs: {len(orders)}")
        logger.info(f"   Total Quantity: {sum(o['quantity'] for o in orders)}")
        logger.info(f"   Buffer: {strategy.buffer_pct*100:.1f}%")
        
        if validation_result["warnings"]:
            logger.warning(f"   Warnings: {validation_result['warnings']}")

    def _track_strategy_performance(self, strategy_name: str, order_count: int):
        """Track strategy performance metrics"""
        if strategy_name not in self.strategy_performance:
            self.strategy_performance[strategy_name] = {
                "execution_count": 0,
                "total_orders": 0,
                "last_execution": datetime.now().isoformat()
            }
        
        self.strategy_performance[strategy_name]["execution_count"] += 1
        self.strategy_performance[strategy_name]["total_orders"] += order_count
        self.strategy_performance[strategy_name]["last_execution"] = datetime.now().isoformat()

    async def _resolve_expiry(self, expiry_type: str) -> Optional[date]:
        """Resolve expiry date"""
        weekly, monthly = registry.get_nifty_expiries()
        return monthly if expiry_type == "MONTHLY" else weekly

    def get_strategy_performance_report(self) -> Dict:
        """Get strategy performance report"""
        return {
            "timestamp": datetime.now().isoformat(),
            "strategies_executed": len(self.strategy_performance),
            "performance": self.strategy_performance,
            "total_orders": sum(s["total_orders"] for s in self.strategy_performance.values())
            }
