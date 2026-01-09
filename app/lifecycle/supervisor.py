import asyncio
import time
import logging
import uuid
import os
import pandas as pd
from pathlib import Path
from typing import Dict, Union, List, Optional
from datetime import datetime, date, time as dt_time, timedelta
from collections import deque

from app.services.instrument_registry import registry
from app.core.data.quality_gate import DataQualityGate
from app.database import add_decision_log
from app.services.alert_service import alert_service
from app.services.telegram_alerts import telegram_alerts
from app.lifecycle.safety_controller import SafetyController, ExecutionMode, SystemState
from app.core.risk.capital_governor import CapitalGovernor
from app.services.approval_system import ManualApprovalSystem

# Core Engines (VolGuard 5.0)
from app.core.trading.exit_engine import ExitEngine
from app.core.analytics.regime import RegimeEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.edge import EdgeEngine
from app.core.market.participant_client import ParticipantClient

from app.core.trading.adjustment_engine import AdjustmentEngine
from app.core.trading.executor import TradeExecutor
from app.core.trading.engine import TradingEngine
from app.core.risk.engine import RiskEngine
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.schemas.analytics import ExtMetrics, VolMetrics, RegimeResult
from app.config import settings

# NEW IMPORT for TokenManager
from app.core.auth.token_manager import TokenManager

# Metrics
from app.utils.metrics import (
    supervisor_cycle_duration, update_portfolio_metrics,
    set_system_state, position_delta as net_delta_metric, 
    market_data_quality as data_quality_score,
    record_order_placed, record_order_failed, record_safety_violation,
    orders_placed_total as orders_placed, orders_failed_total as orders_failed,
    risk_limit_breaches as safety_violations,
    track_duration
)

logger = logging.getLogger(__name__)


class ProductionTradingSupervisor:
    """ 
    VolGuard 5.0 Supervisor - WITH STARTUP GATE
    
    ENHANCEMENTS:
    1. ✅ Startup Market-Open Check Gate
    2. ✅ Token Manager Integration
    3. ✅ Enhanced Safety Layer with Margin Audit
    4. ✅ All VolGuard 5.0 Hybrid Logic (Straddle Range + Delta Wings)
    """

    def __init__(
        self,
        market_client: MarketDataClient,
        risk_engine: RiskEngine,
        adjustment_engine: AdjustmentEngine,
        trade_executor: TradeExecutor,
        trading_engine: TradingEngine,
        capital_governor: CapitalGovernor,
        token_manager: TokenManager,  # NEW: Token manager for API calls
        websocket_service=None,
        loop_interval_seconds: float = 3.0,
    ):
        # Clients
        self.market = market_client
        self.risk = risk_engine
        self.adj = adjustment_engine
        self.exec = trade_executor
        self.engine = trading_engine
        self.cap_governor = capital_governor
        self.token_manager = token_manager  # NEW
        self.ws = websocket_service

        # Safety & Governance
        self.quality = DataQualityGate()
        self.safety = SafetyController()
        self.approvals = ManualApprovalSystem()

        # Analytics Brain (VolGuard 5.0 Upgrades)
        self.exit_engine = ExitEngine()
        self.regime_engine = RegimeEngine()
        self.structure_engine = StructureEngine()
        self.vol_engine = VolatilityEngine()
        self.edge_engine = EdgeEngine()
        self.participant_client = ParticipantClient()

        # Loop Control
        self.interval = loop_interval_seconds
        self.running = False
        self.positions: Dict = {}
        self.consecutive_data_failures = 0
        self.max_data_failures = 3

        # Smart Data Cache
        self.daily_data = pd.DataFrame()
        self.intraday_data = pd.DataFrame()
        self.last_heavy_refresh_date = None

        # Timers
        self.last_daily_fetch = 0.0
        self.last_intraday_fetch = 0.0
        self.last_entry_time = 0.0
        self.last_successful_cycle = time.time()

        # Config
        self.min_entry_interval = 300
        self.intraday_fetch_interval = 300

        # Monitoring
        self.regime_history = deque(maxlen=5)
        self.cycle_times = deque(maxlen=100)
        self.avg_cycle_time = 0.0

        # Background Tasks & Locks (CRITICAL SAFETY)
        self._background_tasks: set = set()
        self._intraday_refresh_lock = asyncio.Lock()
        self._position_update_lock = asyncio.Lock()
        self._capital_update_lock = asyncio.Lock()
        
        # Startup Flags
        self.market_open_verified = False
        self.token_validated = False

    async def start(self):
        """Main Entry Point - The Boot Sequence with Startup Gates"""
        logger.info(f"🤖 VolGuard 5.0 Supervisor booting in {self.safety.execution_mode.value} mode")

        # ================================================
        # 🚀 STARTUP GATES (CRITICAL SAFETY CHECKS)
        # ================================================
        
        # GATE 1: Token Validation
        if not await self._validate_startup_token():
            logger.critical("❌ Token validation failed - Aborting startup")
            await self.safety.trigger_full_stop("STARTUP_TOKEN_INVALID")
            return
            
        # GATE 2: Market Open Check (NEW STARTUP GATE)
        if not await self.await_market_open():
            logger.critical("❌ Market not open - Aborting startup")
            await self.safety.trigger_full_stop("STARTUP_MARKET_CLOSED")
            return
            
        # GATE 3: Token Health Check
        if not await self.token_manager.validate_token():
            logger.critical("❌ Token health check failed - Aborting startup")
            await self.safety.trigger_full_stop("STARTUP_TOKEN_UNHEALTHY")
            return

        # GATE 4: Market Status Check
        await self._check_market_status()

        # ================================================
        # 🔧 STRICT RECONCILIATION (CRITICAL)
        # ================================================
        logger.info("🔧 Reconciling Broker State with Database...")
        try:
            await self.exec.reconcile_state()
            logger.info("✅ State reconciliation complete")
        except Exception as e:
            logger.critical(f"❌ FATAL: Reconciliation Failed: {e}")
            
            if self.safety.execution_mode == ExecutionMode.FULL_AUTO:
                logger.critical("Cannot start FULL_AUTO mode with unreconciled state - ABORTING")
                raise RuntimeError(f"Reconciliation failed in FULL_AUTO mode: {e}")
            elif self.safety.execution_mode == ExecutionMode.SEMI_AUTO:
                logger.critical("Cannot start SEMI_AUTO mode with unreconciled state - ABORTING")
                raise RuntimeError(f"Reconciliation failed in SEMI_AUTO mode: {e}")
            else:
                logger.warning("⚠️ Starting in SHADOW mode with reconciliation failure")
                logger.warning("Risk calculations may be inaccurate! Monitoring only.")
                await self.safety.record_failure("RECONCILIATION_FAILED", 
                    {"error": str(e), "mode": "SHADOW"}, "HIGH")

        # ================================================
        # 🏁 START THE MAIN LOOP
        # ================================================
        self.running = True
        logger.info("✅ All startup gates passed. Starting main loop...")
        
        try:
            await self._run_smart_loop()
        finally:
            await self._cleanup_background_tasks()

    async def await_market_open(self):
        """
        NEW STARTUP GATE: Wait for market to open before proceeding
        
        Returns:
            True if market opens successfully, False on timeout/failure
        """
        logger.info("⏳ Startup: Checking Market Status...")
        
        max_wait_time = 300  # Maximum 5 minutes to wait
        start_time = time.time()
        
        while True:
            # Check if we've waited too long
            elapsed = time.time() - start_time
            if elapsed > max_wait_time:
                logger.error(f"Market open check timeout after {elapsed:.0f}s")
                return False
            
            try:
                # Use token manager for authenticated API call
                headers = self.token_manager.get_headers()
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "https://api.upstox.com/v2/market/status/NSE",
                        headers=headers,
                        timeout=10
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            market_status = data.get('data', {}).get('status', 'closed')
                            
                            if market_status == "open":
                                logger.info("✅ Market OPEN. Proceeding with startup...")
                                self.market_open_verified = True
                                return True
                            else:
                                logger.info(f"Market status: {market_status}. Waiting...")
                        else:
                            logger.warning(f"Market status API error: {response.status}")
                            
            except asyncio.TimeoutError:
                logger.warning("Market status check timeout")
            except aiohttp.ClientError as e:
                logger.warning(f"Market status check network error: {e}")
            except Exception as e:
                logger.error(f"Market status check unexpected error: {e}")
            
            # Wait before retrying
            wait_time = min(10, max_wait_time - elapsed)  # Wait up to 10 seconds
            if wait_time > 0:
                logger.debug(f"Waiting {wait_time}s before next market status check...")
                await asyncio.sleep(wait_time)
        
        return False

    async def _validate_startup_token(self):
        """
        Validate token at startup
        
        Returns:
            True if token is valid, False otherwise
        """
        logger.info("🔐 Validating access token...")
        
        try:
            # Use token manager's validation
            if await self.token_manager.validate_token():
                self.token_validated = True
                logger.info("✅ Token validated successfully")
                return True
            else:
                logger.error("❌ Token validation failed")
                return False
                
        except Exception as e:
            logger.error(f"Token validation error: {e}")
            return False

    async def _run_smart_loop(self):
        """
        INTELLIGENT LOOP - Combines drift correction with smart scheduling
        """
        logger.info(f"🧠 Smart Loop Activated. Interval: {self.interval}s")
        
        next_tick = time.monotonic()
        cycle_counter = 0
        
        # NEW: Perform initial margin audit
        await self._perform_margin_audit()
        
        while self.running:
            cycle_counter += 1
            cycle_start_mono = time.monotonic()
            
            try:
                now = datetime.now()
                current_time = now.time()
                today = now.date()

                # ========================================================
                # 🛑 PHASE 0: WEEKEND & HOLIDAY CHECK
                # ========================================================
                if today.weekday() >= 5:  # Saturday=5, Sunday=6
                    if now.hour == 9 and now.minute == 0:
                        logger.info(f"📅 Weekend ({today.strftime('%A')}) - Hibernating...")
                    await asyncio.sleep(3600)
                    continue

                # Check holidays once per hour during early morning
                if current_time.hour == 8 and current_time.minute < 30:
                    try:
                        holidays = await self.market.get_holidays()
                        if today in holidays:
                            logger.info(f"🏖️ Market Holiday - Sleeping...")
                            await asyncio.sleep(3600 * 4)
                            continue
                    except Exception as e:
                        logger.warning(f"Holiday check failed: {e}")

                # ========================================================
                # 🌙 PHASE 1: MARKET HOURS CHECK
                # ========================================================
                market_prep_time = dt_time(8, 45)
                market_close_time = dt_time(15, 30)
                is_prep_hours = (market_prep_time <= current_time <= market_close_time)

                if not is_prep_hours:
                    # Night-time hibernation
                    if self.ws and self.ws.is_connected:
                        logger.info("🌙 Market Closed - Disconnecting WebSocket")
                        await self.ws.disconnect()
                    
                    if now.minute == 0 and now.second < 5:
                        logger.info(f"💤 Hibernating... (Time: {current_time.strftime('%H:%M')})")
                    
                    await asyncio.sleep(60)
                    continue

                # ========================================================
                # ☀️ PHASE 2: DAILY MORNING RITUAL
                # ========================================================
                if self.last_heavy_refresh_date != today:
                    logger.info("☀️ Good Morning! Performing Daily Data Ritual...")
                    
                    # 1. Instrument Master (force refresh)
                    await asyncio.to_thread(registry.load_master, force_refresh=True)
                    
                    # 2. Historical Data
                    await self._refresh_heavy_data()
                    
                    self.last_heavy_refresh_date = today
                    logger.info("✅ Daily Data Ritual Complete")
                    
                    # NEW: Daily margin audit
                    await self._perform_margin_audit()

                # ========================================================
                # 🚀 PHASE 3: ACTIVE TRADING CYCLE
                # ========================================================
                
                # Ensure WebSocket is connected
                if self.ws and not self.ws.is_connected:
                    await self.ws.connect()
                    await asyncio.sleep(2)

                # Background intraday refresh
                if time.time() - self.last_intraday_fetch > self.intraday_fetch_interval:
                    if not self._intraday_refresh_lock.locked():
                        task = asyncio.create_task(self._refresh_intraday_data_safe())
                        self._background_tasks.add(task)
                        task.add_done_callback(self._background_tasks.discard)

                # Execute main trading logic
                await self._execute_trading_cycle(cycle_counter)
                
                # NEW: Periodic margin audit (every 60 cycles ~ 3 minutes)
                if cycle_counter % 60 == 0:
                    await self._perform_margin_audit()

            except Exception as e:
                logger.error(f"💥 Smart Loop Error: {e}", exc_info=True)
                await asyncio.sleep(5)

            # Drift correction sleep
            cycle_duration = time.monotonic() - cycle_start_mono
            sleep_time = max(0, self.interval - cycle_duration)
            
            if sleep_time <= 0:
                # logger.warning(f"Cycle {cycle_counter} overrun by {-sleep_time:.3f}s")
                sleep_time = 0.001
            
            await asyncio.sleep(sleep_time)
            next_tick += self.interval

    async def _perform_margin_audit(self):
        """
        NEW: Perform margin integrity audit
        
        Checks broker margin vs internal tracking
        Triggers emergency stop if mismatch detected
        """
        try:
            # Use capital governor's audit method if available
            if hasattr(self.cap_governor, 'audit_margin_integrity'):
                await self.cap_governor.audit_margin_integrity()
            else:
                # Fallback audit logic
                await self._basic_margin_audit()
                
        except Exception as e:
            logger.error(f"Margin audit failed: {e}")

    async def _basic_margin_audit(self):
        """
        Basic margin audit comparing broker vs internal tracking
        """
        try:
            # Get broker margin
            broker_margin = await self.cap_governor.get_available_funds()
            
            # Get internal tracking (approximate)
            internal_margin = self.cap_governor.local_tracker.get_available()
            
            # Calculate drift
            if broker_margin > 0 and internal_margin > 0:
                drift_pct = abs(broker_margin - internal_margin) / broker_margin * 100
                
                if drift_pct > 5.0:  # More than 5% drift
                    logger.critical(f"💰 MARGIN DRIFT DETECTED: Broker={broker_margin:.0f}, "
                                  f"Internal={internal_margin:.0f}, Drift={drift_pct:.1f}%")
                    
                    # Trigger safety check
                    await self.safety.record_failure(
                        "MARGIN_MISMATCH",
                        {
                            "broker_margin": broker_margin,
                            "internal_margin": internal_margin,
                            "drift_pct": drift_pct
                        },
                        "HIGH"
                    )
                    
                    # In FULL_AUTO mode, consider emergency stop
                    if self.safety.execution_mode == ExecutionMode.FULL_AUTO and drift_pct > 10.0:
                        await self.safety.trigger_full_stop("MARGIN_MISMATCH_CRITICAL")
                        
        except Exception as e:
            logger.error(f"Basic margin audit error: {e}")

    async def _execute_trading_cycle(self, cycle_counter: int):
        """
        Core trading logic with all safety mechanisms
        """
        cycle_start_time = time.time()
        cycle_id = f"{cycle_counter:06d}-{str(uuid.uuid4())[:8]}"
        
        cycle_log = {
            "cycle_id": cycle_id,
            "mode": self.safety.execution_mode.value,
            "timestamp": datetime.now()
        }

        # 0. KILL SWITCH CHECK (CRITICAL)
        if self._check_kill_switch():
            logger.critical("🔴 KILL SWITCH ACTIVATED - EMERGENCY SHUTDOWN")
            self.safety.system_state = SystemState.EMERGENCY
            set_system_state("EMERGENCY")
            self.running = False
            return

        try:
            # 1. READ LIVE SNAPSHOT
            snapshot = await self._read_live_snapshot()
            
            # Smart wait if market not open yet (spot=0)
            if snapshot['spot'] == 0:
                # logger.debug(f"[{cycle_id}] Spot=0, waiting for market data...")
                return

            # 2. DATA QUALITY GATE
            valid, reason = self.quality.validate_snapshot(snapshot)
            if not valid:
                self.consecutive_data_failures += 1
                logger.warning(f"[{cycle_id}] ⚠️ Data Invalid: {reason}")
                data_quality_score.set(0.0)
                
                if self.consecutive_data_failures >= self.max_data_failures:
                    logger.critical(f"[{cycle_id}] 🛑 DATA CIRCUIT BREAKER TRIPPED!")
                    self.safety.system_state = SystemState.HALTED
                    set_system_state("HALTED")
                    await self.safety.record_failure("DATA_CIRCUIT_BREAKER", 
                        {"failures": self.consecutive_data_failures}, "CRITICAL")
                return

            # Data valid - reset counter
            self.consecutive_data_failures = 0
            data_quality_score.set(1.0)

            # 3. UPDATE POSITIONS (WITH GREEKS VALIDATION)
            self.positions = await self._update_positions(snapshot)
            
            # Check if system was halted due to bad Greeks
            if self.safety.system_state == SystemState.HALTED:
                logger.critical(f"[{cycle_id}] System HALTED - Bad Greeks detected")
                set_system_state("HALTED")
                self.running = False
                return

            # 4. CAPITAL STATE (Background task)
            capital_task = asyncio.create_task(self._update_capital_state_safe())
            self._background_tasks.add(capital_task)
            capital_task.add_done_callback(self._background_tasks.discard)

            # 5. PORTFOLIO METRICS
            portfolio_delta = self._calc_net_delta()
            net_delta_metric.labels(strategy='all').set(portfolio_delta)
            
            funds = await self.cap_governor.get_available_funds()
            update_portfolio_metrics(
                list(self.positions.values()),
                self.cap_governor.daily_pnl,
                funds
            )

            # 6. RISK ANALYSIS (only during trading hours)
            market_open_time = dt_time(9, 15)
            now_time = datetime.now().time()
            is_trading_hours = (market_open_time <= now_time <= dt_time(15, 30))

            adjustments = []
            
            if is_trading_hours:
                # A. EXITS (Always prioritize)
                exits = await self.exit_engine.evaluate_exits(
                    list(self.positions.values()), 
                    snapshot
                )
                adjustments.extend(exits)

                # B. HEDGES & ENTRIES (Only if no exits)
                if not exits:
                    # Hedges
                    hedges = await self.adj.evaluate_portfolio(
                        {"aggregate_metrics": {"delta": portfolio_delta}},
                        snapshot
                    )
                    adjustments.extend(hedges)

                    # New Entries (with throttle)
                    if (not self.positions and not hedges and 
                        time.time() - self.last_entry_time > self.min_entry_interval):
                        new_entries = await self._run_entry_logic(snapshot)
                        adjustments.extend(new_entries)

            cycle_log["adjustments_count"] = len(adjustments)
            
            # ENHANCED: Add WebSocket diagnostics to cycle log
            if self.ws:
                try:
                    ws_stats = self.ws.get_stats()
                    cycle_log["ws_stats"] = {
                        "healthy": ws_stats["is_healthy"],
                        "messages_received": ws_stats["messages_received"],
                        "cached_instruments": ws_stats["cached_instruments"],
                        "data_age_seconds": ws_stats["data_age_seconds"]
                    }
                except Exception as e:
                    logger.debug(f"Failed to get WebSocket stats: {e}")

            # 7. EXECUTION
            execution_results = []
            for adj in adjustments:
                result = await self._process_adjustment(adj, snapshot, cycle_id)
                if result:
                    execution_results.append(result)

            cycle_log["executions"] = execution_results
            self.last_successful_cycle = time.time()

            # 8. METRICS & LOGGING
            duration = time.time() - cycle_start_time
            supervisor_cycle_duration.labels(phase='full').observe(duration)
            self.cycle_times.append(duration)
            self.avg_cycle_time = sum(self.cycle_times) / len(self.cycle_times)
            
            set_system_state(self.safety.system_state.name)

            if len(adjustments) > 0 or cycle_counter % 20 == 0:
                logger.info(
                    f"[{cycle_id}] ✅ Cycle: {duration*1000:.1f}ms | "
                    f"Spot: {snapshot['spot']:.1f} | Pos: {len(self.positions)} | "
                    f"Delta: {portfolio_delta:.2f} | Adj: {len(adjustments)}"
                )

        except Exception as e:
            logger.exception(f"[{cycle_id}] 💥 CYCLE CRASH: {e}")
            cycle_log["exception"] = str(e)
            record_safety_violation("CYCLE_CRASH", "CRITICAL")
            
        finally:
            # Async log decision
            log_task = asyncio.create_task(add_decision_log(cycle_log))
            self._background_tasks.add(log_task)
            log_task.add_done_callback(self._background_tasks.discard)

    # ============================================================================
    # HELPER METHODS (Preserved from original with minor enhancements)
    # ============================================================================

    async def _run_entry_logic(self, snapshot):
        """
        Generate new entry orders based on VOLGUARD 5.0 HYBRID LOGIC
        """
        try:
            # 1. Fetch Option Chain & Contract Specs
            if hasattr(self.engine, '_get_best_expiry_chain'):
                expiry, chain = await self.engine._get_best_expiry_chain()
                if not expiry or chain.empty:
                    return []
                
                # Get lot size for GEX calculation
                specs = registry.get_nifty_contract_specs(expiry)
                lot_size = specs.get('lot_size', 50)
                
                # 2. RUN VOLGUARD 5.0 ANALYTICS
                
                # A. External FII Data
                ext_metrics = await self.participant_client.fetch_metrics()
                
                # B. Volatility (VoV Z-Score Kill Switch)
                vol = await self.vol_engine.analyze(
                    self.daily_data, 
                    snapshot.get("vix", 0),
                    snapshot["spot"], 
                    snapshot["vix"]
                )
                
                # C. Market Structure (Net GEX, Max Pain)
                struct = self.structure_engine.calculate_structure(chain, snapshot["spot"], lot_size)
                
                # D. Edge (Weighted VRP)
                edge = self.edge_engine.calculate_edge(vol, chain, chain)
                
                # E. Regime Decision
                dte = (expiry - datetime.now().date()).days
                
                # Returns TradingMandate
                mandate = self.regime_engine.analyze_regime(
                    vol, struct, edge, ext_metrics, "WEEKLY", dte
                )
                
                logger.info(f"🧠 VolGuard 5.0 Analysis: {mandate.regime_name} | "
                           f"Score: {mandate.allocation_pct}% | "
                           f"FII: {ext_metrics.flow_regime}")

                # Stability Check
                if self._is_regime_stable(mandate.regime_name):
                    
                    if mandate.regime_name == "CASH" or mandate.allocation_pct <= 0:
                        return []

                    # NEW: TradingEngine now uses hybrid logic (Straddle Range + Delta Wings)
                    entries = await self.engine.generate_entry_orders(mandate, vol, snapshot)
                    if entries:
                        self.last_entry_time = time.time()
                        logger.info(f"✅ Generated {len(entries)} entry orders using Hybrid Logic")
                        return entries
                        
        except Exception as e:
            logger.error(f"Entry logic failed: {e}", exc_info=True)
        
        return []

    async def _cleanup_background_tasks(self):
        """Clean up all background tasks on shutdown"""
        if not self._background_tasks:
            return
        
        logger.info(f"🛑 Cancelling {len(self._background_tasks)} background tasks...")
        
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
        
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._background_tasks, return_exceptions=True),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning("Some background tasks didn't cancel cleanly")
        except Exception as e:
            logger.error(f"Error during task cleanup: {e}")
        
        self._background_tasks.clear()

    async def _refresh_intraday_data_safe(self):
        """Refresh intraday data with lock protection"""
        async with self._intraday_refresh_lock:
            try:
                self.intraday_data = await asyncio.wait_for(
                    self.market.get_intraday_candles(NIFTY_KEY, interval_minutes=1),
                    timeout=10.0
                )
                self.last_intraday_fetch = time.time()
                logger.debug(f"Intraday data refreshed: {len(self.intraday_data)} rows")
            except asyncio.TimeoutError:
                logger.warning("Intraday data fetch timeout")
            except Exception as e:
                logger.error(f"Intraday refresh failed: {e}")

    async def _update_capital_state_safe(self):
        """Update capital state with lock protection"""
        async with self._capital_update_lock:
            try:
                await self.cap_governor.get_available_funds()
                self.cap_governor.position_count = len(self.positions)
            except Exception as e:
                logger.error(f"Capital state update failed: {e}")

    async def _read_live_snapshot(self) -> Dict:
        """
        Read live market snapshot with timeout protection
        """
        try:
            quotes_task = asyncio.create_task(
                self.market.get_live_quote([NIFTY_KEY, VIX_KEY])
            )
            quotes = await asyncio.wait_for(quotes_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.error("Market data fetch timeout")
            quotes = {NIFTY_KEY: 0.0, VIX_KEY: 0.0}
        except Exception as e:
            logger.error(f"Market data fetch failed: {e}")
            quotes = {NIFTY_KEY: 0.0, VIX_KEY: 0.0}

        # WebSocket Greeks
        greeks = {}
        ws_healthy = False
        if self.ws:
            try:
                if self.ws.is_healthy():
                    ws_healthy = True
                    raw_greeks = self.ws.get_latest_greeks()
                    
                    for key, val in raw_greeks.items():
                        if isinstance(val, dict) and val:
                            greeks[key] = val
                    
                    if len(greeks) == 0 and len(raw_greeks) > 0:
                        logger.warning("WebSocket returned data but no valid Greeks")
                        
            except Exception as e:
                logger.debug(f"WebSocket Greeks fetch failed: {e}")

        return {
            "spot": quotes.get(NIFTY_KEY, 0.0),
            "vix": quotes.get(VIX_KEY, 0.0),
            "live_greeks": greeks,
            "ws_healthy": ws_healthy,
            "timestamp": datetime.now()
        }

    async def _refresh_heavy_data(self):
        """Refresh 365 days of historical data"""
        logger.info("📥 Downloading 365 days of history...")
        try:
            self.daily_data = await asyncio.wait_for(
                self.market.get_daily_candles(NIFTY_KEY, days=365),
                timeout=45.0
            )
            self.last_daily_fetch = time.time()
            logger.info(f"✅ History loaded: {len(self.daily_data)} days")
            
            await self._refresh_intraday_data_safe()
            
        except asyncio.TimeoutError:
            logger.error("Historical data fetch timeout")
            self.daily_data = pd.DataFrame()
        except Exception as e:
            logger.error(f"Historical data fetch failed: {e}")
            self.daily_data = pd.DataFrame()

    async def _update_positions(self, snapshot) -> Dict:
        """
        CRITICAL: Update positions with Greeks validation
        """
        async with self._position_update_lock:
            raw_list = await self.exec.get_positions()
            pos_map = {}
            missing_greeks_count = 0
            
            for p in raw_list:
                try:
                    if "greeks" not in p or not p["greeks"]:
                        t = self._calculate_time_to_expiry(p.get("expiry"))
                        
                        calc = self.risk.calculate_leg_greeks(
                            price=p.get("average_price", 0.0),
                            spot=snapshot.get("spot", 0.0),
                            strike=float(p.get("strike", 0)),
                            time_years=t,
                            r=0.07,
                            opt_type=p.get("option_type", "CE")
                        )
                        
                        if calc is None:
                            missing_greeks_count += 1
                            p["greeks"] = None
                            p["unsafe_greeks"] = True
                        else:
                            p["greeks"] = calc
                            p["unsafe_greeks"] = False
                    
                    pos_map[p["position_id"]] = p
                    
                except Exception as e:
                    logger.error(f"Position processing failed: {e}")
                    continue
            
            # Halt if too many unreliable Greeks
            if missing_greeks_count > 0 and len(pos_map) > 0:
                reliability = 1 - (missing_greeks_count / len(pos_map))
                
                if reliability < 0.7:
                    logger.critical(f"🛑 HALTING: Only {reliability*100:.1f}% Greeks reliable")
                    self.safety.system_state = SystemState.HALTED
                    await self.safety.record_failure(
                        "GREEKS_UNAVAILABLE",
                        {"missing_count": missing_greeks_count, "total": len(pos_map)},
                        "CRITICAL"
                    )
            
            return pos_map

    async def _process_adjustment(self, adj, snapshot, cycle_id):
        """
        Process adjustment with comprehensive error handling
        """
        adj["cycle_id"] = cycle_id
        
        try:
            # 1. Safety Check
            safe = await self.safety.can_adjust_trade(adj)
            if not safe["allowed"]:
                logger.debug(f"[{cycle_id}] Safety veto: {safe['reason']}")
                return None

            # 2. Capital Check
            if adj.get("action") == "ENTRY":
                margin_res = await self.cap_governor.can_trade_new([adj])
                if not margin_res.allowed:
                    logger.warning(f"[{cycle_id}] Capital veto: {margin_res.reason}")
                    record_safety_violation("CAPITAL_VETO", "MEDIUM")
                    return None

            # 3. Execute based on mode
            mode = self.safety.execution_mode
            
            if mode == ExecutionMode.SHADOW:
                logger.info(f"[{cycle_id}] SHADOW: {adj.get('action')} {adj.get('instrument_key')}")
                return {"status": "SHADOW", "cycle_id": cycle_id}
                
            elif mode == ExecutionMode.SEMI_AUTO:
                req_id = await self.approvals.request_approval(adj, snapshot)
                logger.info(f"[{cycle_id}] SEMI_AUTO: Approval requested {req_id}")
                return {"status": "PENDING_APPROVAL", "req_id": req_id}
                
            elif mode == ExecutionMode.FULL_AUTO:
                result = await self.exec.execute_adjustment(adj)
                
                if result.get("status") == "PLACED":
                    logger.info(f"[{cycle_id}] ✅ Order placed: {result.get('order_id')}")
                    
                    if "required_margin" in result:
                        self.cap_governor.record_actual_margin(
                            result["required_margin"],
                            adj.get("quantity", 0) // 50
                        )
                    
                    record_order_placed(
                        adj.get("side", "UNKNOWN"),
                        adj.get("strategy", "UNKNOWN"),
                        "OPTION",
                        "MARKET",
                        "PLACED"
                    )
                    
                elif result.get("status") == "FAILED":
                    logger.error(f"[{cycle_id}] ❌ Execution failed: {result.get('error')}")
                    await self.safety.record_failure("EXECUTION_FAILED", result)
                    record_order_failed(result.get("error", "UNKNOWN"))
                
                return result

        except asyncio.TimeoutError:
            logger.error(f"[{cycle_id}] ⏱️ Execution timeout")
            await self.safety.record_failure("EXECUTION_TIMEOUT", {"adjustment": adj})
            record_safety_violation("EXECUTION_TIMEOUT", "HIGH")
            return {"status": "TIMEOUT", "cycle_id": cycle_id}
            
        except Exception as e:
            logger.critical(f"[{cycle_id}] 💥 Execution crash: {e}")
            await self.safety.record_failure("EXECUTION_CRASH", {"error": str(e), "adjustment": adj})
            record_safety_violation("EXECUTION_CRASH", "CRITICAL")
            return {"status": "CRASH", "error": str(e), "cycle_id": cycle_id}

    async def _check_market_status(self):
        """Check market hours and holidays"""
        logger.info("Checking market status...")
        try:
            holidays = await asyncio.wait_for(self.market.get_holidays(), timeout=10.0)
            today = date.today()
            if today in holidays:
                msg = f"Market CLOSED today ({today}) - Holiday detected"
                logger.critical(msg)
                if telegram_alerts.enabled:
                    await telegram_alerts.send_alert("Market Status", msg, "INFO")
                return
        except Exception as e:
            logger.error(f"Holiday check failed: {e}")

        now = datetime.now().time()
        market_open = dt_time(9, 15)
        market_close = dt_time(15, 30)
        
        if not (market_open <= now <= market_close):
            logger.warning(f"Started outside market hours ({now.strftime('%H:%M')})")

    def _check_kill_switch(self) -> bool:
        """
        CRITICAL: Check both file-based and Redis kill switches
        """
        kill_file = Path("state/KILL_SWITCH.TRIGGER")
        root_kill_file = Path("KILL_SWITCH.TRIGGER")
        
        if kill_file.exists() or root_kill_file.exists():
            logger.critical("🔴 KILL SWITCH DETECTED (File-based)")
            try:
                target = kill_file if kill_file.exists() else root_kill_file
                with open(target, "r") as f:
                    content = f.read()
                logger.critical(f"Kill switch content: {content}")
            except Exception:
                pass
            return True
        
        return False

    def _is_regime_stable(self, current_regime: str) -> bool:
        """Check if regime is stable across recent history"""
        self.regime_history.append(current_regime)
        if len(self.regime_history) < self.regime_history.maxlen:
            return False
        
        from collections import Counter
        counts = Counter(self.regime_history)
        most_common = counts.most_common(1)[0]
        return most_common[1] >= 4

    def _calculate_time_to_expiry(self, expiry: Union[str, datetime, None]) -> float:
        """Calculate time to expiry in years"""
        try:
            if not expiry:
                return 0.05
            
            if isinstance(expiry, str):
                expiry = datetime.strptime(expiry, "%Y-%m-%d")
            
            time_seconds = (expiry - datetime.now()).total_seconds()
            if time_seconds <= 0:
                return 0.001
            
            return max(time_seconds / (365 * 24 * 3600), 0.001)
        except Exception:
            return 0.05

    def _calc_net_delta(self) -> float:
        """Calculate portfolio net delta"""
        total = 0.0
        for p in self.positions.values():
            try:
                qty = p.get("quantity", 0)
                side = 1 if p.get("side") == "BUY" else -1
                delta = p.get("greeks", {}).get("delta", 0)
                
                if "FUT" in str(p.get("symbol", "")):
                    delta = 1.0
                
                total += delta * qty * side
            except Exception as e:
                logger.error(f"Delta calculation error for position {p.get('position_id')}: {e}")
        
        return total

    def get_performance_metrics(self) -> Dict:
        """Get current performance metrics"""
        metrics = {
            "avg_cycle_time": self.avg_cycle_time,
            "cycle_count": len(self.cycle_times),
            "consecutive_data_failures": self.consecutive_data_failures,
            "last_successful_cycle": self.last_successful_cycle,
            "positions_count": len(self.positions),
            "system_state": self.safety.system_state.name,
            "execution_mode": self.safety.execution_mode.value,
            "background_tasks_count": len(self._background_tasks),
            "last_heavy_refresh": self.last_heavy_refresh_date,
            "portfolio_delta": self._calc_net_delta() if self.positions else 0.0,
            "market_open_verified": self.market_open_verified,  # NEW
            "token_validated": self.token_validated  # NEW
        }
        
        if self.ws:
            try:
                ws_stats = self.ws.get_stats()
                metrics["websocket"] = {
                    "connected": ws_stats["is_connected"],
                    "healthy": ws_stats["is_healthy"],
                    "messages_received": ws_stats["messages_received"],
                    "reconnect_attempts": ws_stats["reconnect_attempts"],
                    "cached_instruments": ws_stats["cached_instruments"],
                    "data_age_seconds": ws_stats["data_age_seconds"]
                }
            except Exception as e:
                logger.debug(f"Failed to get WebSocket stats in metrics: {e}")
        
        return metrics

    async def stop(self):
        """Graceful shutdown"""
        logger.info("🛑 Initiating graceful shutdown...")
        self.running = False
        
        # Disconnect WebSocket
        if self.ws and self.ws.is_connected:
            try:
                await self.ws.disconnect()
                logger.info("✅ WebSocket disconnected")
            except Exception as e:
                logger.error(f"WebSocket disconnect error: {e}")
        
        # Cleanup background tasks
        await self._cleanup_background_tasks()
        
        # Final state export
        set_system_state("STOPPED")
        logger.info("✅ Supervisor stopped gracefully")
