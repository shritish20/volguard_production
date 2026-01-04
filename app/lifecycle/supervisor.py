# app/lifecycle/supervisor.py

import asyncio
import time
import logging
import uuid
import os
import pandas as pd
from typing import Dict, Union, List, Optional
from datetime import datetime, date
from collections import deque

from app.services.instrument_registry import registry
from app.core.data.quality_gate import DataQualityGate
from app.database import add_decision_log
from app.services.alert_service import alert_service
from app.services.telegram_alerts import telegram_alerts
from app.lifecycle.safety_controller import SafetyController, ExecutionMode
from app.core.risk.capital_governor import CapitalGovernor
from app.services.approval_system import ManualApprovalSystem

# Core Engines
from app.core.trading.exit_engine import ExitEngine
from app.core.analytics.regime import RegimeEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.edge import EdgeEngine
from app.core.trading.adjustment_engine import AdjustmentEngine
from app.core.trading.executor import TradeExecutor
from app.core.trading.engine import TradingEngine
from app.core.risk.engine import RiskEngine
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.schemas.analytics import ExtMetrics, VolMetrics, RegimeResult

logger = logging.getLogger(__name__)

class ProductionTradingSupervisor:
    """
    VolGuard Smart Supervisor (VolGuard 3.0)
    
    Orchestrates the "Hybrid Latency" Loop:
    - Boot: Master Clock Check (Holidays)
    - TIER 1 (Daily): Fetch History (Once/Day)
    - TIER 2 (5-Min): Fetch Intraday (Fast Vol)
    - TIER 3 (3-Sec): Live Trading Loop
    """

    def __init__(
        self,
        market_client: MarketDataClient,
        risk_engine: RiskEngine,
        adjustment_engine: AdjustmentEngine,
        trade_executor: TradeExecutor,
        trading_engine: TradingEngine,
        capital_governor: CapitalGovernor,
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
        self.ws = websocket_service

        # Safety & Governance
        self.quality = DataQualityGate()
        self.safety = SafetyController()
        self.approvals = ManualApprovalSystem()

        # Analytics Brain
        self.exit_engine = ExitEngine()
        self.regime_engine = RegimeEngine()
        self.structure_engine = StructureEngine()
        self.vol_engine = VolatilityEngine() # Hybrid Engine
        self.edge_engine = EdgeEngine()

        # Loop Control
        self.interval = loop_interval_seconds
        self.running = False
        self.positions: Dict = {}
        
        # Smart Data Cache (The "Tiered" Data)
        self.daily_data = pd.DataFrame()
        self.intraday_data = pd.DataFrame()
        
        # Timers
        self.last_daily_fetch = 0.0
        self.last_intraday_fetch = 0.0
        self.last_entry_time = 0.0
        
        # Config
        self.min_entry_interval = 300  # 5 mins between new entries
        self.intraday_fetch_interval = 300 # 5 mins
        
        # Regime Stability
        self.regime_history = deque(maxlen=5)
        self.regime_last_change = time.time()

    async def start(self):
        """Main Entry Point"""
        logger.info(f"Supervisor booting in {self.safety.execution_mode.value} mode")
        
        # 1. MASTER CLOCK CHECK (Holidays)
        await self._check_market_status()
        
        # 2. LOAD STATIC DATA
        registry.load_master()
        
        # 3. INITIAL DATA LOAD (Tier 1 & 2)
        await self._refresh_heavy_data()

        if self.ws:
            await self.ws.connect()
            
        self.running = True
        
        # ==================================================================
        # THE MAIN LOOP
        # ==================================================================
        while self.running:
            start_time = time.time()
            cycle_id = str(uuid.uuid4())[:8]
            cycle_log = {"cycle_id": cycle_id, "mode": self.safety.execution_mode.value}
            
            # PHASE 0: KILL SWITCH CHECK
            if self._check_kill_switch():
                break

            try:
                # PHASE 1: SMART DATA REFRESH
                # Poll Intraday candles every 5 mins (Non-blocking)
                if time.time() - self.last_intraday_fetch > self.intraday_fetch_interval:
                    asyncio.create_task(self._refresh_intraday_data())

                # Read Live Snapshot (LTP)
                snapshot = await self._read_live_snapshot()
                
                # Quality Gate
                valid, reason = self.quality.validate_snapshot(snapshot)
                if not valid:
                    logger.warning(f"[{cycle_id}] Data Invalid: {reason}")
                    await self.safety.record_failure("DATA_QUALITY", {"reason": reason})
                    cycle_log["error"] = reason
                    await self._sleep_rest(start_time)
                    continue

                await self.safety.record_success()

                # PHASE 2: POSITIONS & FUNDS
                # Update positions from V2 API
                self.positions = await self._update_positions(snapshot)
                
                # Check real funds (V2) - Can run async to not block
                # We update state, but don't block loop if it takes 200ms
                asyncio.create_task(self._update_capital_state())

                # PHASE 3: RISK SCAN (Stress Test)
                risk_report = await self.risk.run_stress_tests({}, snapshot, self.positions)
                worst_case = risk_report.get("WORST_CASE", {}).get("impact", 0.0)
                
                stress_block = False
                if snapshot["spot"] > 0 and worst_case < -0.03 * snapshot["spot"]: # 3% Portfolio Risk
                    stress_block = True
                    logger.warning(f"[{cycle_id}] STRESS BLOCK ACTIVE (Worst: {worst_case:.2f})")

                # PHASE 4: DECISION ENGINE
                adjustments = []

                # A. Exits (Always allowed)
                exits = await self.exit_engine.evaluate_exits(
                    list(self.positions.values()), snapshot
                )
                adjustments.extend(exits)

                # B. Hedges & Entries
                if not exits:
                    # Hedges (Adjustment Engine)
                    net_delta = self._calc_net_delta()
                    hedges = await self.adj.evaluate_portfolio(
                        {"aggregate_metrics": {"delta": net_delta}}, snapshot
                    )
                    adjustments.extend(hedges)
                    
                    # Entries (Trading Engine)
                    # Check "soft" limits first before doing heavy calculation
                    can_enter_soft = (
                        not self.positions and 
                        not hedges and 
                        not stress_block and
                        (time.time() - self.last_entry_time > self.min_entry_interval)
                    )

                    if can_enter_soft:
                        # Fetch Rich Data (V2 Option Chain)
                        expiry, chain = await self.engine._get_best_expiry_chain()
                        
                        if expiry and not chain.empty:
                            # 1. Volatility (The Hybrid Calculation)
                            vol: VolMetrics = await self.vol_engine.calculate_volatility(
                                self.daily_data, 
                                self.intraday_data, 
                                snapshot["spot"], 
                                snapshot["vix"]
                            )
                            
                            # 2. Structure & Edge
                            lot_size = 50 # Default, engine handles dynamic
                            st = self.structure_engine.analyze_structure(chain, snapshot["spot"], lot_size)
                            # Passing None for monthly chain for simplicity in this cycle
                            ed = self.edge_engine.detect_edges(chain, chain, snapshot["spot"], vol)
                            
                            # 3. Regime
                            ext = ExtMetrics(0, 0, 0, [], False) # Placeholder for External News
                            regime: RegimeResult = self.regime_engine.calculate_regime(vol, st, ed, ext)
                            
                            # Log Regime
                            cycle_log["regime"] = regime.name
                            cycle_log["score"] = regime.score
                            
                            # 4. Generate Entries (Orchestrator)
                            if self._is_regime_stable(regime.name):
                                entries = await self.engine.generate_entry_orders(regime, vol, snapshot)
                                if entries:
                                    self.last_entry_time = time.time()
                                    adjustments.extend(entries)

                # PHASE 5: EXECUTION (With Real Margin Check)
                for adj in adjustments:
                    adj["cycle_id"] = cycle_id
                    
                    # 1. Safety Controller Check
                    safe = await self.safety.can_adjust_trade(adj)
                    if not safe["allowed"]:
                        continue

                    # 2. Capital Governor Check (REAL MARGIN API)
                    # We pass a list of 1 leg because adjustments are usually single actions
                    # unless it's a multi-leg entry
                    legs_to_check = [adj] 
                    # If this is an entry with multiple legs, 'entries' from engine is a list
                    # logic here assumes linear processing, but Governor handles lists.
                    
                    is_entry = adj.get("action") == "ENTRY"
                    if is_entry:
                        # Check Real Margin Requirement
                        margin_res = await self.cap_governor.can_trade_new(legs_to_check)
                        if not margin_res.allowed:
                            logger.warning(f"[{cycle_id}] Capital Veto: {margin_res.reason}")
                            continue

                    # 3. Execute
                    mode = self.safety.execution_mode
                    
                    if mode == ExecutionMode.SHADOW:
                        logger.info(f"[{cycle_id}] SHADOW EXEC: {adj}")
                        
                    elif mode == ExecutionMode.SEMI_AUTO:
                        await self.approvals.request_approval(adj, snapshot)
                        
                    elif mode == ExecutionMode.FULL_AUTO:
                        result = await self.exec.execute_adjustment(adj)
                        if result.get("status") == "PLACED":
                            logger.info(f"[{cycle_id}] Order Placed: {result.get('order_id')}")
                        else:
                            logger.error(f"Execution Failed: {result}")
                            await self.safety.record_failure("EXECUTION_ERROR", result)

            except Exception as e:
                logger.exception(f"[{cycle_id}] Supervisor Cycle Crash")
                cycle_log["exception"] = str(e)
            finally:
                # Log decision asynchronously
                asyncio.create_task(add_decision_log(cycle_log))
                await self._sleep_rest(start_time)

    # ==================================================================
    # HELPER METHODS
    # ==================================================================

    async def _check_market_status(self):
        """Master Clock: Checks holidays on boot"""
        logger.info("Checking Market Status (Holidays)...")
        holidays = await self.market.get_holidays()
        today = date.today()
        
        if today in holidays:
            msg = f"Market is CLOSED today ({today}) for Holiday. Shutting down."
            logger.critical(msg)
            if telegram_alerts.enabled:
                await telegram_alerts.send_alert("Market Status", msg, "INFO")
            exit(0) # Clean exit
        
        # Check Time (9:15 - 15:30)
        now = datetime.now().time()
        market_open = datetime.strptime("09:15", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()
        
        if not (market_open <= now <= market_close):
            logger.warning("Supervisor started outside market hours.")
            # We don't exit, we just warn (allows for testing)

    async def _refresh_heavy_data(self):
        """Fetches Tier 1 (Daily) and Tier 2 (Intraday) data"""
        logger.info("Refreshing Historical Data...")
        # Fetch Daily (Yearly context)
        self.daily_data = await self.market.get_daily_candles(NIFTY_KEY, days=365)
        self.last_daily_fetch = time.time()
        
        # Fetch Intraday (Today's context)
        await self._refresh_intraday_data()

    async def _refresh_intraday_data(self):
        """Background task for 5-min candles"""
        try:
            self.intraday_data = await self.market.get_intraday_candles(NIFTY_KEY, interval_minutes=1)
            self.last_intraday_fetch = time.time()
            # logger.debug("Intraday data refreshed")
        except Exception as e:
            logger.error(f"Background Intraday Refresh Failed: {e}")

    async def _read_live_snapshot(self) -> Dict:
        """Reads V3 LTP"""
        quotes = await self.market.get_live_quote([NIFTY_KEY, VIX_KEY])
        
        # Merge with WebSocket Greeks if available
        greeks = {}
        if self.ws and self.ws.is_healthy():
            greeks = self.ws.get_latest_greeks()
            
        return {
            "spot": quotes.get(NIFTY_KEY, 0.0),
            "vix": quotes.get(VIX_KEY, 0.0),
            "live_greeks": greeks,
            "timestamp": datetime.now()
        }

    async def _update_capital_state(self):
        """Updates funds in background"""
        funds = await self.cap_governor.get_available_funds()
        self.cap_governor.update_position_count(len(self.positions))

    async def _update_positions(self, snapshot) -> Dict:
        """Fetches V2 Positions"""
        raw_list = await self.exec.get_positions()
        pos_map = {}
        for p in raw_list:
            # If Greeks missing (V2 doesn't give greeks), calculate them using Risk Engine
            if "greeks" not in p:
                t = self._calculate_time_to_expiry(p.get("expiry"))
                p["greeks"] = self.risk.calculate_leg_greeks(
                    p["current_price"], 
                    snapshot["spot"], 
                    p.get("strike", 0), 
                    t, 
                    0.06, 
                    p.get("option_type", "CE")
                )
            pos_map[p["position_id"]] = p
        return pos_map

    def _check_kill_switch(self) -> bool:
        if os.path.exists("KILL_SWITCH.TRIGGER"):
            logger.critical("KILL SWITCH DETECTED. Stopping Loop.")
            return True
        return False

    def _is_regime_stable(self, current_regime: str) -> bool:
        """Prevents whipsaws"""
        self.regime_history.append(current_regime)
        if len(self.regime_history) < self.regime_history.maxlen:
            return False
        return len(set(self.regime_history)) == 1

    def _calculate_time_to_expiry(self, expiry: Union[str, datetime, None]) -> float:
        try:
            if not expiry: return 0.05
            if isinstance(expiry, str):
                expiry = datetime.strptime(expiry, "%Y-%m-%d")
            return max((expiry - datetime.now()).total_seconds() / (365 * 24 * 3600), 0.001)
        except:
            return 0.05

    def _calc_net_delta(self) -> float:
        total = 0.0
        for p in self.positions.values():
            qty = p.get("quantity", 0)
            side = 1 if p.get("side") == "BUY" else -1
            delta = p.get("greeks", {}).get("delta", 0)
            if "FUT" in str(p.get("symbol", "")): delta = 1.0
            total += delta * qty * side # Lot size usually handled in qty from Executor
        return total

    async def _sleep_rest(self, start_time):
        elapsed = time.time() - start_time
        await asyncio.sleep(max(0, self.interval - elapsed))
