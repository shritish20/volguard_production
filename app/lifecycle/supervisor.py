import asyncio
import time
import logging
import uuid
import os
from typing import Dict, Optional, Union
from datetime import datetime
from app.services.instrument_registry import registry
from app.core.data.quality_gate import DataQualityGate
from app.database import add_decision_log
from app.services.alert_service import alert_service
from app.lifecycle.safety_controller import SafetyController, ExecutionMode
from app.core.risk.capital_governor import CapitalGovernor
from app.services.approval_system import ManualApprovalSystem

logger = logging.getLogger(__name__)

class ProductionTradingSupervisor:
    def __init__(self, market_client, risk_engine, adjustment_engine, 
                 trade_executor, trading_engine, websocket_service=None, 
                 loop_interval_seconds=3.0, total_capital=1000000):
        
        self.market = market_client
        self.risk = risk_engine
        self.adj = adjustment_engine
        self.exec = trade_executor
        self.engine = trading_engine
        self.ws = websocket_service
        
        # Safety & Governance
        self.quality = DataQualityGate()
        self.safety = SafetyController() 
        self.cap_governor = CapitalGovernor(total_capital=total_capital)
        self.approvals = ManualApprovalSystem()
        
        self.interval = loop_interval_seconds
        self.running = False
        self.positions = {}
        
    async def start(self):
        logger.info(f"Supervisor Starting in {self.safety.execution_mode.value} mode...")
        registry.load_master() 
        if self.ws: await self.ws.connect()
        self.running = True
        
        while self.running:
            # --- PHASE 0: EMERGENCY OVERRIDE ---
            if os.path.exists("KILL_SWITCH.TRIGGER"):
                logger.critical("KILL SWITCH DETECTED. SHUTTING DOWN.")
                try:
                    with open("KILL_SWITCH.TRIGGER", "r") as f: reason = f.read().strip()
                except: reason = "MANUAL_TRIGGER"
                await self.exec.close_all_positions(reason)
                self.running = False
                await alert_service.send_alert("SYSTEM SHUTDOWN", f"Kill switch: {reason}", "EMERGENCY")
                break

            cycle_id = str(uuid.uuid4())[:8]
            start_time = time.time()
            action_taken = False
            cycle_log = {"cycle_id": cycle_id, "details": {}, "mode": self.safety.execution_mode.value}
            
            try:
                # --- PHASE 1: DATA & QUALITY ---
                snapshot = await self._read_data()
                is_valid, reason = self.quality.validate_snapshot(snapshot)
                
                if not is_valid:
                    logger.warning(f"[{cycle_id}] Data Invalid: {reason}. Skipping.")
                    await self.safety.record_failure("DATA_QUALITY", {"reason": reason})
                    cycle_log['details']['error'] = reason
                    await self._sleep_rest(start_time)
                    continue

                await self.safety.record_success()

                # --- PHASE 2: RECONCILIATION ---
                self.positions = await self._update_positions(snapshot)
                
                # Update Capital Governor with current utilization
                # Assuming simplified margin tracking (e.g. 1.5L per lot)
                est_margin_used = len(self.positions) * 150000 
                self.cap_governor.update_state(est_margin_used, len(self.positions))

                # --- PHASE 3: RISK ASSESSMENT ---
                risk_report = await self.risk.run_stress_tests({}, snapshot, self.positions)
                
                # Check for Margin/Risk Breaches
                worst_case = risk_report.get('WORST_CASE', {}).get('impact', 0)
                cycle_log['risks'] = {'worst_case': worst_case, 'spot': snapshot['spot']}

                # --- PHASE 4: DECISION ENGINE ---
                adjs = []
                
                # A. Defensive Hedges (Always active in FULL/SEMI/SHADOW to log intent)
                net_delta = self._calc_net_delta()
                hedges = await self.adj.evaluate_portfolio(
                    {"aggregate_metrics": {"delta": net_delta}}, snapshot
                )
                adjs.extend(hedges)
                
                # B. Offensive Entries
                regime_name = "NEUTRAL"
                # Only look for entries if we have room and aren't hedging
                can_add, _ = self.cap_governor.can_trade_new(150000, {}) # Pre-check
                
                if not self.positions and not hedges and can_add:
                    entries = await self.engine.generate_entry_orders({"name": "AGGRESSIVE_SHORT"}, snapshot)
                    if entries: 
                        regime_name = "AGGRESSIVE_SHORT"
                        adjs.extend(entries)

                cycle_log['regime'] = regime_name

                # --- PHASE 5: EXECUTION GATEKEEPER ---
                # This logic fixes the "Trading in Shadow Mode" risk
                
                for adj in adjs:
                    adj['cycle_id'] = cycle_id
                    
                    # 1. Check System Safety (Halted?)
                    safety_check = await self.safety.can_adjust_trade(adj)
                    if not safety_check['allowed']:
                        logger.error(f"Blocked by Safety: {safety_check['reason']}")
                        continue

                    # 2. Capital Check
                    # We skip check if it's a CLOSE/HEDGE (reducing risk), only check for new ENTRIES
                    is_entry = adj.get('side') == 'SELL' and adj.get('quantity') > 0 # Simplified
                    if is_entry:
                        allowed, cap_msg = self.cap_governor.can_trade_new(150000, adj)
                        if not allowed:
                            logger.warning(f"Blocked by Capital Governor: {cap_msg}")
                            continue

                    # 3. Execution Mode Logic
                    mode = self.safety.execution_mode
                    
                    if mode == ExecutionMode.SHADOW:
                        logger.info(f"[{cycle_id}] SHADOW: Would have executed {adj}")
                        cycle_log['shadow_trade'] = adj
                        
                    elif mode == ExecutionMode.SEMI_AUTO:
                        logger.info(f"[{cycle_id}] SEMI_AUTO: Requesting Approval for {adj}")
                        req_id = await self.approvals.request_approval(adj, snapshot)
                        await alert_service.send_alert("APPROVAL NEEDED", f"Trade Pending: {adj}", "WARNING")
                        
                    elif mode == ExecutionMode.FULL_AUTO:
                        logger.info(f"[{cycle_id}] FULL_AUTO: Executing {adj}")
                        await self.exec.execute_adjustment(adj)
                        action_taken = True

                cycle_log['action_taken'] = action_taken

            except Exception as e:
                logger.error(f"[{cycle_id}] Cycle Crash: {e}", exc_info=True)
                await alert_service.send_alert("Cycle Crash", str(e), "CRITICAL")
                cycle_log['details']['exception'] = str(e)
                
            finally:
                # --- PHASE 6: JOURNALING ---
                await add_decision_log(cycle_log)
                await self._sleep_rest(start_time)

    async def _read_data(self):
        spot = await self.market.get_spot_price()
        vix = await self.market.get_vix()
        greeks = self.ws.get_latest_greeks() if self.ws else {}
        return {"spot": spot, "vix": vix, "live_greeks": greeks}

    async def _update_positions(self, snapshot):
        raw_pos = await self.exec.get_positions()
        pos_map = {}
        for p in raw_pos:
            # ROBUST Expiry Parsing (Fixes the 8% Gap)
            t = self._calculate_time_to_expiry(p.get("expiry"))
            
            # Recalculate Greeks
            greeks = self.risk.calculate_leg_greeks(
                p['current_price'], snapshot['spot'], p.get("strike", 0), t, 0.06, p.get("option_type", "CE")
            )
            p['greeks'] = greeks
            pos_map[p['position_id']] = p
        return pos_map

    def _calculate_time_to_expiry(self, expiry: Union[str, datetime, None]) -> float:
        """Safe Time Calculation that never crashes"""
        try:
            if not expiry: return 0.05 # Default 
            
            if isinstance(expiry, str):
                # Try common formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"]:
                    try:
                        expiry = datetime.strptime(expiry, fmt)
                        break
                    except ValueError: continue
            
            if isinstance(expiry, datetime):
                delta = (expiry - datetime.now()).total_seconds()
                years = delta / (365 * 24 * 3600)
                return max(years, 0.001) # Minimum 1/1000th of year
                
            return 0.05
        except Exception:
            return 0.05

    def _calc_net_delta(self):
        total_delta = 0
        for pid, p in self.positions.items():
            qty = p['quantity'] 
            side = 1 if p['side'] == 'BUY' else -1
            d = p.get('greeks', {}).get('delta', 0)
            if 'FUT' in p.get('symbol', ''): d = 1.0
            total_delta += (d * qty * side)
        return total_delta

    async def _sleep_rest(self, start_time):
        elapsed = time.time() - start_time
        sleep_time = max(0, self.interval - elapsed)
        await asyncio.sleep(sleep_time)
