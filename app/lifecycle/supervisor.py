import asyncio
import time
import logging
import uuid
from typing import Dict
from datetime import datetime
from app.services.instrument_registry import registry
from app.core.data.quality_gate import DataQualityGate
from app.database import add_decision_log
from app.services.alert_service import alert_service
from app.lifecycle.safety_controller import SafetyController, SystemState

logger = logging.getLogger(__name__)

class ProductionTradingSupervisor:
    def __init__(self, market_client, risk_engine, adjustment_engine, 
                 trade_executor, trading_engine, websocket_service=None, 
                 loop_interval_seconds=3.0):
        
        self.market = market_client
        self.risk = risk_engine
        self.adj = adjustment_engine
        self.exec = trade_executor
        self.engine = trading_engine
        self.ws = websocket_service
        
        self.quality = DataQualityGate()
        self.safety = SafetyController() # Internal state machine
        
        self.interval = loop_interval_seconds
        self.running = False
        self.positions = {}
        
    async def start(self):
        logger.info("Supervisor Starting...")
        registry.load_master() 
        if self.ws: await self.ws.connect()
        self.running = True
        
        while self.running:
            cycle_id = str(uuid.uuid4())[:8]
            start_time = time.time()
            action_taken = False
            cycle_log = {"cycle_id": cycle_id, "details": {}}
            
            try:
                # --- PHASE 1: DATA INGESTION ---
                snapshot = await self._read_data()
                
                # Quality Gate
                is_valid, reason = self.quality.validate_snapshot(snapshot)
                if not is_valid:
                    logger.warning(f"[{cycle_id}] Data Invalid: {reason}. Skipping.")
                    # Downgrade system state if repeated
                    await self.safety.record_failure("DATA_QUALITY", {"reason": reason})
                    cycle_log['details']['error'] = reason
                    await self._sleep_rest(start_time)
                    continue

                # Record success to clear degraded states
                await self.safety.record_success()

                # --- PHASE 2: STATE RECONCILIATION ---
                self.positions = await self._update_positions(snapshot)
                
                # --- PHASE 3: RISK ASSESSMENT ---
                risk_report = await self.risk.run_stress_tests({}, snapshot, self.positions)
                
                # Add risk data to log
                cycle_log['risks'] = risk_report.get('WORST_CASE', {})
                cycle_log['spot'] = snapshot['spot']
                cycle_log['vix'] = snapshot['vix']

                # --- PHASE 4: DECISION ENGINE ---
                adjs = []
                
                # A. Hedges (Defensive)
                net_delta = self._calc_net_delta()
                hedges = await self.adj.evaluate_portfolio(
                    {"aggregate_metrics": {"delta": net_delta}}, snapshot
                )
                adjs.extend(hedges)
                
                # B. Entries (Offensive) - Only if safe
                regime_name = "NEUTRAL"
                if not self.positions and not hedges:
                    # Logic to call RegimeEngine would be here. 
                    # Assuming TradingEngine handles internal regime check or we pass it
                    entries = await self.engine.generate_entry_orders({"name": "AGGRESSIVE_SHORT"}, snapshot)
                    adjs.extend(entries)
                    if entries: regime_name = "AGGRESSIVE_SHORT"

                cycle_log['regime'] = regime_name

                # --- PHASE 5: EXECUTION ---
                for adj in adjs:
                    logger.info(f"[{cycle_id}] Executing: {adj}")
                    # Safety check before execute
                    if (await self.safety.can_adjust_trade(adj))['allowed']:
                        await self.exec.execute_adjustment(adj)
                        action_taken = True
                    else:
                        logger.error(f"Safety Controller BLOCKED: {adj}")

                cycle_log['action_taken'] = action_taken

            except Exception as e:
                logger.error(f"[{cycle_id}] Cycle Crash: {e}", exc_info=True)
                await alert_service.send_alert("Supervisor Cycle Crash", str(e), "CRITICAL")
                cycle_log['details']['exception'] = str(e)
                
            finally:
                # --- PHASE 6: JOURNALING (The Memory) ---
                # Save state to DB for audit
                await add_decision_log(cycle_log)
                
                # Maintain Rhythm
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
            # Re-Calculate Greeks
            expiry = p.get("expiry")
            strike = p.get("strike", 0)
            
            t = 0.05
            if isinstance(expiry, datetime):
                 t = max((expiry - datetime.now()).total_seconds() / (365*24*3600), 0.001)
            
            greeks = self.risk.calculate_leg_greeks(
                p['current_price'], snapshot['spot'], strike, t, 0.06, p.get("option_type", "CE")
            )
            p['greeks'] = greeks
            pos_map[p['position_id']] = p
        return pos_map

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
