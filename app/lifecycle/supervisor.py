# app/lifecycle/supervisor.py

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict

from app.config import settings, Environment
from app.core.data.quality_gate import DataQualityGate
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.edge import EdgeEngine
from app.core.analytics.regime import RegimeEngine
from app.services.persistence import PersistenceService
from app.services.alert_service import alert_service

logger = logging.getLogger(__name__)

class ProductionTradingSupervisor:
    def __init__(self, market_client, risk_engine, adjustment_engine, 
                 trade_executor, trading_engine, capital_governor, websocket_service, 
                 loop_interval_seconds=3.0):
        
        self.client = market_client
        self.risk = risk_engine
        self.adj = adjustment_engine
        self.executor = trade_executor
        self.trading = trading_engine
        self.cap_governor = capital_governor
        self.ws = websocket_service
        self.interval = loop_interval_seconds
        
        self.gate = DataQualityGate()
        self.persistence = PersistenceService()
        
        # Analytics Cores
        self.vol_engine = VolatilityEngine()
        self.struct_engine = StructureEngine()
        self.edge_engine = EdgeEngine()
        self.regime_engine = RegimeEngine()
        
        # State
        self.regime_history: List[str] = []
        self.is_running = False

    async def start(self):
        logger.info(f"Supervisor Booting... Mode: {settings.ENVIRONMENT}")
        
        # 1. Warmup Data (Load History)
        hist_data = await self.persistence.load_daily_history("NSE_INDEX|Nifty 50", days=365)
        if hist_data.empty:
            logger.critical("Failed to load historical data. Cannot compute Volatility.")
            return

        self.vol_engine.warmup(hist_data)
        logger.info("Volatility Engine Warmed Up.")
        
        # 2. Start Websocket
        if settings.SUPERVISOR_WEBSOCKET_ENABLED:
            await self.ws.connect()
            
        self.is_running = True
        logger.info(">>> SUPERVISOR LOOP STARTED <<<")
        
        while self.is_running:
            start_time = time.time()
            try:
                await self._execute_cycle()
            except Exception as e:
                logger.error(f"Cycle Crash: {e}", exc_info=True)
            
            # Precise Loop Timing
            elapsed = time.time() - start_time
            sleep_time = max(0.0, self.interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def _execute_cycle(self):
        cycle_id = int(time.time())
        
        # 1. Data Ingestion
        snapshot = await self.client.get_market_snapshot("NSE_INDEX|Nifty 50")
        
        # 2. Quality Gate
        is_valid, reason = self.gate.validate_snapshot(snapshot)
        if not is_valid:
            logger.warning(f"Data Gate Rejection: {reason}")
            return

        # 3. Analytics Pipeline
        vol = self.vol_engine.compute_intraday(snapshot)
        struct = self.struct_engine.analyze(snapshot) # PCR, etc
        edge = self.edge_engine.calculate_vrp(vol, snapshot)
        
        # 4. Regime Detection
        regime = self.regime_engine.detect_regime(vol, struct, edge)
        is_stable = self._is_regime_stable(regime.name)

        # 5. STRUCTURED LOGGING (Vital for Shadow Mode Analysis)
        logger.info(
            f"[DECISION] Cycle={cycle_id} | "
            f"Regime={regime.name} (score={regime.score:.2f}) | "
            f"Stable={is_stable} | "
            f"IVP={vol.ivp1y:.1f} | VRP={edge.vrp_pk_w:.2f} | "
            f"Spot={snapshot['spot']}"
        )

        # 6. Trading Logic (Only if Stable)
        if is_stable and settings.ENVIRONMENT != Environment.SHADOW:
            # A. Check for Adjustments (Risk Management)
            positions = await self.executor.get_positions()
            
            # If we have positions, run Adjustment Engine
            if positions:
                adjustments = await self.adj.evaluate(positions, snapshot, vol)
                if adjustments:
                    for adj in adjustments:
                        await self.executor.execute_adjustment(adj)
            
            # B. Check for New Entries (Only if flat)
            else:
                trade_plan = await self.trading.analyze_and_select(
                    regime, snapshot, self.cap_governor
                )
                if trade_plan:
                    logger.info(f"Entry Generated: {trade_plan['strategy']} ({trade_plan['lots']} lots)")
                    
                    # Double Check with Capital Governor before Executing
                    # (Though engine suggests lots, governor validates final payload)
                    if await self.cap_governor.check_capital(trade_plan['orders']):
                        # In FULL_AUTO/SEMI, we execute.
                        # For now, let's assume direct execution in FULL_AUTO
                        if settings.ENVIRONMENT == Environment.FULL_AUTO:
                             # Logic to execute legs would go here
                             # await self.executor.execute_batch(trade_plan['orders'])
                             pass
                        elif settings.ENVIRONMENT == Environment.SEMI_AUTO:
                             # Send to Approval System
                             pass

    def _is_regime_stable(self, current_regime: str) -> bool:
        """
        3-out-of-5 Voting Logic.
        Prevents whipsaw but allows regime transition.
        """
        self.regime_history.append(current_regime)
        
        # Keep sliding window of 5
        if len(self.regime_history) > 5:
            self.regime_history.pop(0)
            
        if len(self.regime_history) < 3:
            return False 
            
        # Check if current regime appears 3+ times in last 5 cycles
        count = self.regime_history.count(current_regime)
        return count >= 3
