import asyncio
import time
import logging
from typing import Dict
from datetime import datetime
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

class ProductionTradingSupervisor:
    def __init__(self, market_client, risk_engine, adjustment_engine, trade_executor, trading_engine, websocket_service=None, total_capital=1000000, loop_interval_seconds=3.0):
        self.market = market_client
        self.risk = risk_engine
        self.adj = adjustment_engine
        self.exec = trade_executor
        self.engine = trading_engine
        self.ws = websocket_service
        self.interval = loop_interval_seconds
        self.running = False
        self.positions = {}

    async def start(self):
        logger.info("Supervisor Starting...")
        registry.load_master() # Load Data
        if self.ws: await self.ws.connect()
        self.running = True
        
        while self.running:
            start = time.time()
            try:
                # 1. Market Data
                snapshot = await self._read_data()
                
                # 2. Positions & Greeks (Critical Fix: Dynamic T)
                self.positions = await self._update_positions(snapshot)
                
                # 3. Risk
                risk = await self.risk.run_stress_tests({}, snapshot, self.positions)
                
                # 4. Adjustments (Hedge)
                adjs = await self.adj.evaluate_portfolio({"aggregate_metrics": risk}, snapshot)
                
                # 5. New Entries (Brain)
                if not self.positions:
                    entries = await self.engine.generate_entry_orders({"name": "AGGRESSIVE_SHORT"}, snapshot)
                    adjs.extend(entries)
                
                # 6. Execute
                for adj in adjs:
                    await self.exec.execute_adjustment(adj)
                    
            except Exception as e:
                logger.error(f"Cycle Error: {e}")
            
            await asyncio.sleep(max(0, self.interval - (time.time() - start)))

    async def _read_data(self):
        spot = await self.market.get_spot_price()
        vix = await self.market.get_vix()
        greeks = self.ws.get_latest_greeks() if self.ws else {}
        return {"spot": spot, "vix": vix, "live_greeks": greeks}

    async def _update_positions(self, snapshot):
        raw_pos = await self.exec.get_positions()
        pos_map = {}
        for p in raw_pos:
            # Greeks Calculation
            expiry = p.get("expiry")
            strike = p.get("strike", 0)
            now = datetime.now()
            
            # Dynamic T
            if expiry:
                t = (expiry - now).total_seconds() / (365*24*3600)
            else:
                t = 0.05
            
            greeks = self.risk.calculate_leg_greeks(p['current_price'], snapshot['spot'], strike, t, 0.06, p.get("option_type", "CE"))
            p['greeks'] = greeks
            pos_map[p['position_id']] = p
        return pos_map
