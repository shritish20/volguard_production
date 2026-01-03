# app/lifecycle/supervisor.py

import asyncio
import time
import logging
import uuid
import os
from typing import Dict, Union
from datetime import datetime, timedelta
from collections import deque

from app.services.instrument_registry import registry
from app.core.data.quality_gate import DataQualityGate
from app.database import add_decision_log
from app.services.alert_service import alert_service
from app.services.telegram_alerts import telegram_alerts
from app.lifecycle.safety_controller import SafetyController, ExecutionMode
from app.core.risk.capital_governor import CapitalGovernor
from app.services.approval_system import ManualApprovalSystem
from app.core.trading.exit_engine import ExitEngine

from app.core.analytics.regime import RegimeEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.edge import EdgeEngine

from app.core.market.market_calendar import MarketCalendar

logger = logging.getLogger(__name__)


class ProductionTradingSupervisor:
    """
    FINAL PRODUCTION SUPERVISOR

    Philosophy:
    - Conservative option selling
    - No firefighting
    - No late adjustments
    - Exit early, sleep peacefully
    """

    def __init__(
        self,
        market_client,
        risk_engine,
        adjustment_engine,
        trade_executor,
        trading_engine,
        websocket_service,
        market_calendar: MarketCalendar,
        loop_interval_seconds: float,
        total_capital: float,
    ):
        # Core services
        self.market = market_client
        self.risk = risk_engine
        self.adj = adjustment_engine
        self.exec = trade_executor
        self.engine = trading_engine
        self.ws = websocket_service
        self.calendar = market_calendar

        # Governance
        self.quality = DataQualityGate()
        self.safety = SafetyController()
        self.cap_governor = CapitalGovernor(total_capital)
        self.approvals = ManualApprovalSystem()

        # Analytics (used only for ENTRY)
        self.exit_engine = ExitEngine()
        self.regime_engine = RegimeEngine()
        self.structure_engine = StructureEngine()
        self.vol_engine = VolatilityEngine()
        self.edge_engine = EdgeEngine()

        # Loop control
        self.interval = loop_interval_seconds
        self.positions: Dict = {}
        self.last_entry_time = 0.0
        self.min_entry_interval = 300  # seconds

        # Regime stability
        self.regime_history = deque(maxlen=5)

    # ==========================================================
    # MAIN LOOP
    # ==========================================================
    async def start(self):
        logger.info(f"Supervisor started | Mode={self.safety.execution_mode.value}")

        registry.load_master()

        if telegram_alerts.enabled:
            await telegram_alerts.send_alert(
                "VolGuard Supervisor",
                f"Started in {self.safety.execution_mode.value}",
                "INFO",
            )

        while True:
            cycle_id = uuid.uuid4().hex[:8]
            start_time = time.time()

            cycle_log = {
                "cycle_id": cycle_id,
                "execution_mode": self.safety.execution_mode.value,
            }

            # ======================================================
            # HARD MARKET GATE
            # ======================================================
            if not self.calendar.is_market_open_now():
                if self.ws:
                    await self.ws.disconnect()
                await asyncio.sleep(60)
                continue

            # ======================================================
            # KILL SWITCH
            # ======================================================
            if os.path.exists("KILL_SWITCH.TRIGGER"):
                reason = open("KILL_SWITCH.TRIGGER").read().strip()
                logger.critical("KILL SWITCH TRIGGERED")

                await self.exec.close_all_positions(reason)
                await alert_service.send_alert(
                    "SYSTEM SHUTDOWN",
                    f"Kill switch: {reason}",
                    "EMERGENCY",
                )
                break

            try:
                # ======================================================
                # SNAPSHOT
                # ======================================================
                snapshot = {
                    "spot": await self.market.get_spot_price(),
                    "vix": await self.market.get_vix(),
                    "live_greeks": self.ws.get_latest_greeks() if self.ws else {},
                }

                valid, reason = self.quality.validate_snapshot(snapshot)
                if not valid:
                    await self.safety.record_failure("DATA", {"reason": reason})
                    cycle_log["error"] = reason
                    await self._sleep(start_time)
                    continue

                # ======================================================
                # POSITIONS
                # ======================================================
                self.positions = await self.exec.get_positions()
                cycle_log["positions"] = len(self.positions)

                # ======================================================
                # FORCED EXPIRY EXIT (NO HEROICS)
                # ======================================================
                expiry_exits = self._expiry_square_off()
                adjustments = expiry_exits.copy()

                # ======================================================
                # NORMAL EXITS (PROFIT / RISK)
                # ======================================================
                exits = await self.exit_engine.evaluate_exits(
                    list(self.positions.values()), snapshot
                )
                adjustments.extend(exits)

                # ======================================================
                # ENTRIES (ONLY WHEN FLAT)
                # ======================================================
                can_enter = (
                    not self.positions
                    and not adjustments
                    and time.time() - self.last_entry_time > self.min_entry_interval
                )

                if can_enter:
                    regime = await self.engine.evaluate_regime(snapshot)
                    self.regime_history.append(regime.name)

                    stable = (
                        len(self.regime_history) == self.regime_history.maxlen
                        and len(set(self.regime_history)) == 1
                    )

                    if stable:
                        entries = await self.engine.generate_entry_orders(
                            regime, snapshot
                        )
                        if entries:
                            self.last_entry_time = time.time()
                            adjustments.extend(entries)

                # ======================================================
                # EXECUTION
                # ======================================================
                for adj in adjustments:
                    adj["cycle_id"] = cycle_id

                    safe = await self.safety.can_adjust_trade(adj)
                    if not safe["allowed"]:
                        continue

                    if self.safety.execution_mode == ExecutionMode.SHADOW:
                        logger.info(f"[{cycle_id}] SHADOW {adj}")

                    elif self.safety.execution_mode == ExecutionMode.SEMI_AUTO:
                        await self.approvals.request_approval(adj, snapshot)

                    elif self.safety.execution_mode == ExecutionMode.FULL_AUTO:
                        result = await self.exec.execute_adjustment(adj)
                        if result.get("status") != "SUCCESS":
                            await self.safety.record_failure(
                                "EXECUTION", result
                            )

                cycle_log["action_taken"] = bool(adjustments)

            except Exception as e:
                logger.exception(f"[{cycle_id}] Supervisor crash")
                cycle_log["exception"] = str(e)

            finally:
                asyncio.create_task(add_decision_log(cycle_log))
                await self._sleep(start_time)

    # ==========================================================
    # EXPIRY SAFETY (NO FIREFIGHTING)
    # ==========================================================
    def _expiry_square_off(self):
        """
        Square off ALL option positions
        1 day before expiry (seller discipline)
        """
        exits = []
        today = datetime.now().date()

        for p in self.positions.values():
            expiry = p.get("expiry")
            if not expiry:
                continue

            if isinstance(expiry, str):
                expiry = datetime.strptime(expiry, "%Y-%m-%d").date()
            elif isinstance(expiry, datetime):
                expiry = expiry.date()

            dte = (expiry - today).days

            if dte <= 1:
                exits.append(
                    {
                        "action": "EXIT",
                        "instrument_key": p["instrument_key"],
                        "quantity": abs(p["quantity"]),
                        "side": "SELL" if p["side"] == "BUY" else "BUY",
                        "strategy": "EXPIRY_EXIT",
                        "reason": "Forced square-off before expiry",
                    }
                )

        return exits

    async def _sleep(self, start_time):
        elapsed = time.time() - start_time
        await asyncio.sleep(max(0, self.interval - elapsed))
