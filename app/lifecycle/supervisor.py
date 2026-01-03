# app/lifecycle/supervisor.py

import asyncio
import time
import logging
import uuid
import os
from typing import Dict, Union
from datetime import datetime
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

from app.core.market.data_client import NIFTY_KEY, VIX_KEY
from app.schemas.analytics import ExtMetrics, VolMetrics, RegimeResult

logger = logging.getLogger(__name__)


class ProductionTradingSupervisor:
    """
    AUTHORITATIVE PRODUCTION SUPERVISOR
    Regime → Strategy → Execution
    Risk-first, capital-aware, execution-safe
    """

    def __init__(
        self,
        market_client,
        risk_engine,
        adjustment_engine,
        trade_executor,
        trading_engine,
        websocket_service=None,
        loop_interval_seconds: float = 3.0,
        total_capital: float = 1_000_000,
    ):
        # Core services
        self.market = market_client
        self.risk = risk_engine
        self.adj = adjustment_engine
        self.exec = trade_executor
        self.engine = trading_engine
        self.ws = websocket_service

        # Safety & governance
        self.quality = DataQualityGate()
        self.safety = SafetyController()
        self.cap_governor = CapitalGovernor(total_capital=total_capital)
        self.approvals = ManualApprovalSystem()

        # Analytics brain
        self.exit_engine = ExitEngine()
        self.regime_engine = RegimeEngine()
        self.structure_engine = StructureEngine()
        self.vol_engine = VolatilityEngine()
        self.edge_engine = EdgeEngine()

        # Loop control
        self.interval = loop_interval_seconds
        self.running = False
        self.positions: Dict = {}
        self.cycle_counter = 0

        # Entry controls
        self.last_entry_time = 0.0
        self.min_entry_interval = 300  # seconds

        # Regime stability protection
        self.regime_history = deque(maxlen=5)
        self.regime_last_change = time.time()

    # ==========================================================
    # MAIN LOOP
    # ==========================================================
    async def start(self):
        logger.info(f"Supervisor starting in {self.safety.execution_mode.value} mode")

        if telegram_alerts.enabled:
            await telegram_alerts.send_alert(
                title="VolGuard Supervisor STARTING",
                message=f"Mode: {self.safety.execution_mode.value}",
                severity="INFO",
            )

        registry.load_master()
        if self.ws:
            await self.ws.connect()

        self.running = True

        while self.running:
            # ======================================================
            # PHASE 0: EMERGENCY KILL SWITCH
            # ======================================================
            if os.path.exists("KILL_SWITCH.TRIGGER"):
                reason = "MANUAL_TRIGGER"
                try:
                    with open("KILL_SWITCH.TRIGGER", "r") as f:
                        reason = f.read().strip()
                except Exception:
                    pass

                logger.critical("KILL SWITCH TRIGGERED")

                if telegram_alerts.enabled:
                    await telegram_alerts.send_emergency_stop_alert(
                        reason, "KILL_SWITCH_FILE"
                    )

                await self.exec.close_all_positions(reason)
                await alert_service.send_alert(
                    "SYSTEM SHUTDOWN", f"Kill switch: {reason}", "EMERGENCY"
                )
                break

            cycle_id = str(uuid.uuid4())[:8]
            start_time = time.time()
            action_taken = False

            cycle_log = {
                "cycle_id": cycle_id,
                "execution_mode": self.safety.execution_mode.value,
            }

            try:
                # ======================================================
                # PHASE 1: DATA & QUALITY
                # ======================================================
                snapshot = await self._read_data()
                valid, reason = self.quality.validate_snapshot(snapshot)

                if not valid:
                    logger.warning(f"[{cycle_id}] Data invalid: {reason}")
                    await self.safety.record_failure("DATA_QUALITY", {"reason": reason})
                    cycle_log["error"] = reason
                    await self._sleep_rest(start_time)
                    continue

                await self.safety.record_success()

                # ======================================================
                # PHASE 2: POSITION RECONCILIATION
                # ======================================================
                self.positions = await self._update_positions(snapshot)
                est_margin = len(self.positions) * 150_000  # placeholder, conservative
                self.cap_governor.update_state(est_margin, len(self.positions))

                # ======================================================
                # PHASE 3: RISK ASSESSMENT (HARD GATE)
                # ======================================================
                risk_report = await self.risk.run_stress_tests(
                    {}, snapshot, self.positions
                )
                worst_case = risk_report.get("WORST_CASE", {}).get("impact", 0.0)

                cycle_log["risk"] = risk_report.get("WORST_CASE", {})

                # Block new entries if stress risk is unacceptable
                stress_block = worst_case < -0.03 * snapshot["spot"]
                if stress_block:
                    logger.warning(
                        f"[{cycle_id}] Stress block active (worst={worst_case:.2f})"
                    )

                # ======================================================
                # PHASE 4: DECISION ENGINE
                # ======================================================
                adjustments = []

                # ---- A. Exits FIRST (always allowed)
                exits = await self.exit_engine.evaluate_exits(
                    list(self.positions.values()), snapshot
                )
                adjustments.extend(exits)

                # ---- B. Hedges & Entries
                if not exits:
                    net_delta = self._calc_net_delta()

                    hedges = await self.adj.evaluate_portfolio(
                        {"aggregate_metrics": {"delta": net_delta}}, snapshot
                    )
                    adjustments.extend(hedges)

                    can_add, cap_msg = self.cap_governor.can_trade_new(
                        150_000, {"strategy": "ENTRY"}
                    )

                    if time.time() - self.last_entry_time < self.min_entry_interval:
                        can_add = False

                    if stress_block:
                        can_add = False

                    if not self.positions and not hedges and can_add:
                        expiry, mo_expiry, lot = await self.market.get_expiries_and_lot()

                        if expiry:
                            nh, vh, wc = await asyncio.gather(
                                self.market.get_history(NIFTY_KEY),
                                self.market.get_history(VIX_KEY),
                                self.market.get_option_chain(expiry),
                            )

                            mc = (
                                await self.market.get_option_chain(mo_expiry)
                                if mo_expiry
                                else None
                            )

                            if not nh.empty and not vh.empty and not wc.empty:
                                vol: VolMetrics = await self.vol_engine.calculate_volatility(
                                    nh, vh, snapshot["spot"], snapshot["vix"]
                                )
                                st = self.structure_engine.analyze_structure(
                                    wc, snapshot["spot"], lot
                                )
                                ed = self.edge_engine.detect_edges(
                                    wc, mc, snapshot["spot"], vol
                                )
                                ext = ExtMetrics(0, 0, 0, [], False)

                                regime: RegimeResult = self.regime_engine.calculate_regime(
                                    vol, st, ed, ext
                                )

                                # ---- Regime stability tracking
                                if (
                                    len(self.regime_history) > 0
                                    and regime.name != self.regime_history[-1]
                                ):
                                    self.regime_last_change = time.time()

                                self.regime_history.append(regime.name)

                                stable = (
                                    len(self.regime_history)
                                    == self.regime_history.maxlen
                                    and len(set(self.regime_history)) == 1
                                )

                                override = (
                                    time.time() - self.regime_last_change > 1800
                                )  # 30 min

                                cycle_log["regime"] = regime.name
                                cycle_log["score"] = regime.score
                                cycle_log["regime_stable"] = stable
                                cycle_log["regime_override"] = override

                                if stable or override:
                                    entries = await self.engine.generate_entry_orders(
                                        regime, vol, snapshot
                                    )
                                    if entries:
                                        self.last_entry_time = time.time()
                                        adjustments.extend(entries)
                                else:
                                    logger.info(
                                        f"[{cycle_id}] Regime unstable → entry skipped"
                                    )

                # ======================================================
                # PHASE 5: EXECUTION
                # ======================================================
                for adj in adjustments:
                    adj["cycle_id"] = cycle_id

                    safe = await self.safety.can_adjust_trade(adj)
                    if not safe["allowed"]:
                        continue

                    is_entry = adj.get("action") == "ENTRY"
                    if is_entry:
                        allowed, cap_msg = self.cap_governor.can_trade_new(
                            150_000, adj
                        )
                        if not allowed:
                            logger.warning(f"Capital veto: {cap_msg}")
                            continue

                    mode = self.safety.execution_mode

                    if mode == ExecutionMode.SHADOW:
                        logger.info(f"[{cycle_id}] SHADOW {adj}")

                    elif mode == ExecutionMode.SEMI_AUTO:
                        await self.approvals.request_approval(adj, snapshot)

                    elif mode == ExecutionMode.FULL_AUTO:
                        result = await self.exec.execute_adjustment(adj)
                        if result.get("status") == "SUCCESS":
                            action_taken = True
                        else:
                            logger.error(
                                f"[{cycle_id}] Execution failed: {result}"
                            )
                            await self.safety.record_failure(
                                "EXECUTION", result
                            )

                cycle_log["action_taken"] = action_taken
                cycle_log["positions"] = len(self.positions)
                cycle_log["entries_attempted"] = any(
                    a.get("action") == "ENTRY" for a in adjustments
                )

                self.cycle_counter += 1

            except Exception as e:
                logger.exception(f"[{cycle_id}] Supervisor crash")
                cycle_log["exception"] = str(e)

            finally:
                asyncio.create_task(add_decision_log(cycle_log))
                await self._sleep_rest(start_time)

    # ==========================================================
    # HELPERS
    # ==========================================================
    async def _read_data(self):
        spot = await self.market.get_spot_price()
        vix = await self.market.get_vix()
        greeks = self.ws.get_latest_greeks() if self.ws else {}
        return {"spot": spot, "vix": vix, "live_greeks": greeks}

    async def _update_positions(self, snapshot):
        raw = await self.exec.get_positions()
        pos_map = {}

        for p in raw:
            t = self._calculate_time_to_expiry(p.get("expiry"))
            if p.get("greeks", {}).get("delta") is None:
                p["greeks"] = self.risk.calculate_leg_greeks(
                    p["current_price"],
                    snapshot["spot"],
                    p.get("strike", 0),
                    t,
                    0.06,
                    p.get("option_type", "CE"),
                )
            pos_map[p["position_id"]] = p

        return pos_map

    def _calculate_time_to_expiry(self, expiry: Union[str, datetime, None]) -> float:
        try:
            if not expiry:
                return 0.05
            if isinstance(expiry, str):
                for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        expiry = datetime.strptime(expiry, fmt)
                        break
                    except ValueError:
                        continue
            if isinstance(expiry, datetime):
                return max(
                    (expiry - datetime.now()).total_seconds()
                    / (365 * 24 * 3600),
                    0.001,
                )
        except Exception:
            pass
        return 0.05

    def _calc_net_delta(self) -> float:
        total = 0.0
        for p in self.positions.values():
            qty = p.get("quantity", 0)
            lot = p.get("lot_size", 50)
            side = 1 if p.get("side") == "BUY" else -1
            delta = p.get("greeks", {}).get("delta", 0)

            if "FUT" in p.get("symbol", ""):
                delta = 1.0

            total += delta * qty * lot * side

        return total

    async def _sleep_rest(self, start_time):
        elapsed = time.time() - start_time
        await asyncio.sleep(max(0, self.interval - elapsed))
