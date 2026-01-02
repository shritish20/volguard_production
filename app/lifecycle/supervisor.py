"""
Production Trading Supervisor - The continuous risk loop.
"""
import asyncio
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
import json
from pathlib import Path

from app.lifecycle.safety_controller import SafetyController, SystemState, ExecutionMode
from app.core.risk.capital_governor import CapitalGovernor
from app.services.approval_system import ManualApprovalSystem
from app.lifecycle.emergency_executor import SynchronousEmergencyExecutor
from app.database import AsyncSessionLocal, DecisionJournal

logger = logging.getLogger(__name__)

class ProductionTradingSupervisor:
    """
    The continuous risk loop that orchestrates everything.
    """
    
    def __init__(
        self,
        market_client,
        risk_engine,
        adjustment_engine,
        trade_executor,
        websocket_service=None,
        total_capital: float = 10_00_000,
        loop_interval_seconds: float = 3.0
    ):
        self.market_client = market_client
        self.risk_engine = risk_engine
        self.adjustment_engine = adjustment_engine
        self.trade_executor = trade_executor
        self.websocket_service = websocket_service
        self.loop_interval = loop_interval_seconds
        
        # Safety systems
        self.safety = SafetyController()
        self.capital_governor = CapitalGovernor(total_capital)
        self.approval_system = ManualApprovalSystem()
        self.emergency_executor = SynchronousEmergencyExecutor(trade_executor)
        
        # State
        self.is_running = False
        self.shutdown_requested = False
        
        # Metrics
        self.cycles_completed = 0
        self.cycles_blocked = 0
        self.capital_breaches = 0
        self.approval_required = 0
        self.approval_granted = 0
        self.data_quality = 0.0
        
        # Positions
        self.active_positions: Dict[str, Dict] = {}
        
        # Register safety callbacks
        self.safety.register_state_change_callback(self._on_system_state_change)
    
    async def start(self):
        """Start the continuous supervision loop"""
        logger.info("🚀 Starting Production Trading Supervisor...")
        
        self.is_running = True
        self.safety.set_execution_mode(ExecutionMode.SEMI_AUTO)
        
        if self.websocket_service:
            asyncio.create_task(self.websocket_service.connect())
            await asyncio.sleep(1)
        
        await self._load_initial_positions()
        
        try:
            while self.is_running and not self.shutdown_requested:
                cycle_start = time.time()
                
                try:
                    # ✅ STEP 1: Read market data
                    market_snapshot = await self._read_market_data()
                    
                    # ✅ STEP 2: Check emergency state
                    if not self.emergency_executor.can_proceed():
                        await self._handle_emergency_block()
                        await self._sleep_cycle(cycle_start)
                        continue
                    
                    # ✅ STEP 3: Check data quality and update mode
                    data_quality = self._assess_data_quality(market_snapshot)
                    self._update_execution_mode(data_quality)
                    
                    # ✅ STEP 4: Check if we can proceed
                    can_proceed, reason = self._check_can_proceed(data_quality)
                    
                    if not can_proceed:
                        self.cycles_blocked += 1
                        logger.warning(f"Cycle blocked: {reason}")
                        await self._journal_blocked_cycle(market_snapshot, reason)
                        await self._sleep_cycle(cycle_start)
                        continue
                    
                    # ✅ STEP 5: Update position Greeks
                    updated_positions = await self._update_position_greeks(market_snapshot)
                    
                    # ✅ STEP 6: Recalculate portfolio risk
                    portfolio_risk = await self._calculate_portfolio_risk(updated_positions, market_snapshot)
                    
                    # ✅ STEP 7: Check capital-at-risk
                    capital_metrics, capital_breaches = self.capital_governor.update_portfolio_state(
                        updated_positions,
                        portfolio_risk.get("stress_results", {}),
                        market_snapshot
                    )
                    
                    if capital_breaches:
                        await self._handle_capital_breaches(capital_breaches, portfolio_risk)
                    
                    # ✅ STEP 8: Evaluate adjustments
                    adjustments = await self._evaluate_adjustments(portfolio_risk, market_snapshot)
                    
                    # ✅ STEP 9: Execute through all gates
                    if adjustments:
                        executed = await self._execute_with_gates(adjustments, portfolio_risk, market_snapshot, capital_metrics)
                    
                    # ✅ STEP 10: Journal everything
                    await self._journal_cycle(
                        market_snapshot=market_snapshot,
                        portfolio_risk=portfolio_risk,
                        capital_metrics=capital_metrics,
                        capital_breaches=capital_breaches,
                        adjustments=adjustments,
                        data_quality=data_quality
                    )
                    
                    # Record success
                    await self.safety.record_success()
                    
                except Exception as e:
                    logger.error(f"Cycle failed: {str(e)}", exc_info=True)
                    await self.safety.record_failure("cycle_exception", {"error": str(e)})
                
                # Sleep for remaining interval
                await self._sleep_cycle(cycle_start)
                self.cycles_completed += 1
                
        except asyncio.CancelledError:
            logger.info("Supervisor cancelled")
        finally:
            await self._graceful_shutdown()
    
    async def _read_market_data(self) -> Dict:
        """Read all market data"""
        spot = await self.market_client.get_spot_price()
        vix = await self.market_client.get_vix()
        weekly_chain = await self.market_client.get_option_chain("current_week")
        monthly_chain = await self.market_client.get_option_chain("current_month")
        
        live_greeks = {}
        if self.websocket_service:
            live_greeks = self.websocket_service.get_latest_greeks()
        
        return {
            "timestamp": datetime.utcnow(),
            "spot": spot,
            "vix": vix,
            "weekly_chain": weekly_chain,
            "monthly_chain": monthly_chain,
            "live_greeks": live_greeks,
            "data_source": "websocket" if live_greeks else "rest"
        }
    
    def _assess_data_quality(self, market_snapshot: Dict) -> float:
        """Assess data quality score (0-1)"""
        # Simplified quality assessment
        score = 0.9
        
        if not market_snapshot.get("live_greeks"):
            score *= 0.8
        
        if market_snapshot.get("data_source") == "rest":
            score *= 0.9
        
        self.data_quality = score
        return score
    
    def _update_execution_mode(self, data_quality: float):
        """Update execution mode based on data quality"""
        if data_quality >= 0.8 and self.safety.execution_mode != ExecutionMode.FULL_AUTO:
            self.safety.set_execution_mode(ExecutionMode.FULL_AUTO)
        elif data_quality >= 0.6 and self.safety.execution_mode != ExecutionMode.SEMI_AUTO:
            self.safety.set_execution_mode(ExecutionMode.SEMI_AUTO)
        elif data_quality < 0.6 and self.safety.execution_mode != ExecutionMode.PAPER:
            self.safety.set_execution_mode(ExecutionMode.PAPER)
    
    def _check_can_proceed(self, data_quality: float) -> tuple[bool, str]:
        """Check if we can proceed with trading cycle"""
        safety_status = self.safety.get_safety_status()
        
        if safety_status["system_state"] in ["HALTED", "EMERGENCY", "SHUTDOWN"]:
            return False, f"System state: {safety_status['system_state']}"
        
        if data_quality < 0.3:
            return False, f"Data quality too low: {data_quality}"
        
        if self.safety.execution_mode == ExecutionMode.PAPER:
            return True, "Paper mode - monitoring allowed"
        
        if data_quality < 0.6:
            return False, f"Data quality too low for trading: {data_quality}"
        
        return True, "All checks passed"
    
    async def _update_position_greeks(self, market_snapshot: Dict) -> Dict[str, Dict]:
        """Update Greeks for all positions"""
        updated = {}
        
        for pos_id, position in self.active_positions.items():
            instrument_key = position["instrument_key"]
            greeks = market_snapshot["live_greeks"].get(instrument_key)
            
            if not greeks:
                greeks = await self.market_client.get_greeks(instrument_key)
            
            if not greeks:
                greeks = self._compute_fallback_greeks(position, market_snapshot)
                greeks["source"] = "FALLBACK"
                greeks["confidence"] = 0.3
            
            updated[pos_id] = {
                **position,
                "greeks": greeks,
                "updated_at": datetime.utcnow()
            }
        
        return updated
    
    async def _calculate_portfolio_risk(self, positions: Dict, market_snapshot: Dict) -> Dict:
        """Calculate portfolio risk"""
        aggregate = {
            "delta": 0.0,
            "gamma": 0.0,
            "vega": 0.0,
            "theta": 0.0,
            "pnl": 0.0,
            "margin": 0.0
        }
        
        for position in positions.values():
            direction = 1 if position["side"] == "BUY" else -1
            quantity = position["quantity"]
            greeks = position.get("greeks", {})
            
            aggregate["delta"] += direction * quantity * greeks.get("delta", 0)
            aggregate["gamma"] += direction * quantity * greeks.get("gamma", 0)
            aggregate["vega"] += direction * quantity * greeks.get("vega", 0)
            aggregate["theta"] += direction * quantity * greeks.get("theta", 0)
        
        # Run stress tests
        stress_results = await self.risk_engine.run_stress_tests(aggregate, market_snapshot, positions)
        
        # Check for breaches
        breaches = self.risk_engine.check_breaches(aggregate)
        
        return {
            "timestamp": datetime.utcnow(),
            "aggregate_metrics": aggregate,
            "stress_results": stress_results,
            "breaches": breaches,
            "position_count": len(positions)
        }
    
    async def _evaluate_adjustments(self, portfolio_risk: Dict, market_snapshot: Dict) -> List[Dict]:
        """Evaluate needed adjustments"""
        adjustments = []
        
        # Check portfolio-level adjustments
        portfolio_adjustments = await self.adjustment_engine.evaluate_portfolio(
            portfolio_risk, market_snapshot
        )
        adjustments.extend(portfolio_adjustments)
        
        # Check trade-level adjustments
        active_trades = self.trade_executor.get_active_trades()
        for trade_id, trade in active_trades.items():
            trade_adjustments = await self.adjustment_engine.evaluate_trade(
                trade, portfolio_risk, market_snapshot
            )
            for adj in trade_adjustments:
                adj["trade_id"] = trade_id
                adj["timestamp"] = datetime.utcnow()
                adjustments.append(adj)
        
        return adjustments
    
    async def _execute_with_gates(
        self,
        adjustments: List[Dict],
        portfolio_risk: Dict,
        market_snapshot: Dict,
        capital_metrics: Dict
    ) -> List[Dict]:
        """Execute adjustments through all safety gates"""
        executed = []
        
        for adj in adjustments:
            # Capital gate
            trade_size = self._estimate_trade_size(adj)
            can_trade, reason = self.capital_governor.can_trade_new(trade_size, adj)
            if not can_trade:
                logger.warning(f"Capital gate blocked: {reason}")
                continue
            
            # Safety gate
            safety_check = await self.safety.can_adjust_trade(adj)
            if not safety_check["allowed"]:
                if safety_check.get("required_override") == "MANUAL_APPROVAL":
                    if self.safety.execution_mode == ExecutionMode.SEMI_AUTO:
                        await self.approval_system.request_approval(adj, market_snapshot)
                        self.approval_required += 1
                        logger.info(f"Queued for manual approval: {adj.get('action')}")
                        continue
                
                logger.warning(f"Safety gate blocked: {safety_check['reason']}")
                continue
            
            # Execute
            try:
                result = await self.trade_executor.execute_adjustment(adj)
                executed.append({
                    "adjustment": adj,
                    "result": result,
                    "capital_check": can_trade,
                    "safety_check": safety_check
                })
                
                # Update positions
                await self._load_initial_positions()
                await asyncio.sleep(0.3)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Adjustment execution failed: {e}")
                await self.safety.record_failure("adjustment_execution", {"error": str(e)})
        
        return executed
    
    async def _handle_capital_breaches(self, breaches: List, portfolio_risk: Dict):
        """Handle capital limit breaches"""
        for breach in breaches:
            self.capital_breaches += 1
            logger.warning(f"CAPITAL BREACH: {breach.limit_type} at {breach.breach_percentage:.1f}%")
            
            if breach.action_taken in ["HALT_TRADING", "REDUCE_EXPOSURE_IMMEDIATELY"]:
                emergency_action = {
                    "type": "CAPITAL_RISK_EMERGENCY",
                    "action": breach.action_taken.lower(),
                    "breach_details": breach.__dict__,
                    "portfolio_risk": portfolio_risk
                }
                
                self.emergency_executor.execute_emergency_action(emergency_action)
                
                await self.safety.escalate_state(
                    SystemState.EMERGENCY,
                    f"capital_breach_{breach.limit_type}",
                    breach.__dict__
                )
    
    async def _handle_emergency_block(self):
        """Handle when emergency executor blocks progress"""
        if self.emergency_executor.in_emergency:
            await asyncio.sleep(1)
        else:
            logger.warning("Emergency block cleared")
    
    async def _journal_cycle(
        self,
        market_snapshot: Dict,
        portfolio_risk: Dict,
        capital_metrics: Dict,
        capital_breaches: List,
        adjustments: List[Dict],
        data_quality: float
    ):
        """Journal complete cycle"""
        try:
            # Database journal
            async with AsyncSessionLocal() as session:
                journal = DecisionJournal(
                    cycle_timestamp=datetime.utcnow(),
                    market_snapshot=market_snapshot,
                    portfolio_risk=portfolio_risk,
                    capital_metrics=capital_metrics,
                    adjustments_evaluated=adjustments,
                    safety_status=self.safety.get_safety_status(),
                    data_quality=data_quality,
                    execution_mode=self.safety.execution_mode.value
                )
                session.add(journal)
                await session.commit()
            
            # File backup
            self._backup_cycle_to_file({
                "timestamp": datetime.utcnow().isoformat(),
                "market_spot": market_snapshot.get("spot"),
                "portfolio_delta": portfolio_risk.get("aggregate_metrics", {}).get("delta"),
                "data_quality": data_quality,
                "adjustments_count": len(adjustments),
                "capital_breaches": len(capital_breaches)
            })
            
        except Exception as e:
            logger.error(f"Journaling failed: {e}")
    
    def _backup_cycle_to_file(self, data: Dict):
        """Backup cycle data to file"""
        log_dir = Path("journal")
        log_dir.mkdir(exist_ok=True)
        
        file_path = log_dir / f"cycles_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
        
        with open(file_path, "a") as f:
            f.write(json.dumps(data) + "\n")
    
    def _compute_fallback_greeks(self, position: Dict, market_snapshot: Dict) -> Dict:
        """Compute fallback Greeks"""
        return {
            "delta": position.get("delta", 0.5),
            "gamma": 0.05,
            "vega": 15.0,
            "theta": -5.0,
            "iv": market_snapshot.get("vix", 15.0),
            "source": "FALLBACK",
            "confidence": 0.3
        }
    
    def _estimate_trade_size(self, adjustment: Dict) -> float:
        """Estimate trade size for capital checks"""
        return abs(adjustment.get("quantity", 0)) * adjustment.get("price", 0) * 50
    
    async def _load_initial_positions(self):
        """Load current positions"""
        try:
            positions = await self.trade_executor.get_positions()
            self.active_positions = {p["position_id"]: p for p in positions}
        except Exception as e:
            logger.error(f"Failed to load positions: {e}")
    
    async def _sleep_cycle(self, cycle_start: float):
        """Sleep for remaining cycle time"""
        elapsed = time.time() - cycle_start
        if elapsed < self.loop_interval:
            await asyncio.sleep(self.loop_interval - elapsed)
        else:
            logger.warning(f"Cycle took {elapsed:.2f}s (> {self.loop_interval}s)")
    
    async def _journal_blocked_cycle(self, market_snapshot: Dict, reason: str):
        """Journal blocked cycle"""
        logger.warning(f"Cycle blocked: {reason}")
    
    async def _graceful_shutdown(self):
        """Graceful shutdown"""
        logger.info("Initiating graceful shutdown...")
        self.is_running = False
        
        if self.websocket_service:
            await self.websocket_service.disconnect()
        
        await self.market_client.close()
        logger.info("Supervisor shutdown complete")
    
    def _on_system_state_change(self, old_state, new_state, reason):
        """Handle system state changes"""
        logger.warning(f"System state changed: {old_state.name} → {new_state.name}. Reason: {reason}")
        
        if new_state.priority == SystemState.EMERGENCY.priority:
            asyncio.create_task(self._emergency_shutdown())
    
    async def _emergency_shutdown(self):
        """Emergency shutdown procedure"""
        logger.critical("INITIATING EMERGENCY SHUTDOWN")
        
        try:
            await self.trade_executor.close_all_positions("EMERGENCY")
        except Exception as e:
            logger.error(f"Emergency close failed: {e}")
        
        self.shutdown_requested = True
    
    def get_status(self) -> Dict:
        """Get supervisor status"""
        return {
            "is_running": self.is_running,
            "cycles_completed": self.cycles_completed,
            "cycles_blocked": self.cycles_blocked,
            "data_quality": self.data_quality,
            "active_positions": len(self.active_positions),
            "safety": self.safety.get_safety_status(),
            "capital": self.capital_governor.get_capital_status(),
            "approvals": self.approval_system.get_approval_stats(),
            "emergency": self.emergency_executor.get_emergency_status()
              }
