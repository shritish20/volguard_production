# app/lifecycle/safety_controller.py

import asyncio
import logging
import httpx
from enum import Enum, auto
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ==== SYSTEM STATES ====
class SystemState(Enum):
    NORMAL = auto()
    DEGRADED = auto()
    HALTED = auto()
    EMERGENCY = auto()
    SHUTDOWN = auto()

    def priority(self) -> int:
        return {
            SystemState.NORMAL: 0,
            SystemState.DEGRADED: 1,
            SystemState.HALTED: 2,
            SystemState.EMERGENCY: 3,
            SystemState.SHUTDOWN: 4,
        }[self]

# ==== EXECUTION MODES ====
class ExecutionMode(Enum):
    SHADOW = "shadow"
    SEMI_AUTO = "semi_auto"
    FULL_AUTO = "full_auto"

# ==== VIOLATION RECORD ====
@dataclass
class SafetyViolation:
    timestamp: datetime
    violation_type: str
    severity: str
    details: Dict
    resolved: bool = False
    resolution: Optional[str] = None

# ==== SAFETY CONTROLLER ====
class SafetyController:
    """
    Final authority on whether trades may be executed.
    
    NEW FUNCTIONALITY:
    1. 🚨 PANIC BUTTON: trigger_full_stop() - Atomic exit all positions
    2. 🔄 MARGIN AUDIT: Integration with CapitalGovernor
    3. 📊 ENHANCED MONITORING: Real-time system health
    
    Protects against system, data, and execution failures.
    """

    def __init__(self, token_manager=None, capital_governor=None, trade_executor=None):
        """
        Initialize SafetyController with dependencies
        
        Args:
            token_manager: TokenManager for API authentication
            capital_governor: CapitalGovernor for margin checks
            trade_executor: TradeExecutor for emergency exits
        """
        self.system_state = SystemState.NORMAL
        self.execution_mode = ExecutionMode.SHADOW  # Default SAFE
        self.violation_history: List[SafetyViolation] = []
        self._state_lock = asyncio.Lock()
        self.consecutive_failures = 0
        
        # Dependencies (optional)
        self.token_manager = token_manager
        self.capital_governor = capital_governor
        self.trade_executor = trade_executor
        
        # Emergency tracking
        self.emergency_triggered = False
        self.last_emergency_time = None
        self.emergency_reason = ""
        
        # Rate limiting
        self.last_panic_time = None
        self.panic_cooldown = 60  # 1 minute cooldown between panic triggers
        
        # Health metrics
        self.health_checks_passed = 0
        self.health_checks_failed = 0
        self.last_health_check = None

    # ---- PANIC BUTTON / EMERGENCY STOP ----
    async def trigger_full_stop(self, reason: str):
        """
        🚨 EMERGENCY HALT: Atomic exit all positions
        
        This is the "Nuclear Button" - exits ALL positions immediately
        regardless of P&L, then halts the system.
        
        Args:
            reason: Reason for emergency stop (e.g., "MARGIN_MISMATCH", "MANUAL_TRIGGER")
        """
        logger.critical(f"🚨🚨🚨 EMERGENCY HALT TRIGGERED: {reason}")
        
        # Check cooldown
        if self.last_panic_time and (datetime.now() - self.last_panic_time).seconds < self.panic_cooldown:
            logger.warning(f"Panic button cooldown active. Last trigger: {self.last_panic_time}")
            return
        
        # Update state immediately
        self.system_state = SystemState.EMERGENCY
        self.execution_mode = ExecutionMode.SHADOW
        self.emergency_triggered = True
        self.last_emergency_time = datetime.now()
        self.emergency_reason = reason
        self.last_panic_time = datetime.now()
        
        # Record emergency
        await self.record_failure(
            violation_type="EMERGENCY_STOP",
            details={"reason": reason, "timestamp": datetime.now().isoformat()},
            severity="CRITICAL"
        )
        
        # ==== ATOMIC EXIT SEQUENCE ====
        try:
            logger.critical("🔄 Initiating ATOMIC EXIT of all positions...")
            
            # Method 1: Use TradeExecutor if available
            if self.trade_executor:
                logger.info("Using TradeExecutor for atomic exit...")
                exit_result = await self.trade_executor.exit_all_positions(reason="EMERGENCY_HALT")
                
                if exit_result.get("success"):
                    logger.info(f"✅ Atomic exit via TradeExecutor: {exit_result}")
                else:
                    logger.error(f"❌ TradeExecutor exit failed: {exit_result}")
                    # Fall back to direct API
                    await self._direct_atomic_exit()
                    
            else:
                # Method 2: Direct API call
                await self._direct_atomic_exit()
                
        except Exception as e:
            logger.critical(f"💥 Atomic exit failed: {e}")
            # Even if exit fails, we stay in emergency state
            
        # Final state
        logger.critical(f"🛑 SYSTEM IN EMERGENCY STATE. Reason: {reason}")
        logger.critical("All trading halted. Manual intervention required.")

    async def _direct_atomic_exit(self):
        """
        Direct API call to exit all positions
        Uses Upstox's bulk exit endpoint if available
        """
        if not self.token_manager:
            logger.error("No token manager for direct atomic exit")
            return
            
        try:
            headers = self.token_manager.get_headers()
            
            # Method A: Try Upstox's exit-all endpoint
            exit_url = "https://api.upstox.com/v2/order/positions/exit"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Exit by segment and tag
                params = {
                    'segment': 'SEC',
                    'tag': 'VolGuard'
                }
                
                response = await client.post(
                    exit_url,
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"✅ Atomic Exit API Success: {result.get('status', 'UNKNOWN')}")
                    logger.info(f"Exited positions: {result.get('data', {}).get('exited_count', 0)}")
                elif response.status_code == 400:
                    # Try alternative method - close each position individually
                    logger.warning("Bulk exit failed, trying individual position exit...")
                    await self._exit_positions_individually(headers)
                else:
                    logger.error(f"Exit API error: {response.status_code} - {response.text}")
                    
        except Exception as e:
            logger.error(f"Direct atomic exit failed: {e}")

    async def _exit_positions_individually(self, headers: Dict):
        """
        Fallback: Exit each position individually
        """
        try:
            # Get current positions
            positions_url = "https://api.upstox.com/v2/portfolio/short-term-positions"
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                pos_response = await client.get(positions_url, headers=headers)
                
                if pos_response.status_code == 200:
                    positions = pos_response.json().get('data', [])
                    
                    # Filter for open positions
                    open_positions = [p for p in positions if p.get('quantity', 0) != 0]
                    
                    logger.info(f"Exiting {len(open_positions)} positions individually...")
                    
                    # Exit each position
                    exit_count = 0
                    for pos in open_positions:
                        exit_order = {
                            "instrument_token": pos.get('instrument_token'),
                            "transaction_type": "SELL" if pos.get('quantity', 0) > 0 else "BUY",
                            "quantity": abs(pos.get('quantity', 0)),
                            "order_type": "MARKET",
                            "product": "I",
                            "validity": "DAY",
                            "disclosed_quantity": 0,
                            "trigger_price": 0,
                            "tag": "EMERGENCY_EXIT"
                        }
                        
                        order_url = "https://api.upstox.com/v2/order/place"
                        order_response = await client.post(order_url, json=exit_order, headers=headers)
                        
                        if order_response.status_code == 200:
                            exit_count += 1
                        else:
                            logger.warning(f"Failed to exit position {pos.get('instrument_token')}")
                    
                    logger.info(f"✅ Exited {exit_count}/{len(open_positions)} positions")
                    
        except Exception as e:
            logger.error(f"Individual position exit failed: {e}")

    # ---- ENHANCED GATEKEEPER ----
    async def can_adjust_trade(self, adjustment: Dict) -> Dict:
        """
        Enhanced gatekeeper with emergency state checks
        
        Returns:
            Dict with 'allowed' boolean and 'reason' string
        """
        action = adjustment.get("action", "")
        strategy = adjustment.get("strategy", "")
        
        # Emergency state check
        if self.system_state == SystemState.EMERGENCY:
            return {
                "allowed": False,
                "reason": f"SYSTEM EMERGENCY - {self.emergency_reason}",
                "blocking_state": "EMERGENCY"
            }

        # Always allow safety exits (even in degraded/halted states)
        if action in ["EXIT", "CLOSE", "EMERGENCY_EXIT"] or strategy in ["HEDGE", "KILL_SWITCH", "SAFETY"]:
            return {
                "allowed": True,
                "reason": "Safety Action Allowed",
                "priority": "HIGH"
            }

        # Block all trades in HALTED state
        if self.system_state == SystemState.HALTED:
            return {
                "allowed": False,
                "reason": f"System HALTED - Requires manual reset",
                "blocking_state": "HALTED"
            }

        # Block all trades in SHADOW mode (except safety actions)
        if self.execution_mode == ExecutionMode.SHADOW:
            return {
                "allowed": False,
                "reason": "Execution Mode = SHADOW (Monitor Only)",
                "blocking_mode": "SHADOW"
            }
        
        # Additional checks for DEGRADED state
        if self.system_state == SystemState.DEGRADED:
            # Allow only defensive trades in degraded state
            if action == "ENTRY" and strategy not in ["IRON_FLY", "DEFENSIVE"]:
                return {
                    "allowed": False,
                    "reason": "System DEGRADED - Only defensive strategies allowed",
                    "blocking_state": "DEGRADED"
                }
        
        # All checks passed
        return {
            "allowed": True,
            "reason": "OK",
            "system_state": self.system_state.name,
            "execution_mode": self.execution_mode.value
        }

    # ---- ENHANCED FAILURE RECORDING ----
    async def record_failure(self, violation_type: str, details: Dict, severity: str = "MEDIUM"):
        """
        Record failure and escalate system state
        
        Severity levels: LOW, MEDIUM, HIGH, CRITICAL
        """
        async with self._state_lock:
            self.consecutive_failures += 1
            self.health_checks_failed += 1
            
            violation = SafetyViolation(
                timestamp=datetime.utcnow(),
                violation_type=violation_type,
                severity=severity,
                details=details,
                resolved=False
            )
            
            self.violation_history.append(violation)
            
            # Keep only last 100 violations
            if len(self.violation_history) > 100:
                self.violation_history = self.violation_history[-100:]
            
            logger.warning(f"⚠️ Safety Violation: {violation_type} ({severity})")
            
            # ==== ESCALATION LADDER ====
            
            # CRITICAL violations trigger immediate emergency
            if severity == "CRITICAL" and self.system_state != SystemState.EMERGENCY:
                logger.critical(f"🚨 CRITICAL violation - triggering emergency: {violation_type}")
                await self.trigger_full_stop(f"CRITICAL_VIOLATION_{violation_type}")
                return
                
            # HIGH violations degrade system
            elif severity == "HIGH" and self.system_state == SystemState.NORMAL:
                self.system_state = SystemState.DEGRADED
                logger.warning("System DEGRADED due to HIGH severity violation.")
                
            # Consecutive MEDIUM violations escalate
            elif self.consecutive_failures >= 3 and self.system_state == SystemState.NORMAL:
                self.system_state = SystemState.DEGRADED
                self.execution_mode = ExecutionMode.SHADOW
                logger.warning("System DEGRADED. Downgrading to SHADOW mode.")
                
            elif self.consecutive_failures >= 5:
                self.system_state = SystemState.HALTED
                self.execution_mode = ExecutionMode.SHADOW
                logger.critical("System HALTED due to repeated failures.")
                
            # Log state change
            if violation_type != "STATE_CHANGE":
                logger.info(f"System state: {self.system_state.name}, Failures: {self.consecutive_failures}")

    # ---- SUCCESS RECORDING & RECOVERY ----
    async def record_success(self, check_type: str = "GENERIC"):
        """
        Record successful operation for auto-recovery
        
        Args:
            check_type: Type of successful check (e.g., "DATA_QUALITY", "API_HEALTH")
        """
        async with self._state_lock:
            self.consecutive_failures = max(0, self.consecutive_failures - 1)
            self.health_checks_passed += 1
            self.last_health_check = datetime.now()
            
            # Auto-recovery logic
            if self.system_state == SystemState.DEGRADED and self.consecutive_failures == 0:
                # Stay in DEGRADED but allow SEMI_AUTO if conditions improve
                if self.health_checks_passed >= 10:  # 10 consecutive successes
                    self.execution_mode = ExecutionMode.SEMI_AUTO
                    logger.info("System recovering - upgraded to SEMI_AUTO mode.")
                    
            elif self.system_state == SystemState.DEGRADED and self.consecutive_failures == 0:
                # Full recovery after extended success
                if self.health_checks_passed >= 30:  # 30 consecutive successes
                    self.system_state = SystemState.NORMAL
                    logger.info("✅ System fully recovered to NORMAL state.")

    # ---- SYSTEM HEALTH CHECKS ----
    async def perform_health_check(self) -> Dict:
        """
        Perform comprehensive system health check
        
        Returns:
            Dict with health status and details
        """
        health = {
            "timestamp": datetime.now().isoformat(),
            "system_state": self.system_state.name,
            "execution_mode": self.execution_mode.value,
            "emergency_triggered": self.emergency_triggered,
            "consecutive_failures": self.consecutive_failures,
            "health_checks_passed": self.health_checks_passed,
            "health_checks_failed": self.health_checks_failed,
            "checks": {}
        }
        
        # Check 1: System state validity
        health["checks"]["system_state"] = {
            "valid": self.system_state != SystemState.EMERGENCY,
            "details": f"State: {self.system_state.name}"
        }
        
        # Check 2: Recent failures
        recent_failures = [v for v in self.violation_history 
                          if (datetime.utcnow() - v.timestamp) < timedelta(hours=1)]
        health["checks"]["recent_failures"] = {
            "valid": len(recent_failures) < 5,
            "details": f"{len(recent_failures)} failures in last hour"
        }
        
        # Check 3: Emergency state (if applicable)
        if self.emergency_triggered:
            health["checks"]["emergency_recovery"] = {
                "valid": False,
                "details": f"Emergency triggered: {self.emergency_reason} at {self.last_emergency_time}"
            }
        
        # Check 4: Cooldown status
        if self.last_panic_time:
            cooldown_remaining = self.panic_cooldown - (datetime.now() - self.last_panic_time).seconds
            health["checks"]["panic_cooldown"] = {
                "valid": cooldown_remaining <= 0,
                "details": f"Cooldown: {max(0, cooldown_remaining)}s remaining"
            }
        
        # Determine overall health
        all_valid = all(check["valid"] for check in health["checks"].values())
        health["overall_healthy"] = all_valid
        
        # Record result
        if all_valid:
            await self.record_success("HEALTH_CHECK")
        else:
            await self.record_failure(
                "HEALTH_CHECK_FAILED",
                {"health_report": health},
                "MEDIUM"
            )
        
        return health

    # ---- SYSTEM CONTROL METHODS ----
    async def reset_system(self, reason: str = "MANUAL_RESET"):
        """
        Manual system reset (requires external approval)
        
        Args:
            reason: Reason for reset
        """
        logger.warning(f"🔄 Manual system reset requested: {reason}")
        
        async with self._state_lock:
            # Can't reset from EMERGENCY without special procedure
            if self.system_state == SystemState.EMERGENCY:
                logger.error("Cannot reset from EMERGENCY state - manual intervention required")
                return False
                
            # Reset to defaults
            self.system_state = SystemState.NORMAL
            self.execution_mode = ExecutionMode.SHADOW  # Start safe
            self.consecutive_failures = 0
            self.emergency_triggered = False
            self.emergency_reason = ""
            
            # Record reset
            await self.record_failure(
                "SYSTEM_RESET",
                {"reason": reason, "timestamp": datetime.now().isoformat()},
                "LOW"
            )
            
            logger.info("✅ System reset completed. Starting in SHADOW mode.")
            return True

    def set_execution_mode(self, mode: ExecutionMode, reason: str = "MANUAL"):
        """
        Set execution mode with reason
        
        Args:
            mode: New execution mode
            reason: Reason for mode change
        """
        old_mode = self.execution_mode
        self.execution_mode = mode
        
        logger.info(f"🔧 Execution mode changed: {old_mode.value} -> {mode.value} ({reason})")
        
        # Record mode change
        asyncio.create_task(self.record_failure(
            "MODE_CHANGE",
            {"from": old_mode.value, "to": mode.value, "reason": reason},
            "LOW"
        ))

    # ---- QUERY METHODS ----
    def get_status(self) -> Dict:
        """
        Get current safety controller status
        
        Returns:
            Dict with system status
        """
        recent_violations = [
            {
                "type": v.violation_type,
                "severity": v.severity,
                "timestamp": v.timestamp.isoformat(),
                "resolved": v.resolved
            }
            for v in self.violation_history[-10:]  # Last 10 violations
        ]
        
        return {
            "system_state": self.system_state.name,
            "execution_mode": self.execution_mode.value,
            "emergency_triggered": self.emergency_triggered,
            "emergency_reason": self.emergency_reason if self.emergency_triggered else None,
            "last_emergency_time": self.last_emergency_time.isoformat() if self.last_emergency_time else None,
            "consecutive_failures": self.consecutive_failures,
            "recent_violations": recent_violations,
            "health_checks_passed": self.health_checks_passed,
            "health_checks_failed": self.health_checks_failed,
            "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None,
            "panic_cooldown_active": (
                self.last_panic_time and 
                (datetime.now() - self.last_panic_time).seconds < self.panic_cooldown
            ),
            "panic_cooldown_remaining": (
                max(0, self.panic_cooldown - (datetime.now() - self.last_panic_time).seconds)
                if self.last_panic_time else 0
            )
        }

    def is_trading_allowed(self) -> bool:
        """
        Quick check if trading is allowed
        
        Returns:
            True if trading is allowed in current state
        """
        return (
            self.system_state in [SystemState.NORMAL, SystemState.DEGRADED] and
            self.execution_mode in [ExecutionMode.SEMI_AUTO, ExecutionMode.FULL_AUTO] and
            not self.emergency_triggered
        )

    def get_violation_summary(self) -> Dict:
        """
        Get summary of violations by severity
        
        Returns:
            Dict with violation counts by severity
        """
        from collections import Counter
        severities = Counter(v.severity for v in self.violation_history)
        
        return {
            "total_violations": len(self.violation_history),
            "by_severity": dict(severities),
            "unresolved": sum(1 for v in self.violation_history if not v.resolved),
            "last_24h": sum(1 for v in self.violation_history 
                           if (datetime.utcnow() - v.timestamp) < timedelta(hours=24))
                }
