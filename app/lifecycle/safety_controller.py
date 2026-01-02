"""
FIXED: Global safety controller with proper state priority.
"""
import asyncio
from enum import Enum, auto
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class SystemState(Enum):
    """Hierarchical system states with proper ordering"""
    NORMAL = auto()     # 0: Full automated operation
    DEGRADED = auto()   # 1: Limited operations, alerts active
    HALTED = auto()     # 2: No new trades, existing monitored
    EMERGENCY = auto()  # 3: Close ALL positions immediately
    SHUTDOWN = auto()   # 4: System offline
    
    # Explicit priority mapping
    @property
    def priority(self) -> int:
        """Numerical priority for comparison"""
        return {
            SystemState.NORMAL: 0,
            SystemState.DEGRADED: 1,
            SystemState.HALTED: 2,
            SystemState.EMERGENCY: 3,
            SystemState.SHUTDOWN: 4,
        }[self]

class ExecutionMode(Enum):
    """Execution permission levels"""
    PAPER = "paper"            # Simulated only
    SEMI_AUTO = "semi_auto"   # Human approval needed
    FULL_AUTO = "full_auto"   # Fully automated (requires NORMAL state)

@dataclass
class SafetyViolation:
    """Record of safety violations"""
    timestamp: datetime
    violation_type: str
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    details: Dict
    triggered_by: str
    system_state_override: Optional[SystemState] = None

class SafetyController:
    """
    FIXED: Safety controller with proper state comparison.
    Single source of truth for system safety.
    """
    
    def __init__(self):
        self.system_state = SystemState.NORMAL
        self.execution_mode = ExecutionMode.SEMI_AUTO  # Start safe
        self.violation_history: List[SafetyViolation] = []
        
        # Risk thresholds
        self.thresholds = {
            "max_consecutive_failures": 5,
            "max_risk_score": 85,
            "max_greek_confidence_drop": 0.3,
            "max_adjustments_per_hour": 10,
            "max_loss_rate_per_minute": 5000,
            "min_greeks_confidence": 0.6,
        }
        
        # Counters
        self.consecutive_failures = 0
        self.adjustments_this_hour = 0
        self.hourly_reset_task = None
        
        # Locks
        self._state_lock = asyncio.Lock()
        self._overrides: Set[str] = set()  # Manual overrides
        
        # Callbacks
        self._state_change_callbacks = []
        
        # Start monitoring
        asyncio.create_task(self._start_safety_monitoring())
    
    async def _start_safety_monitoring(self):
        """Continuous safety monitoring"""
        while True:
            await self._check_safety_conditions()
            await asyncio.sleep(2)
    
    async def _check_safety_conditions(self):
        """Check all safety conditions"""
        if self.hourly_reset_task is None or self.hourly_reset_task.done():
            self.hourly_reset_task = asyncio.create_task(self._reset_hourly_counters())
        
        if self.system_state.priority < SystemState.HALTED.priority:
            if self.consecutive_failures >= self.thresholds["max_consecutive_failures"]:
                await self.escalate_state(
                    SystemState.HALTED,
                    "consecutive_failures",
                    {"failures": self.consecutive_failures}
                )
    
    async def _reset_hourly_counters(self):
        """Reset hourly counters"""
        await asyncio.sleep(3600)
        self.adjustments_this_hour = 0
    
    async def can_execute_trade(self, trade_details: Dict) -> Dict:
        """
        Final gate before any trade execution
        """
        async with self._state_lock:
            if self.system_state in [SystemState.EMERGENCY, SystemState.SHUTDOWN]:
                return {
                    "allowed": False,
                    "reason": f"System state is {self.system_state.name}",
                    "required_override": "SYSTEM_STATE_OVERRIDE"
                }
            
            if self.execution_mode == ExecutionMode.PAPER:
                return {"allowed": True, "reason": "Paper trading allowed"}
            
            if self.execution_mode == ExecutionMode.SEMI_AUTO:
                return {
                    "allowed": False,
                    "reason": "Semi-auto mode requires manual approval",
                    "required_override": "MANUAL_APPROVAL",
                    "approval_data": trade_details
                }
            
            if self.execution_mode == ExecutionMode.FULL_AUTO:
                if self.adjustments_this_hour >= self.thresholds["max_adjustments_per_hour"]:
                    await self.escalate_state(
                        SystemState.DEGRADED,
                        "adjustment_rate_limit",
                        {"adjustments": self.adjustments_this_hour}
                    )
                    return {
                        "allowed": False,
                        "reason": f"Adjustment rate limit reached: {self.adjustments_this_hour}/hour",
                        "required_override": "RATE_LIMIT_OVERRIDE"
                    }
            
            return {"allowed": True, "reason": "All checks passed"}
    
    async def can_adjust_trade(self, adjustment_details: Dict) -> Dict:
        """Check if adjustment is allowed"""
        result = await self.can_execute_trade(adjustment_details)
        
        if result["allowed"]:
            self.adjustments_this_hour += 1
        
        return result
    
    async def record_failure(self, failure_type: str, details: Dict):
        """Record system failure for safety tracking"""
        async with self._state_lock:
            self.consecutive_failures += 1
            
            violation = SafetyViolation(
                timestamp=datetime.utcnow(),
                violation_type=failure_type,
                severity="HIGH" if self.consecutive_failures > 3 else "MEDIUM",
                details=details,
                triggered_by="system"
            )
            
            self.violation_history.append(violation)
            
            if len(self.violation_history) > 1000:
                self.violation_history = self.violation_history[-1000:]
    
    async def record_success(self):
        """Reset failure counter on success"""
        async with self._state_lock:
            self.consecutive_failures = 0
    
    async def escalate_state(self, new_state: SystemState, reason: str, details: Dict):
        """FIXED: Compare using priority, not string value"""
        async with self._state_lock:
            old_state = self.system_state
            
            # CORRECT: Compare priorities, not .value
            if new_state.priority > old_state.priority:
                self.system_state = new_state
                
                violation = SafetyViolation(
                    timestamp=datetime.utcnow(),
                    violation_type="state_escalation",
                    severity="CRITICAL",
                    details={
                        "from": old_state.name,
                        "to": new_state.name,
                        "priority_from": old_state.priority,
                        "priority_to": new_state.priority,
                        "reason": reason,
                        **details
                    },
                    triggered_by="safety_controller",
                    system_state_override=new_state
                )
                
                self.violation_history.append(violation)
                
                for callback in self._state_change_callbacks:
                    try:
                        callback(old_state, new_state, reason)
                    except Exception as e:
                        logger.error(f"State change callback failed: {e}")
                
                logger.critical(f"System state escalated: {old_state.name} → {new_state.name}. Reason: {reason}")
    
    async def can_deescalate_to(self, target_state: SystemState) -> bool:
        """Check if de-escalation is allowed"""
        async with self._state_lock:
            return target_state.priority < self.system_state.priority
    
    def add_override(self, override_type: str):
        """Add a manual override"""
        self._overrides.add(override_type)
        logger.warning(f"Manual override added: {override_type}")
    
    def remove_override(self, override_type: str):
        """Remove a manual override"""
        if override_type in self._overrides:
            self._overrides.remove(override_type)
            logger.warning(f"Manual override removed: {override_type}")
    
    def set_execution_mode(self, mode: ExecutionMode):
        """Change execution mode"""
        old_mode = self.execution_mode
        self.execution_mode = mode
        
        logger.info(f"Execution mode changed: {old_mode.value} → {mode.value}")
        
        if mode == ExecutionMode.FULL_AUTO and self.system_state != SystemState.NORMAL:
            logger.warning(f"Cannot switch to FULL_AUTO while system state is {self.system_state.name}")
            self.execution_mode = old_mode
    
    def register_state_change_callback(self, callback):
        """Register callback for state changes"""
        self._state_change_callbacks.append(callback)
    
    def get_safety_status(self) -> Dict:
        """Get current safety status"""
        return {
            "system_state": self.system_state.name,
            "execution_mode": self.execution_mode.value,
            "consecutive_failures": self.consecutive_failures,
            "adjustments_this_hour": self.adjustments_this_hour,
            "active_overrides": list(self._overrides),
            "violations_last_hour": len([
                v for v in self.violation_history 
                if datetime.utcnow() - v.timestamp < timedelta(hours=1)
            ]),
            "thresholds": self.thresholds
              }
