# app/lifecycle/safety_controller.py

import asyncio
from enum import Enum, auto
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ======================================================
# SYSTEM STATES
# ======================================================
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


# ======================================================
# EXECUTION MODES
# ======================================================
class ExecutionMode(Enum):
    SHADOW = "shadow"
    SEMI_AUTO = "semi_auto"
    FULL_AUTO = "full_auto"


# ======================================================
# VIOLATION RECORD
# ======================================================
@dataclass
class SafetyViolation:
    timestamp: datetime
    violation_type: str
    severity: str
    details: Dict


# ======================================================
# SAFETY CONTROLLER
# ======================================================
class SafetyController:
    """
    Final authority on whether trades may be executed.
    Protects against system, data, and execution failures.
    """

    def __init__(self):
        self.system_state = SystemState.NORMAL
        self.execution_mode = ExecutionMode.SHADOW  # Default SAFE
        self.violation_history: List[SafetyViolation] = []
        self._state_lock = asyncio.Lock()
        self.consecutive_failures = 0

    # --------------------------------------------------
    # GATEKEEPER
    # --------------------------------------------------
    async def can_adjust_trade(self, adjustment: Dict) -> Dict:
        """
        Decides whether an adjustment is allowed.
        EXIT / HEDGE actions are always allowed.
        """

        action = adjustment.get("action", "")
        strategy = adjustment.get("strategy", "")

        # Always allow safety exits
        if action in ["EXIT", "CLOSE"] or strategy in ["HEDGE", "KILL_SWITCH"]:
            return {"allowed": True, "reason": "Safety Action Allowed"}

        # Enforce system state
        if self.system_state.priority() >= SystemState.HALTED.priority():
            return {
                "allowed": False,
                "reason": f"System {self.system_state.name}",
            }

        # Enforce execution mode
        if self.execution_mode == ExecutionMode.SHADOW:
            return {
                "allowed": False,
                "reason": "Execution Mode = SHADOW",
            }

        return {"allowed": True, "reason": "OK"}

    # --------------------------------------------------
    # FAILURE RECORDING
    # --------------------------------------------------
    async def record_failure(self, violation_type: str, details: Dict, severity: str = "MEDIUM"):
        async with self._state_lock:
            self.consecutive_failures += 1

            self.violation_history.append(
                SafetyViolation(
                    timestamp=datetime.utcnow(),
                    violation_type=violation_type,
                    severity=severity,
                    details=details,
                )
            )

            # Escalation ladder
            if self.consecutive_failures >= 3 and self.system_state == SystemState.NORMAL:
                self.system_state = SystemState.DEGRADED
                self.execution_mode = ExecutionMode.SHADOW
                logger.warning("System DEGRADED. Downgrading to SHADOW mode.")

            elif self.consecutive_failures >= 5:
                self.system_state = SystemState.HALTED
                self.execution_mode = ExecutionMode.SHADOW
                logger.critical("System HALTED due to repeated failures.")

    # --------------------------------------------------
    # SUCCESS RECORDING
    # --------------------------------------------------
    async def record_success(self):
        async with self._state_lock:
            self.consecutive_failures = 0

            # Auto-recovery
            if self.system_state == SystemState.DEGRADED:
                self.system_state = SystemState.NORMAL
                logger.info("System recovered to NORMAL state.")
