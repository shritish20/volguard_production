import asyncio
from enum import Enum, auto
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

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
            SystemState.SHUTDOWN: 4
        }[self]

class ExecutionMode(Enum):
    SHADOW = "shadow"       # Logs only, no trades
    SEMI_AUTO = "semi_auto" # Requires manual approval
    FULL_AUTO = "full_auto" # Full automation

@dataclass
class SafetyViolation:
    timestamp: datetime
    violation_type: str
    severity: str
    details: Dict

class SafetyController:
    def __init__(self):
        self.system_state = SystemState.NORMAL
        # DEFAULT TO SHADOW MODE FOR SAFETY
        self.execution_mode = ExecutionMode.SHADOW 
        self.violation_history: List[SafetyViolation] = []
        self._state_lock = asyncio.Lock()
        self.consecutive_failures = 0

    async def can_adjust_trade(self, adjustment: Dict) -> Dict:
        """Gatekeeper"""
        if self.system_state.priority() >= SystemState.HALTED.priority():
            return {"allowed": False, "reason": f"HALTED ({self.system_state.name})"}
        return {"allowed": True, "reason": "OK"}

    async def record_failure(self, type_: str, details: Dict):
        async with self._state_lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 5:
                self.system_state = SystemState.HALTED
                logger.critical(f"System HALTED due to {self.consecutive_failures} failures.")

    async def record_success(self):
        async with self._state_lock:
            self.consecutive_failures = 0
            # Auto-recover from DEGRADED if healthy
            if self.system_state == SystemState.DEGRADED:
                self.system_state = SystemState.NORMAL
