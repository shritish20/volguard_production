import asyncio
from enum import Enum, auto
from typing import Dict, List, Set
from dataclasses import dataclass, field
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
        mapping = {
            SystemState.NORMAL: 0,
            SystemState.DEGRADED: 1,
            SystemState.HALTED: 2,
            SystemState.EMERGENCY: 3,
            SystemState.SHUTDOWN: 4
        }
        return mapping[self]

class ExecutionMode(Enum):
    PAPER = "paper"
    SEMI_AUTO = "semi_auto"
    FULL_AUTO = "full_auto"

@dataclass
class SafetyViolation:
    timestamp: datetime
    violation_type: str
    severity: str
    details: Dict

class SafetyController:
    def __init__(self):
        self.system_state = SystemState.NORMAL
        self.execution_mode = ExecutionMode.SEMI_AUTO
        self.violation_history: List[SafetyViolation] = []
        self._state_lock = asyncio.Lock()
        self.consecutive_failures = 0

    async def can_adjust_trade(self, adjustment: Dict) -> Dict:
        """Gatekeeper for adjustments"""
        if self.system_state.priority() >= SystemState.HALTED.priority():
            return {"allowed": False, "reason": f"System HALTED ({self.system_state.name})"}
        
        return {"allowed": True, "reason": "OK"}

    async def record_failure(self, type_: str, details: Dict):
        async with self._state_lock:
            self.consecutive_failures += 1
            self.violation_history.append(SafetyViolation(datetime.now(), type_, "HIGH", details))
            
            if self.consecutive_failures >= 5:
                await self.escalate_state(SystemState.HALTED, "Too many failures")

    async def record_success(self):
        async with self._state_lock:
            self.consecutive_failures = 0

    async def escalate_state(self, target: SystemState, reason: str):
        async with self._state_lock:
            if target.priority() > self.system_state.priority():
                logger.critical(f"ESCALATING STATE: {self.system_state.name} -> {target.name} ({reason})")
                self.system_state = target

    def get_safety_status(self) -> Dict:
        return {
            "system_state": self.system_state.name,
            "execution_mode": self.execution_mode.value,
            "failures": self.consecutive_failures
        }
    
    def set_execution_mode(self, mode: ExecutionMode):
        self.execution_mode = mode
    
    def register_state_change_callback(self, cb): pass # Stub
