import asyncio
import time
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class SynchronousEmergencyExecutor:
    """
    Handles Emergency Actions (Kill Switch, Halt).
    Uses Asyncio Locks to ensure thread safety without blocking the Event Loop.
    """
    def __init__(self, trade_executor):
        self.trade_executor = trade_executor
        self.lock = asyncio.Lock()
        self.in_emergency = False
        self.emergency_history = []

    def can_proceed(self) -> bool:
        """Helper for Supervisor loop"""
        return not self.in_emergency

    async def execute_emergency_action(self, action: Dict) -> Dict:
        """
        Executes critical safety actions.
        """
        async with self.lock:
            if self.in_emergency:
                return {"status": "ALREADY_IN_EMERGENCY"}

            self.in_emergency = True
            action_type = action.get("type", "UNKNOWN")
            logger.critical(f"🚨 EXECUTING EMERGENCY ACTION: {action_type}")

            try:
                result = {}
                if action_type == "CAPITAL_RISK_EMERGENCY":
                    # Close positions to free margin
                    result = await self.trade_executor.close_all_positions("CAPITAL_BREACH")
                
                elif action_type == "GLOBAL_KILL_SWITCH":
                    # Panic Button
                    result = await self.trade_executor.close_all_positions("KILL_SWITCH")
                
                self.emergency_history.append({
                    "timestamp": time.time(),
                    "action": action,
                    "result": result
                })
                return result

            except Exception as e:
                logger.error(f"Emergency Execution Failed: {e}")
                return {"status": "FAILED", "error": str(e)}

    def get_emergency_status(self) -> Dict:
        return {
            "in_emergency": self.in_emergency,
            "history_count": len(self.emergency_history)
        }
