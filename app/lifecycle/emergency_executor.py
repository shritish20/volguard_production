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
        """Helper for Supervisor loop to check if trading is permitted"""
        return not self.in_emergency

    async def execute_emergency_action(self, action: Dict) -> Dict:
        """
        Executes critical safety actions.
        Strategy-Agnostic: Closes all risk regardless of which strategy opened it.
        """
        async with self.lock:
            # Check if we are already in a halted state
            if self.in_emergency and action.get("type") != "FORCE_RESET":
                return {"status": "ALREADY_IN_EMERGENCY", "timestamp": time.time()}

            self.in_emergency = True
            action_type = action.get("type", "UNKNOWN")
            reason = action.get("reason", "No reason provided")
            
            logger.critical(f"🚨 EXECUTING EMERGENCY ACTION: {action_type} | Reason: {reason}")

            try:
                result = {"status": "TRIGGERED"}
                
                # Action Mapping
                if action_type in ["CAPITAL_RISK_EMERGENCY", "GLOBAL_KILL_SWITCH", "PORTFOLIO_CRASH"]:
                    # The executor doesn't care about strategy; it flattens everything.
                    result = await self.trade_executor.close_all_positions(f"EMERGENCY_{action_type}")
                
                elif action_type == "HALT_TRADING":
                    # Just stops new entries without closing current ones
                    result = {"status": "TRADING_HALTED", "positions_retained": True}

                self.emergency_history.append({
                    "timestamp": time.time(),
                    "action": action,
                    "result": result
                })
                
                # Persist the state change to a file for supervisor recovery after restart
                with open("KILL_SWITCH.TRIGGER", "w") as f:
                    f.write(f"TYPE={action_type}|REASON={reason}|TIME={time.time()}")

                return result

            except Exception as e:
                logger.error(f"FATAL: Emergency Execution Failed: {e}")
                return {"status": "FAILED", "error": str(e)}

    async def reset_emergency(self) -> bool:
        """
        Manual recovery method. 
        Requires manual deletion of KILL_SWITCH.TRIGGER file and calling this.
        """
        async with self.lock:
            self.in_emergency = False
            logger.info("Emergency state cleared. Trading resume permitted.")
            return True

    def get_emergency_status(self) -> Dict:
        return {
            "in_emergency": self.in_emergency,
            "last_action": self.emergency_history[-1] if self.emergency_history else None,
            "history_count": len(self.emergency_history)
        }

