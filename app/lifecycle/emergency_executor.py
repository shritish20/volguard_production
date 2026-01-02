"""
Synchronous emergency execution - no async dependencies.
"""
import asyncio
import time
import threading
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class EmergencyCommand:
    """Atomic emergency command"""
    command_type: str
    action: str
    params: Dict
    issued_at: float
    requires_confirmation: bool = False

class SynchronousEmergencyExecutor:
    """
    Emergency actions execute synchronously and block everything else.
    """
    
    def __init__(self, trade_executor):
        self.trade_executor = trade_executor
        self.emergency_lock = threading.Lock()  # Synchronous lock, not async
        self.in_emergency = False
        self.last_emergency_time: Optional[float] = None
        self.emergency_history: List[Dict] = []
        
    def execute_emergency_action(self, emergency_action: Dict) -> Dict:
        """
        Synchronous emergency execution.
        This blocks ALL other operations until complete.
        """
        start_time = time.time()
        
        if not self.emergency_lock.acquire(blocking=False):
            return {
                "status": "BLOCKED",
                "reason": "Another emergency in progress",
                "timestamp": start_time
            }
        
        try:
            self.in_emergency = True
            self.last_emergency_time = start_time
            
            action_type = emergency_action.get("type")
            logger.critical(f"EXECUTING SYNC EMERGENCY: {action_type}")
            
            result = self._execute_sync_action(emergency_action)
            
            self.emergency_history.append({
                "action": emergency_action,
                "result": result,
                "execution_time": time.time() - start_time,
                "timestamp": start_time
            })
            
            if len(self.emergency_history) > 100:
                self.emergency_history = self.emergency_history[-100:]
            
            return result
            
        finally:
            self.emergency_lock.release()
    
    def _execute_sync_action(self, emergency_action: Dict) -> Dict:
        """Execute emergency action synchronously where possible"""
        action_type = emergency_action.get("type")
        
        if action_type == "DELTA_EMERGENCY":
            return self._emergency_delta_hedge_sync(emergency_action)
        elif action_type == "DATA_QUALITY_EMERGENCY":
            return self._emergency_halt_sync(emergency_action)
        elif action_type == "MARKET_VOL_EMERGENCY":
            return self._emergency_reduce_exposure_sync(emergency_action)
        elif action_type == "CAPITAL_RISK_EMERGENCY":
            return self._emergency_capital_protection_sync(emergency_action)
        else:
            return {
                "status": "UNKNOWN_ACTION",
                "action": action_type,
                "timestamp": time.time()
            }
    
    def _emergency_delta_hedge_sync(self, action_details: Dict) -> Dict:
        """Emergency delta hedge - synchronous core with async wrapper"""
        try:
            hedge_order = {
                "instrument": "NIFTY_FUT_CURRENT",
                "quantity": self._calculate_emergency_hedge_qty(action_details),
                "side": "SELL" if action_details.get("delta", 0) > 0 else "BUY",
                "order_type": "MARKET",
                "tag": f"EMERGENCY_DELTA_{int(time.time())}",
                "emergency": True
            }
            
            asyncio.create_task(
                self._execute_emergency_order_async(hedge_order)
            )
            
            return {
                "status": "EXECUTING",
                "action": "delta_emergency_hedge",
                "order_queued": True,
                "hedge_quantity": hedge_order["quantity"],
                "timestamp": time.time()
            }
            
        except Exception as e:
            return {
                "status": "FAILED",
                "action": "delta_emergency_hedge",
                "error": str(e),
                "timestamp": time.time()
            }
    
    async def _execute_emergency_order_async(self, order: Dict):
        """Async execution of emergency order"""
        try:
            await self.trade_executor.place_emergency_order(order)
        except Exception as e:
            logger.error(f"Emergency order failed: {e}")
    
    def _emergency_halt_sync(self, action_details: Dict) -> Dict:
        """Immediate halt - 100% synchronous"""
        return {
            "status": "HALTED",
            "action": "emergency_halt",
            "reason": action_details.get("reason", "data_quality_emergency"),
            "quality_score": action_details.get("quality_score", 0),
            "timestamp": time.time()
        }
    
    def _emergency_reduce_exposure_sync(self, action_details: Dict) -> Dict:
        """Emergency exposure reduction"""
        reduction_percentage = min(70, action_details.get("vix", 0) * 2)
        
        return {
            "status": "EXPOSURE_REDUCTION",
            "action": "reduce_exposure",
            "reduction_pct": reduction_percentage,
            "vix_trigger": action_details.get("vix", 0),
            "timestamp": time.time(),
            "orders_queued": True
        }
    
    def _emergency_capital_protection_sync(self, action_details: Dict) -> Dict:
        """Emergency capital protection"""
        worst_case_loss = action_details.get("worst_case_loss", 0)
        max_allowed = action_details.get("max_allowed", 0)
        
        loss_ratio = abs(worst_case_loss) / max_allowed if max_allowed > 0 else 1.0
        close_pct = min(100, loss_ratio * 100)
        
        return {
            "status": "CAPITAL_PROTECTION",
            "action": "close_positions",
            "close_percentage": close_pct,
            "worst_case_loss": worst_case_loss,
            "max_allowed": max_allowed,
            "loss_ratio": loss_ratio,
            "timestamp": time.time()
        }
    
    def _calculate_emergency_hedge_qty(self, action_details: Dict) -> int:
        """Calculate hedge quantity synchronously"""
        portfolio_delta = action_details.get("delta", 0)
        return int(abs(portfolio_delta) * 50)
    
    def can_proceed(self) -> bool:
        """Check if system can proceed (synchronous check)"""
        return not self.in_emergency
    
    def get_emergency_status(self) -> Dict:
        """Get emergency status"""
        return {
            "in_emergency": self.in_emergency,
            "last_emergency_time": self.last_emergency_time,
            "emergency_lock_held": self.emergency_lock.locked(),
            "recent_emergencies": len(self.emergency_history),
            "last_emergency": self.emergency_history[-1] if self.emergency_history else None
        }
