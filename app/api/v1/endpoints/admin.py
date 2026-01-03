from fastapi import APIRouter, HTTPException, Depends, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import logging
from app.config import settings
from app.lifecycle.emergency_executor import SynchronousEmergencyExecutor
from app.services.alert_service import alert_service
# Note: You need a way to inject/access the global executor instance.
# In a real app, this is usually done via dependency injection or a singleton service.
# For this architecture, we will assume a global service reference or similar pattern.
# However, to keep it clean, we will trigger the lock file or flag that Supervisor watches.

router = APIRouter()
logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-Admin-Key", auto_error=True)

class EmergencyRequest(BaseModel):
    reason: str
    action: str = "GLOBAL_KILL_SWITCH"

async def verify_admin(api_key: str = Security(API_KEY_HEADER)):
    # Simple security: X-Admin-Key must match a secret env var
    # In production, use real auth (OAuth2/JWT)
    # For now, we compare against UPSTOX_ACCESS_TOKEN as a poor-man's secret or a dedicated ADMIN_SECRET
    if api_key != settings.UPSTOX_ACCESS_TOKEN: # REPLACE with settings.ADMIN_SECRET in future
        raise HTTPException(status_code=403, detail="Invalid Admin Key")
    return api_key

@router.post("/emergency_stop")
async def trigger_emergency_stop(req: EmergencyRequest, admin: str = Depends(verify_admin)):
    """
    The Big Red Button.
    Forces the Supervisor to enter EMERGENCY state and liquidate positions.
    """
    logger.critical(f"ADMIN TRIGGERED EMERGENCY STOP: {req.reason}")
    
    try:
        # We broadcast the emergency. The Supervisor loop checks 'safety' state every cycle.
        # But for immediate action, we use the EmergencyExecutor logic.
        
        # 1. Send Alert
        await alert_service.send_alert(
            "MANUAL KILL SWITCH TRIGGERED", 
            f"Admin triggered stop. Reason: {req.reason}", 
            "EMERGENCY"
        )
        
        # 2. Force State Change (This requires access to the running Supervisor instance)
        # Since API and Supervisor are separate processes (likely), we use a shared flag (Redis/File).
        # We will create a 'KILL_SWITCH' file that Supervisor watches.
        with open("KILL_SWITCH.TRIGGER", "w") as f:
            f.write(f"{req.action}|{req.reason}")
            
        return {"status": "TRIGGERED", "message": "Kill signal sent to Supervisor"}
        
    except Exception as e:
        logger.error(f"Failed to trigger emergency: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/system_health")
async def system_status():
    """Check if the system is running, degraded, or halted."""
    # Read the latest state from the shared safety file or similar
    try:
        # Check if kill switch is active
        import os
        is_killed = os.path.exists("KILL_SWITCH.TRIGGER")
        return {
            "status": "EMERGENCY" if is_killed else "NORMAL", 
            "maintenance_mode": is_killed
        }
    except Exception as e:
        return {"status": "UNKNOWN", "error": str(e)}
