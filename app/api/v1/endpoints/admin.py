from fastapi import APIRouter, HTTPException, Depends, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import logging
import os
from app.config import settings
from app.services.alert_service import alert_service

router = APIRouter()
logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-Admin-Key", auto_error=True)

class EmergencyRequest(BaseModel):
    reason: str
    action: str = "GLOBAL_KILL_SWITCH"

async def verify_admin(api_key: str = Security(API_KEY_HEADER)):
    """
    Verifies the admin key against the dedicated secret in config.
    """
    if api_key != settings.ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid Admin Secret")
    return api_key

@router.post("/emergency_stop")
async def trigger_emergency_stop(req: EmergencyRequest, admin: str = Depends(verify_admin)):
    """
    The Big Red Button.
    Forces the Supervisor to enter EMERGENCY state and liquidate positions.
    """
    logger.critical(f"ADMIN TRIGGERED EMERGENCY STOP: {req.reason}")
    
    try:
        # 1. Send Alert
        await alert_service.send_alert(
            "MANUAL KILL SWITCH TRIGGERED", 
            f"Admin triggered stop. Reason: {req.reason}", 
            "EMERGENCY"
        )
        
        # 2. Force State Change via File Flag
        # The Supervisor loop checks for this file's existence every cycle (Phase 0)
        with open("KILL_SWITCH.TRIGGER", "w") as f:
            f.write(f"{req.action}|{req.reason}")
            
        return {"status": "TRIGGERED", "message": "Kill signal sent to Supervisor"}
        
    except Exception as e:
        logger.error(f"Failed to trigger emergency: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/system_health")
async def system_status():
    """Check if the system is running, degraded, or halted."""
    try:
        is_killed = os.path.exists("KILL_SWITCH.TRIGGER")
        return {
            "status": "EMERGENCY" if is_killed else "NORMAL", 
            "maintenance_mode": is_killed
        }
    except Exception as e:
        return {"status": "UNKNOWN", "error": str(e)}
