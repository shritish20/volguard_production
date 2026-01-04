from fastapi import APIRouter, Depends
from typing import Dict, Any
from datetime import datetime
import logging

# We need a way to access the running Supervisor state.
# In a single-process deployment (like Docker), we can sometimes rely on a singleton 
# or shared memory. For this API, we will check the 'heartbeat' file or database logs 
# since the Supervisor runs in a separate process/loop from the API.

from app.services.alert_service import alert_service
from app.services.telegram_alerts import telegram_alerts
from app.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/status")
async def get_supervisor_status() -> Dict[str, Any]:
    """
    Returns the health status of the Supervisor.
    Used by deploy.sh and monitoring tools.
    """
    # 1. Check if Kill Switch is active
    import os
    is_killed = os.path.exists("KILL_SWITCH.TRIGGER")
    
    # 2. Check Database Connectivity (Simple ping)
    from app.database import AsyncSessionLocal
    from sqlalchemy import text
    db_status = "UNKNOWN"
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
            db_status = "CONNECTED"
    except Exception as e:
        db_status = f"DISCONNECTED: {str(e)}"

    return {
        "status": "EMERGENCY" if is_killed else "RUNNING",
        "timestamp": datetime.now(),
        "environment": settings.ENVIRONMENT,
        "database": db_status,
        "kill_switch_active": is_killed,
        "telegram_enabled": telegram_alerts.enabled
    }

@router.get("/heartbeat")
async def heartbeat():
    """Simple 200 OK for load balancers"""
    return {"status": "ok"}
