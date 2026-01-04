from fastapi import APIRouter, HTTPException, Depends, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import logging
import os
from pathlib import Path
from app.config import settings
from app.services.alert_service import alert_service
from app.services.telegram_alerts import telegram_alerts

router = APIRouter()
logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-Admin-Key", auto_error=True)

# 🔴 FIX #4: Use Shared Volume Path for Docker Communication
KILL_SWITCH_FILE = Path("state/KILL_SWITCH.TRIGGER")

class EmergencyRequest(BaseModel):
    reason: str
    action: str = "GLOBAL_KILL_SWITCH"

class TestTelegramRequest(BaseModel):
    test_type: str = "basic"

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
        
        # 2. Force State Change via Shared File Flag (Fix #4)
        # Ensure directory exists (for local testing)
        KILL_SWITCH_FILE.parent.mkdir(exist_ok=True)
        
        with open(KILL_SWITCH_FILE, "w") as f:
            f.write(f"{req.action}|{req.reason}")
            
        return {"status": "TRIGGERED", "message": f"Kill signal written to {KILL_SWITCH_FILE}"}
        
    except Exception as e:
        logger.error(f"Failed to trigger emergency: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/test_telegram")
async def test_telegram_alert(req: TestTelegramRequest = None, admin: str = Depends(verify_admin)):
    """Test Telegram alert connection"""
    try:
        if req is None:
            req = TestTelegramRequest()
            
        if not telegram_alerts.enabled:
            return {
                "status": "DISABLED", 
                "message": "Telegram alerts are disabled. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env"
            }
        
        test_type = req.test_type if req else "basic"
        
        if test_type == "basic":
            success = await telegram_alerts.send_test_alert()
            message = "Test alert sent to Telegram" if success else "Failed to send test alert"
            
        elif test_type == "trade":
            success = await telegram_alerts.send_trade_alert(
                action="TEST",
                instrument="NIFTY23DEC21500CE",
                quantity=50,
                side="SELL",
                strategy="STRANGLE",
                reason="Test trade alert"
            )
            message = "Trade test alert sent to Telegram" if success else "Failed to send trade alert"
            
        elif test_type == "emergency":
            success = await telegram_alerts.send_emergency_stop_alert(
                reason="Test emergency",
                triggered_by="ADMIN_API"
            )
            message = "Emergency test alert sent to Telegram" if success else "Failed to send emergency alert"
            
        else:
            return {"status": "ERROR", "message": f"Unknown test type: {test_type}"}
        
        return {"status": "SUCCESS" if success else "FAILED", "message": message}
        
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

@router.get("/telegram_status")
async def telegram_status(admin: str = Depends(verify_admin)):
    """Check Telegram alert status"""
    return {
        "enabled": telegram_alerts.enabled,
        "bot_token_set": bool(settings.TELEGRAM_BOT_TOKEN),
        "chat_id_set": bool(settings.TELEGRAM_CHAT_ID),
        "recent_alerts": len(telegram_alerts.alert_history),
        "status": "ACTIVE" if telegram_alerts.enabled else "DISABLED"
    }

@router.get("/system_health")
async def system_status():
    """Check if the system is running, degraded, or halted."""
    try:
        # Check shared volume path
        is_killed = KILL_SWITCH_FILE.exists()
        telegram_status = "ACTIVE" if telegram_alerts.enabled else "DISABLED"
        
        return {
            "status": "EMERGENCY" if is_killed else "NORMAL", 
            "maintenance_mode": is_killed,
            "telegram_alerts": telegram_status,
            "kill_switch_path": str(KILL_SWITCH_FILE)
        }
    except Exception as e:
        return {"status": "UNKNOWN", "error": str(e)}
