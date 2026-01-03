"""
Updated Alert Service - Integrates Telegram with existing logging
"""
import logging
import httpx
from typing import Dict, Any, Optional
from app.config import settings
from app.services.telegram_alerts import telegram_alerts

logger = logging.getLogger(__name__)

class AlertService:
    """ 
    Unified alert system: Logging + Telegram
    """ 
    def __init__(self):
        self.webhook_url = settings.SLACK_WEBHOOK_URL
        self.client = httpx.AsyncClient(timeout=5.0)
        
        # Log Telegram status
        if telegram_alerts.enabled:
            logger.info("Telegram alerts INTEGRATED with AlertService")
        else:
            logger.warning("Telegram alerts NOT configured")

    async def send_alert(self, title: str, message: str, severity: str = "INFO", 
                        data: Optional[Dict] = None, telegram: bool = True):
        """ 
        Send alert to all configured channels.
        """
        # 1. Log to File/Console (JSON format via logging config)
        log_payload = {"title": title, "msg": message, "severity": severity, "data": data}
        
        if severity in ["CRITICAL", "EMERGENCY"]:
            logger.critical(str(log_payload))
        elif severity == "WARNING":
            logger.warning(str(log_payload))
        else:
            logger.info(str(log_payload))

        # 2. Send to Telegram (if enabled and requested)
        if telegram and telegram_alerts.enabled and severity in ["CRITICAL", "EMERGENCY", "WARNING", "TRADE"]:
            await telegram_alerts.send_alert(title, message, severity, data)
        
        # 3. Send to Slack/Discord if configured (existing logic)
        if self.webhook_url and severity in ["CRITICAL", "EMERGENCY", "WARNING", "TRADE"]:
            await self._dispatch_webhook(title, message, severity, data)

    async def _dispatch_webhook(self, title: str, message: str, severity: str, data: Dict):
        try:
            # Color coding
            color = "#36a64f" # Green/Info
            if severity == "WARNING": color = "#ffcc00"
            if severity in ["CRITICAL", "EMERGENCY"]: color = "#ff0000"

            payload = {
                "text": f"*{severity}*: {title}\n{message}",
                "attachments": [
                    {
                        "color": color,
                        "fields": [{"title": k, "value": str(v), "short": True} for k, v in (data or {}).items()]
                    }
                ]
            }
            await self.client.post(self.webhook_url, json=payload)
        except Exception as e:
            logger.error(f"Failed to send webhook: {e}")

    async def send_emergency_stop(self, reason: str, triggered_by: str = "SYSTEM"):
        """Emergency stop with Telegram alert"""
        # Critical log
        logger.critical(f"EMERGENCY STOP: {reason} (triggered by: {triggered_by})")
        
        # Telegram emergency alert
        if telegram_alerts.enabled:
            await telegram_alerts.send_emergency_stop_alert(reason, triggered_by)
    
    async def send_trade_execution(self, action: str, instrument: str, quantity: int, 
                                  side: str, strategy: str, reason: str = ""):
        """Trade execution alert"""
        logger.info(f"TRADE {action}: {side} {quantity} {instrument} ({strategy}) - {reason}")
        
        # Telegram trade alert
        if telegram_alerts.enabled:
            await telegram_alerts.send_trade_alert(action, instrument, quantity, side, strategy, reason)
    
    async def test_telegram_connection(self):
        """Test Telegram connectivity"""
        logger.info("Testing Telegram connection...")
        if not telegram_alerts.enabled:
            logger.warning("Telegram alerts are disabled")
            return False
        
        success = await telegram_alerts.send_test_alert()
        
        if success:
            logger.info("✅ Telegram alerts: CONNECTED")
            return True
        else:
            logger.warning("❌ Telegram alerts: FAILED")
            return False

    async def close(self):
        await self.client.aclose()
        if telegram_alerts.enabled:
            await telegram_alerts.close()

# Global Instance
alert_service = AlertService()
