import logging
import httpx
from typing import Dict, Any, Optional
from app.config import settings

logger = logging.getLogger(__name__)

class AlertService:
    """
    Handles system notifications via Logging and Webhooks (Slack/Discord).
    """
    def __init__(self):
        self.webhook_url = settings.SLACK_WEBHOOK_URL
        self.client = httpx.AsyncClient(timeout=5.0)

    async def send_alert(self, title: str, message: str, severity: str = "INFO", data: Optional[Dict] = None):
        """
        Send alert to configured channels.
        """
        # 1. Log to File/Console (JSON format via logging config)
        log_payload = {"title": title, "msg": message, "data": data}
        
        if severity in ["CRITICAL", "EMERGENCY"]:
            logger.critical(str(log_payload))
        elif severity == "WARNING":
            logger.warning(str(log_payload))
        else:
            logger.info(str(log_payload))

        # 2. Send to Slack/Discord if configured
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

    async def close(self):
        await self.client.aclose()

# Global Instance
alert_service = AlertService()
