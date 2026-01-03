import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class AlertService:
    """
    Handles system notifications (Slack, Email, Logs).
    Currently configured for Logging Only.
    """
    def __init__(self, config: Dict = None):
        self.config = config or {}

    async def send_alert(self, title: str, message: str, severity: str = "INFO", data: Optional[Dict] = None):
        """
        Send alert to configured channels.
        """
        log_msg = f"[{severity}] {title}: {message}"
        if data:
            log_msg += f" | Data: {data}"

        if severity in ["CRITICAL", "HIGH"]:
            logger.critical(log_msg)
            # Future: await self._send_to_slack(title, message)
        elif severity == "WARNING":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

# Global Instance
alert_service = AlertService()
