"""
Updated Alert Service - Integrates Telegram with existing logging
PRODUCTION HARDENED
"""
import logging
import httpx
import asyncio
import time
from typing import Dict, Any, Optional
from app.config import settings
from app.services.telegram_alerts import telegram_alerts

logger = logging.getLogger(__name__)

# Allowed severities (single source of truth)
SEVERITIES = {"INFO", "WARNING", "CRITICAL", "EMERGENCY", "TRADE"}

class AlertService:
    """
    Unified alert system: Logging + Telegram + Webhooks
    """
    def __init__(self):
        self.webhook_url = settings.SLACK_WEBHOOK_URL
        self._client: Optional[httpx.AsyncClient] = None

        # Simple anti-flood (per severity)
        self._last_sent = {}
        self._cooldown_sec = {
            "WARNING": 10,
            "CRITICAL": 30,
            "EMERGENCY": 60,
            "TRADE": 5,
        }

        if telegram_alerts.enabled:
            logger.info("Telegram alerts INTEGRATED with AlertService")
        else:
            logger.warning("Telegram alerts NOT configured")

    # --------------------------------------------------
    # Internal helpers
    # --------------------------------------------------
    def _allowed(self, severity: str) -> bool:
        now = time.time()
        cd = self._cooldown_sec.get(severity, 0)
        last = self._last_sent.get(severity, 0)
        if now - last < cd:
            return False
        self._last_sent[severity] = now
        return True

    async def _get_client(self) -> httpx.AsyncClient:
        if not self._client:
            self._client = httpx.AsyncClient(timeout=5.0)
        return self._client

    # --------------------------------------------------
    # Public API
    # --------------------------------------------------
    async def send_alert(
        self,
        title: str,
        message: str,
        severity: str = "INFO",
        data: Optional[Dict] = None,
        telegram: bool = True,
    ):
        if severity not in SEVERITIES:
            logger.warning(f"Unknown severity '{severity}', treating as INFO")
            severity = "INFO"

        payload = {
            "title": title,
            "msg": message,
            "severity": severity,
            "data": data,
        }

        # 1. Logging (always)
        if severity in ("CRITICAL", "EMERGENCY"):
            logger.critical(payload)
        elif severity == "WARNING":
            logger.warning(payload)
        else:
            logger.info(payload)

        if not self._allowed(severity):
            return

        # 2. Telegram
        if telegram and telegram_alerts.enabled and severity in ("CRITICAL", "EMERGENCY", "WARNING", "TRADE"):
            try:
                await telegram_alerts.send_alert(title, message, severity, data)
            except Exception as e:
                logger.error(f"Telegram alert failed: {e}")

        # 3. Webhook
        if self.webhook_url and severity in ("CRITICAL", "EMERGENCY", "WARNING", "TRADE"):
            await self._dispatch_webhook(title, message, severity, data)

    async def _dispatch_webhook(self, title: str, message: str, severity: str, data: Dict):
        color = "#36a64f"
        if severity == "WARNING":
            color = "#ffcc00"
        elif severity in ("CRITICAL", "EMERGENCY"):
            color = "#ff0000"

        payload = {
            "text": f"*{severity}*: {title}\n{message}",
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {"title": k, "value": str(v), "short": True}
                        for k, v in (data or {}).items()
                    ],
                }
            ],
        }

        try:
            client = await self._get_client()
            await client.post(self.webhook_url, json=payload)
        except Exception as e:
            logger.warning(f"Webhook delivery failed: {e}")

    async def send_emergency_stop(self, reason: str, triggered_by: str = "SYSTEM"):
        logger.critical(f"EMERGENCY STOP: {reason} (triggered by {triggered_by})")
        if telegram_alerts.enabled:
            await telegram_alerts.send_emergency_stop_alert(reason, triggered_by)

    async def send_trade_execution(
        self,
        action: str,
        instrument: str,
        quantity: int,
        side: str,
        strategy: str,
        reason: str = "",
    ):
        logger.info(
            f"TRADE {action}: {side} {quantity} {instrument} ({strategy}) - {reason}"
        )
        if telegram_alerts.enabled:
            await telegram_alerts.send_trade_alert(
                action, instrument, quantity, side, strategy, reason
            )

    async def close(self):
        if self._client:
            await self._client.aclose()
        if telegram_alerts.enabled:
            await telegram_alerts.close()


# Global instance
alert_service = AlertService()
