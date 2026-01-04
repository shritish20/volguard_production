# app/services/alert_service.py

import logging
import httpx
import time
from typing import Dict, Optional, Set
from app.config import settings
from app.services.telegram_alerts import telegram_alerts

logger = logging.getLogger(__name__)

# Allowed severities (single source of truth)
SEVERITIES: Set[str] = {"INFO", "WARNING", "CRITICAL", "EMERGENCY", "TRADE", "SUCCESS"}

class AlertService:
    """
    Unified alert system: Logging + Telegram + Webhooks.
    Acts as a facade over TelegramAlertService to add logging and Webhook support.
    """
    def __init__(self):
        self.webhook_url = settings.SLACK_WEBHOOK_URL
        self._client: Optional[httpx.AsyncClient] = None

        # Anti-flood (per severity) - Prevents spamming the same error type
        self._last_sent: Dict[str, float] = {}
        self._cooldown_sec: Dict[str, int] = {
            "WARNING": 10,
            "CRITICAL": 30,
            "EMERGENCY": 60,
            "TRADE": 2, # Fast updates for trades
            "SUCCESS": 0,
            "INFO": 5
        }

        if telegram_alerts.enabled:
            logger.info("Telegram alerts INTEGRATED with AlertService")
        else:
            logger.warning("Telegram alerts NOT configured")

    # --------------------------------------------------
    # Internal helpers
    # --------------------------------------------------
    def _allowed(self, severity: str) -> bool:
        """Checks internal cooldown to prevent flooding external APIs."""
        if severity == "TRADE": return True # Never block trade alerts
        
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
        """
        Main entry point for sending alerts.
        """
        if severity not in SEVERITIES:
            logger.warning(f"Unknown severity '{severity}', treating as INFO")
            severity = "INFO"

        # 1. Logging (Always happens, even if rate limited)
        log_msg = f"[{severity}] {title}: {message}"
        if data:
            log_msg += f" | Data: {data}"

        if severity in ("CRITICAL", "EMERGENCY"):
            logger.critical(log_msg)
        elif severity == "WARNING":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        # Rate Limit Check
        if not self._allowed(severity):
            return

        # 2. Telegram Dispatch
        # We forward specific severities to Telegram
        if telegram and telegram_alerts.enabled:
            # Info level is usually too noisy for phones unless it's a specific success message
            should_send = severity in ("CRITICAL", "EMERGENCY", "WARNING", "TRADE", "SUCCESS")
            
            if should_send:
                try:
                    await telegram_alerts.send_alert(title, message, severity, data)
                except Exception as e:
                    logger.error(f"Telegram alert dispatch failed: {e}")

        # 3. Webhook Dispatch (Slack/Discord)
        if self.webhook_url and severity in ("CRITICAL", "EMERGENCY", "WARNING", "TRADE"):
            await self._dispatch_webhook(title, message, severity, data)

    async def _dispatch_webhook(self, title: str, message: str, severity: str, data: Dict):
        """Sends payload to configured Webhook URL"""
        color = "#36a64f" # Green
        if severity == "WARNING":
            color = "#ffcc00" # Yellow
        elif severity in ("CRITICAL", "EMERGENCY"):
            color = "#ff0000" # Red

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

    # --------------------------------------------------
    # Shortcuts
    # --------------------------------------------------
    async def send_emergency_stop(self, reason: str, triggered_by: str = "SYSTEM"):
        """Shortcut for Kill Switch"""
        await self.send_alert(
            title="🛑 EMERGENCY STOP",
            message=f"System Halted.\nReason: {reason}\nTrigger: {triggered_by}",
            severity="EMERGENCY"
        )

    async def send_trade_execution(
        self,
        action: str,
        instrument: str,
        quantity: int,
        side: str,
        strategy: str,
        reason: str = "",
    ):
        """Shortcut for Trade Execution"""
        await telegram_alerts.send_trade_alert(
            action, instrument, quantity, side, strategy, reason
        )
        # We also log it here locally via standard logging
        logger.info(f"TRADE EXECUTION: {side} {quantity} {instrument} [{strategy}]")

    async def close(self):
        if self._client:
            await self._client.aclose()
        if telegram_alerts.enabled:
            await telegram_alerts.close()

# Global Singleton Instance
alert_service = AlertService()
