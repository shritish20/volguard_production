"""
Telegram Alert Service - Sends critical alerts to Telegram

FIX #10: Emergency alerts bypass rate limiting for critical system events.
ENHANCED: Added market hours alerts, WebSocket success, and daily summaries.
"""
import logging
import asyncio
import httpx
from typing import Dict, Any, Optional, List
from datetime import datetime
from app.config import settings

logger = logging.getLogger(__name__)

class TelegramAlertService:
    """
    Sends critical system alerts to Telegram.
    Prioritizes critical events and includes actionable information.
    
    FIX #10: EMERGENCY and CRITICAL alerts NEVER rate limited.
    """
    
    def __init__(self):
        self.bot_token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.enabled = bool(self.bot_token and self.chat_id)
        self.client = httpx.AsyncClient(timeout=10.0)
        self.alert_history: List[Dict] = []
        self.max_history = 200  # Increased for better monitoring
        
        if self.enabled:
            logger.info("Telegram alerts ENABLED")
        else:
            logger.warning("Telegram alerts DISABLED (missing token or chat_id)")
    
    async def send_alert(self, 
                         title: str, 
                         message: str, 
                         severity: str = "INFO",
                         data: Optional[Dict] = None,
                         include_timestamp: bool = True) -> bool:
        """
        Send alert to Telegram with priority handling.
        Returns True if successful, False otherwise.
        
        Severity levels:
        - INFO: Normal operations (green)
        - WARNING: Attention needed (yellow) 
        - CRITICAL: Immediate action (red)
        - EMERGENCY: System failure (red with 🔴) - NEVER rate limited
        - TRADE: Trade execution (blue)
        - SUCCESS: Positive events (green)
        """
        
        # Log to file regardless
        log_payload = {
            "title": title,
            "msg": message,
            "severity": severity,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
        
        if severity == "CRITICAL" or severity == "EMERGENCY":
            logger.critical(str(log_payload))
        elif severity == "WARNING":
            logger.warning(str(log_payload))
        else:
            logger.info(str(log_payload))
        
        # Only send to Telegram if enabled
        if not self.enabled:
            return False
        
        # Rate limiting check (EMERGENCY/CRITICAL bypass automatically)
        if self._should_rate_limit(severity):
            logger.debug(f"Rate limiting Telegram alert: {title}")
            return False
        
        try:
            # ✅ FIX #10: For EMERGENCY, add attention-grabbing formatting
            if severity == "EMERGENCY":
                message = f"🚨🚨🚨 EMERGENCY 🚨🚨🚨\n\n{message}\n\n⚠️ IMMEDIATE ACTION REQUIRED"
            
            # Format message with Markdown
            formatted_msg = self._format_message(
                title=title,
                message=message,
                severity=severity,
                data=data,
                include_timestamp=include_timestamp
            )
            
            # ✅ FIX #10: For critical alerts, try multiple times
            max_retries = 3 if severity in ["EMERGENCY", "CRITICAL"] else 1
            
            success = False
            for attempt in range(max_retries):
                success = await self._send_telegram_message(formatted_msg)
                
                if success:
                    break
                elif attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            
            # Store in history
            self._store_alert({
                "title": title,
                "message": message,
                "severity": severity,
                "timestamp": datetime.now(),
                "success": success
            })
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")
            return False
    
    def _should_rate_limit(self, severity: str) -> bool:
        """
        Prevent alert spam with EMERGENCY bypass.
        
        ✅ FIX #10: EMERGENCY and CRITICAL alerts NEVER rate limited.
        """
        
        # ✅ FIX: NEVER rate limit critical alerts
        if severity in ["EMERGENCY", "CRITICAL"]:
            return False
        
        # Rate limit other severities
        now = datetime.now()
        recent_alerts = [
            alert for alert in self.alert_history[-20:]  # Check last 20 (increased from 10)
            if (now - alert["timestamp"]).total_seconds() < 300  # 5 minutes
        ]
        
        # Different limits by severity
        if severity == "WARNING":
            max_alerts = 15  # Allow more warnings
        elif severity == "TRADE":
            max_alerts = 20  # Allow many trade alerts
        else:  # INFO, SUCCESS
            max_alerts = 10
        
        return len(recent_alerts) >= max_alerts
    
    async def _send_telegram_message(self, message: str) -> bool:
        """Send formatted message to Telegram"""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }
            
            response = await self.client.post(url, json=payload)
            
            if response.status_code == 200:
                return True
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False
    
    def _format_message(self, 
                       title: str, 
                       message: str, 
                       severity: str,
                       data: Optional[Dict],
                       include_timestamp: bool) -> str:
        """Format message with Markdown"""
        # Severity emojis
        emoji_map = {
            "INFO": "ℹ️",
            "WARNING": "⚠️",
            "CRITICAL": "🔴",
            "EMERGENCY": "🆘",
            "TRADE": "💰",
            "SUCCESS": "✅"
        }
        
        emoji = emoji_map.get(severity, "📝")
        
        # Build message
        lines = []
        
        # Header with emoji
        lines.append(f"{emoji} *{severity}: {title}*")
        lines.append("")
        
        # Message body
        lines.append(message)
        lines.append("")
        
        # Data fields if provided
        if data:
            lines.append("*Details:*")
            for key, value in data.items():
                # Format values nicely
                if isinstance(value, float):
                    value_str = f"{value:.4f}"
                elif isinstance(value, dict):
                    value_str = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                else:
                    value_str = str(value)
                
                lines.append(f"• *{key}:* `{value_str}`")
            lines.append("")
        
        # Timestamp
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"⏰ `{timestamp}`")
        
        # System tag
        lines.append("")
        lines.append("#VolGuard")
        
        return "\n".join(lines)
    
    def _store_alert(self, alert: Dict):
        """Store alert in history (circular buffer) - OPTIMIZED"""
        self.alert_history.append(alert)
        if len(self.alert_history) > self.max_history:
            # ✅ OPTIMIZED: More efficient than pop(0)
            self.alert_history = self.alert_history[-self.max_history:]
    
    async def send_test_alert(self) -> bool:
        """Send test alert to verify Telegram setup"""
        return await self.send_alert(
            title="System Test",
            message="VolGuard Telegram alerts are working correctly! ✅",
            severity="INFO",
            data={"test": True, "system": "VolGuard"},
            include_timestamp=True
        )
    
    async def send_emergency_stop_alert(self, reason: str, triggered_by: str = "SYSTEM") -> bool:
        """Special alert for emergency stop"""
        return await self.send_alert(
            title="EMERGENCY STOP ACTIVATED",
            message=f"🛑 TRADING HALTED\n\nReason: {reason}\nTriggered by: {triggered_by}",
            severity="EMERGENCY",
            data={
                "action": "GLOBAL_KILL_SWITCH",
                "reason": reason,
                "triggered_by": triggered_by,
                "timestamp": datetime.now().isoformat()
            }
        )
    
    async def send_trade_alert(self, 
                              action: str,
                              instrument: str,
                              quantity: int,
                              side: str,
                              strategy: str,
                              reason: str = "") -> bool:
        """Alert for trade execution"""
        emoji = "🟢" if side == "BUY" else "🔴"
        return await self.send_alert(
            title=f"{emoji} Trade {action}",
            message=f"*{side}* {quantity} of {instrument}\nStrategy: {strategy}",
            severity="TRADE",
            data={
                "action": action,
                "instrument": instrument,
                "quantity": quantity,
                "side": side,
                "strategy": strategy,
                "reason": reason
            }
        )
    
    async def send_capital_breach_alert(self, 
                                       current_margin: float,
                                       total_capital: float,
                                       estimated_margin: float,
                                       breach_type: str) -> bool:
        """Alert for capital limit breaches"""
        utilization = (current_margin / total_capital) * 100
        return await self.send_alert(
            title="CAPITAL LIMIT BREACH",
            message=f"Capital utilization: {utilization:.1f}%\nBreach type: {breach_type}",
            severity="CRITICAL",
            data={
                "current_margin": current_margin,
                "total_capital": total_capital,
                "estimated_margin": estimated_margin,
                "utilization_percent": round(utilization, 2),
                "breach_type": breach_type
            }
        )
    
    async def send_data_quality_alert(self, 
                                     quality_score: float,
                                     issues: List[str],
                                     system_state: str) -> bool:
        """Alert for data quality issues"""
        return await self.send_alert(
            title=f"DATA QUALITY ALERT - {system_state}",
            message=f"Data quality score: {quality_score:.2f}\nIssues detected: {len(issues)}",
            severity="WARNING",
            data={
                "quality_score": quality_score,
                "issues": issues[:5],  # First 5 issues only
                "system_state": system_state,
                "total_issues": len(issues)
            }
        )
    
    async def send_supervisor_status(self,
                                   system_state: str,
                                   execution_mode: str,
                                   positions_count: int,
                                   cycle_time: float,
                                   data_quality: float) -> bool:
        """Daily/periodic status update"""
        return await self.send_alert(
            title="Supervisor Status Report",
            message=f"System: {system_state} | Mode: {execution_mode}\nPositions: {positions_count} | Cycle: {cycle_time:.2f}s",
            severity="INFO",
            data={
                "system_state": system_state,
                "execution_mode": execution_mode,
                "positions_count": positions_count,
                "avg_cycle_time": round(cycle_time, 2),
                "data_quality": round(data_quality, 2),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
            }
        )
    
    async def send_greeks_failure_alert(self, 
                                      missing_count: int, 
                                      total_positions: int,
                                      instrument_keys: List[str]) -> bool:
        """Alert for Greek calculation failures"""
        percent_failed = (missing_count / total_positions * 100) if total_positions > 0 else 0
        
        return await self.send_alert(
            title="GREEKS CALCULATION FAILURE",
            message=f"⚠️ {missing_count}/{total_positions} positions ({percent_failed:.1f}%) have unreliable Greeks",
            severity="CRITICAL",
            data={
                "missing_greeks_count": missing_count,
                "total_positions": total_positions,
                "failure_percentage": round(percent_failed, 2),
                "affected_instruments": instrument_keys[:5],  # First 5 only
                "action": "HALTED_OR_DEGRADED_MODE"
            }
        )
    
    async def send_redis_failure_alert(self, 
                                      component: str,
                                      error: str,
                                      impact: str) -> bool:
        """Alert for Redis failures"""
        return await self.send_alert(
            title=f"REDIS FAILURE - {component}",
            message=f"Redis connection failed for {component}\nImpact: {impact}\nError: {error[:100]}",
            severity="CRITICAL",
            data={
                "component": component,
                "error": error[:200],  # Truncate long errors
                "impact": impact,
                "timestamp": datetime.now().isoformat()
            }
        )
    
    async def send_websocket_reconnect_alert(self, 
                                           attempt: int,
                                           max_attempts: int,
                                           delay: float,
                                           last_success: Optional[datetime]) -> bool:
        """Alert for WebSocket reconnection attempts"""
        last_success_str = last_success.strftime("%H:%M:%S") if last_success else "Never"
        
        return await self.send_alert(
            title="WEBSOCKET RECONNECTION",
            message=f"Attempt {attempt}/{max_attempts} in {delay:.0f}s\nLast success: {last_success_str}",
            severity="WARNING",
            data={
                "attempt": attempt,
                "max_attempts": max_attempts,
                "delay_seconds": delay,
                "last_success": last_success_str,
                "timestamp": datetime.now().isoformat()
            }
        )
    
    # ========================================
    # ✨ NEW ENHANCED ALERTS
    # ========================================
    
    async def send_websocket_connected_alert(self, instruments_count: int) -> bool:
        """Alert when WebSocket successfully connects (NEW)"""
        return await self.send_alert(
            title="✅ WEBSOCKET CONNECTED",
            message=f"Live market data feed established\nSubscribed to {instruments_count} instruments",
            severity="SUCCESS",
            data={
                "instruments_count": instruments_count,
                "mode": "full",
                "features": ["Live Quotes", "Greeks", "Depth"],
                "timestamp": datetime.now().isoformat()
            }
        )
    
    async def send_market_hours_alert(self, event: str) -> bool:
        """Alert for market open/close events (NEW)"""
        emoji = "🔔" if event == "OPEN" else "🌙"
        message = f"NSE Market is now *{event}*"
        
        if event == "OPEN":
            message += "\n\n✅ Trading system active"
        else:
            message += "\n\n💤 Trading system hibernating"
        
        return await self.send_alert(
            title=f"{emoji} Market {event}",
            message=message,
            severity="INFO",
            data={
                "event": event,
                "timestamp": datetime.now().strftime("%H:%M:%S IST"),
                "trading_hours": "09:15 - 15:30" if event == "OPEN" else "Closed"
            }
        )
    
    async def send_daily_summary(self, stats: Dict) -> bool:
        """Send end-of-day trading summary (NEW)"""
        pnl = stats.get('pnl', 0)
        pnl_emoji = "📈" if pnl > 0 else "📉" if pnl < 0 else "➡️"
        
        return await self.send_alert(
            title=f"📊 Daily Trading Summary {pnl_emoji}",
            message=f"Trades: {stats.get('trades', 0)}\nPnL: ₹{pnl:,.0f}\nWin Rate: {stats.get('win_rate', 0):.1f}%",
            severity="INFO",
            data={
                "total_trades": stats.get('trades', 0),
                "winning_trades": stats.get('wins', 0),
                "losing_trades": stats.get('losses', 0),
                "daily_pnl": pnl,
                "win_rate": stats.get('win_rate', 0),
                "max_drawdown": stats.get('max_dd', 0),
                "largest_win": stats.get('largest_win', 0),
                "largest_loss": stats.get('largest_loss', 0)
            }
        )
    
    async def send_deployment_alert(self, environment: str) -> bool:
        """Alert when system starts (NEW)"""
        emoji = "🚀" if environment == "shadow" else "⚠️"
        
        return await self.send_alert(
            title=f"{emoji} VolGuard Deployed",
            message=f"System started in *{environment.upper()}* mode\n\nMonitoring active",
            severity="INFO",
            data={
                "environment": environment,
                "deployment_time": datetime.now().isoformat(),
                "version": "3.1.0"
            }
        )
    
    async def close(self):
        """Cleanup"""
        await self.client.aclose()

# Global instance
telegram_alerts = TelegramAlertService()
