# app/services/approval_system.py

import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from sqlalchemy import Column, String, DateTime, JSON
from sqlalchemy.future import select

from app.database import Base, AsyncSessionLocal
from app.services.telegram_alerts import telegram_alerts

logger = logging.getLogger(__name__)

# --------------------------------------------------
# DATABASE MODEL
# --------------------------------------------------
class ApprovalRequest(Base):
    """
    Stores pending trade requests for Semi-Auto mode.
    """
    __tablename__ = "approval_requests"

    id = Column(String, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)

    adjustment = Column(JSON)
    market_snapshot = Column(JSON)

    # Status: PENDING → APPROVED / REJECTED / EXPIRED → CONSUMED
    status = Column(String, default="PENDING")
    decision_time = Column(DateTime, nullable=True)
    consumed_at = Column(DateTime, nullable=True)


# --------------------------------------------------
# APPROVAL SYSTEM
# --------------------------------------------------
class ManualApprovalSystem:
    """
    Manages SEMI_AUTO trade approvals.
    Features:
    1. Time-limited requests (Auto-expire).
    2. Atomic Consumption (Prevents double execution).
    3. Telegram Notifications (Real-time alerts).
    """

    def __init__(self):
        self.approval_timeout_minutes = 5

    # ---------------------------
    # REQUEST
    # ---------------------------
    async def request_approval(self, adjustment: Dict, market: Dict) -> str:
        """
        Creates a pending request and notifies admin via Telegram.
        """
        req_id = str(uuid.uuid4())
        expires = datetime.utcnow() + timedelta(minutes=self.approval_timeout_minutes)

        # 1. Persist to DB
        async with AsyncSessionLocal() as session:
            req = ApprovalRequest(
                id=req_id,
                timestamp=datetime.utcnow(),
                expires_at=expires,
                adjustment=adjustment,
                market_snapshot=market,
                status="PENDING",
            )
            session.add(req)
            await session.commit()

        # 2. Log & Alert
        strategy = adjustment.get("strategy", "UNKNOWN")
        action = adjustment.get("action", "TRADE")
        symbol = adjustment.get("instrument_key", "UNKNOWN")
        qty = adjustment.get("quantity", 0)
        
        logger.info(f"APPROVAL REQUESTED [{req_id}] {strategy}")
        
        # SMART FEATURE: Send Telegram Ping
        msg = (
            f"Strategy: *{strategy}*\n"
            f"Action: `{action}` {qty} x {symbol}\n"
            f"Expires in: {self.approval_timeout_minutes} mins\n"
            f"ID: `{req_id}`"
        )
        
        await telegram_alerts.send_alert(
            title="⚠️ APPROVAL REQUIRED",
            message=msg,
            severity="WARNING",
            data={"req_id": req_id}
        )
        
        return req_id

    # ---------------------------
    # CHECK (READ-ONLY)
    # ---------------------------
    async def check_approval_status(self, req_id: str) -> str:
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if not req:
                return "UNKNOWN"

            # Auto-Expire Check
            if req.status == "PENDING" and datetime.utcnow() > req.expires_at:
                req.status = "EXPIRED"
                await session.commit()
                return "EXPIRED"

            return req.status

    # ---------------------------
    # CONSUME (CRITICAL)
    # ---------------------------
    async def consume_if_approved(self, req_id: str) -> Dict:
        """
        Atomically checks and consumes an approval.
        Returns adjustment dict if approved, else empty dict.
        """
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ApprovalRequest).where(ApprovalRequest.id == req_id)
            )
            req = result.scalar_one_or_none()

            if not req:
                return {}

            # Expiry check
            if req.status == "PENDING" and datetime.utcnow() > req.expires_at:
                req.status = "EXPIRED"
                await session.commit()
                return {}

            # Only consume if APPROVED
            if req.status != "APPROVED":
                return {}

            # Atomic Consumption
            req.status = "CONSUMED"
            req.consumed_at = datetime.utcnow()
            await session.commit()

            return req.adjustment or {}

    # ---------------------------
    # ADMIN ACTIONS
    # ---------------------------
    async def approve_request(self, req_id: str) -> bool:
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if req and req.status == "PENDING":
                req.status = "APPROVED"
                req.decision_time = datetime.utcnow()
                await session.commit()
                
                await telegram_alerts.send_alert(
                    title="✅ REQUEST APPROVED",
                    message=f"Request {req_id[:8]} authorized for execution.",
                    severity="SUCCESS"
                )
                return True
        return False

    async def reject_request(self, req_id: str) -> bool:
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if req and req.status == "PENDING":
                req.status = "REJECTED"
                req.decision_time = datetime.utcnow()
                await session.commit()
                
                await telegram_alerts.send_alert(
                    title="🚫 REQUEST REJECTED",
                    message=f"Request {req_id[:8]} was rejected.",
                    severity="INFO"
                )
                return True
        return False
