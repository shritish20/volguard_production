# app/services/approval_system.py

import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict
from sqlalchemy import Column, String, DateTime, JSON
from sqlalchemy.future import select
from app.database import Base, AsyncSessionLocal

logger = logging.getLogger(__name__)

# --------------------------------------------------
# DATABASE MODEL
# --------------------------------------------------
class ApprovalRequest(Base):
    __tablename__ = "approval_requests"

    id = Column(String, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)

    adjustment = Column(JSON)
    market_snapshot = Column(JSON)

    # PENDING → APPROVED / REJECTED / EXPIRED → CONSUMED
    status = Column(String, default="PENDING")
    decision_time = Column(DateTime, nullable=True)
    consumed_at = Column(DateTime, nullable=True)


# --------------------------------------------------
# APPROVAL SYSTEM
# --------------------------------------------------
class ManualApprovalSystem:
    """
    Manages SEMI_AUTO trade approvals.
    Guarantees one-time consumption.
    """

    def __init__(self):
        self.approval_timeout_minutes = 5

    # ---------------------------
    # REQUEST
    # ---------------------------
    async def request_approval(self, adjustment: Dict, market: Dict) -> str:
        req_id = str(uuid.uuid4())
        expires = datetime.utcnow() + timedelta(minutes=self.approval_timeout_minutes)

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

        logger.info(f"APPROVAL REQUESTED [{req_id}] {adjustment.get('strategy')}")
        return req_id

    # ---------------------------
    # CHECK (READ-ONLY)
    # ---------------------------
    async def check_approval_status(self, req_id: str) -> str:
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if not req:
                return "UNKNOWN"

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
        Returns adjustment if approved, else {}.
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

            if req.status != "APPROVED":
                return {}

            # Consume exactly once
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
                return True
        return False

    async def reject_request(self, req_id: str) -> bool:
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if req and req.status == "PENDING":
                req.status = "REJECTED"
                req.decision_time = datetime.utcnow()
                await session.commit()
                return True
        return False
