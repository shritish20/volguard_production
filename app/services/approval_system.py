import asyncio
import uuid
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Optional
from sqlalchemy import Column, String, DateTime, JSON, Boolean
from app.database import Base, AsyncSessionLocal

logger = logging.getLogger(__name__)

# --- Database Model for Approvals ---
class ApprovalRequest(Base):
    __tablename__ = "approval_requests"
    
    id = Column(String, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)
    
    adjustment = Column(JSON) # The trade details
    market_snapshot = Column(JSON)
    
    status = Column(String, default="PENDING") # PENDING, APPROVED, REJECTED, EXPIRED
    decision_time = Column(DateTime, nullable=True)

class ManualApprovalSystem:
    """
    Manages trade approvals for SEMI_AUTO mode.
    Persists requests to DB so they survive restarts.
    """
    def __init__(self):
        self.approval_timeout_minutes = 5

    async def request_approval(self, adjustment: Dict, market: Dict) -> str:
        """
        Creates a new approval request in the database.
        """
        req_id = str(uuid.uuid4())
        expires = datetime.utcnow() + timedelta(minutes=self.approval_timeout_minutes)
        
        async with AsyncSessionLocal() as session:
            req = ApprovalRequest(
                id=req_id,
                timestamp=datetime.utcnow(),
                expires_at=expires,
                adjustment=adjustment,
                market_snapshot=market,
                status="PENDING"
            )
            session.add(req)
            await session.commit()
            
        logger.info(f"APPROVAL REQUESTED [{req_id}]: {adjustment.get('action')} {adjustment.get('instrument_key')}")
        return req_id

    async def check_approval_status(self, req_id: str) -> str:
        """
        Checks if a request has been approved/rejected via API/Admin.
        """
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if not req: return "UNKNOWN"
            
            # Check Expiry
            if req.status == "PENDING" and datetime.utcnow() > req.expires_at:
                req.status = "EXPIRED"
                await session.commit()
                return "EXPIRED"
                
            return req.status

    async def approve_request(self, req_id: str) -> bool:
        """Called by Admin API"""
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if req and req.status == "PENDING":
                req.status = "APPROVED"
                req.decision_time = datetime.utcnow()
                await session.commit()
                return True
        return False

    async def reject_request(self, req_id: str) -> bool:
        """Called by Admin API"""
        async with AsyncSessionLocal() as session:
            req = await session.get(ApprovalRequest, req_id)
            if req and req.status == "PENDING":
                req.status = "REJECTED"
                req.decision_time = datetime.utcnow()
                await session.commit()
                return True
        return False
