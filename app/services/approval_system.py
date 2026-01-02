"""
Manual approval system for SEMI_AUTO mode.
"""
import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class ApprovalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"

@dataclass
class ApprovalRequest:
    """Approval request with expiry"""
    request_id: str
    trade_details: Dict
    adjustment_details: Dict
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=2))
    status: ApprovalStatus = ApprovalStatus.PENDING
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None
    market_snapshot_at_request: Dict = field(default_factory=dict)
    
    def is_valid(self, current_market: Dict) -> Tuple[bool, str]:
        if datetime.utcnow() > self.expires_at:
            return False, f"Approval expired at {self.expires_at}"
        
        if self._is_market_invalidated(current_market):
            return False, "Market move invalidated approval"
        
        if self.status != ApprovalStatus.PENDING:
            return False, f"Request already {self.status.value}"
        
        return True, "Valid"
    
    def _is_market_invalidated(self, current_market: Dict) -> bool:
        if not self.market_snapshot_at_request:
            return False
        
        old_spot = self.market_snapshot_at_request.get("spot", 0)
        new_spot = current_market.get("spot", 0)
        
        if old_spot > 0 and new_spot > 0:
            move_pct = abs(new_spot - old_spot) / old_spot * 100
            
            strategy = self.adjustment_details.get("strategy", "")
            
            if "IRON_CONDOR" in strategy or "FLY" in strategy:
                return move_pct > 0.5
            elif "STRANGLE" in strategy:
                return move_pct > 1.0
            else:
                return move_pct > 1.5
        
        return False

class ManualApprovalSystem:
    """
    Production manual approval system.
    """
    
    def __init__(self):
        self.pending_requests: Dict[str, ApprovalRequest] = {}
        self.request_history: List[ApprovalRequest] = []
        
        self.invalidation_thresholds = {
            "spot_move_pct": 1.0,
            "vix_move_pct": 15.0,
            "time_expiry_seconds": 120,
        }
        
        self.cleanup_task = asyncio.create_task(self._cleanup_expired_requests())
    
    async def request_approval(self, adjustment: Dict, market_snapshot: Dict) -> ApprovalRequest:
        request_id = f"APPROVAL_{int(datetime.utcnow().timestamp())}_{len(self.pending_requests)}"
        
        request = ApprovalRequest(
            request_id=request_id,
            trade_details=adjustment.get("trade_details", {}),
            adjustment_details=adjustment,
            market_snapshot_at_request=market_snapshot.copy(),
            expires_at=datetime.utcnow() + timedelta(seconds=self.invalidation_thresholds["time_expiry_seconds"])
        )
        
        self.pending_requests[request_id] = request
        self.request_history.append(request)
        
        if len(self.request_history) > 1000:
            self.request_history = self.request_history[-1000:]
        
        await self._notify_dashboard(request)
        
        return request
    
    async def check_approval(self, request_id: str, current_market: Dict) -> Tuple[bool, Optional[ApprovalRequest], str]:
        request = self.pending_requests.get(request_id)
        
        if not request:
            return False, None, f"Request {request_id} not found"
        
        is_valid, reason = request.is_valid(current_market)
        
        if not is_valid:
            if "expired" in reason.lower():
                request.status = ApprovalStatus.EXPIRED
            else:
                request.status = ApprovalStatus.INVALIDATED
            
            self.pending_requests.pop(request_id, None)
            
            return False, request, reason
        
        if request.status == ApprovalStatus.APPROVED:
            self.pending_requests.pop(request_id, None)
            return True, request, "Approved"
        
        return False, request, f"Pending approval ({request.status.value})"
    
    async def approve_request(self, request_id: str, approver: str, notes: str = "") -> Tuple[bool, str]:
        request = self.pending_requests.get(request_id)
        
        if not request:
            return False, f"Request {request_id} not found"
        
        is_valid, reason = request.is_valid({})
        
        if not is_valid:
            request.status = ApprovalStatus.INVALIDATED
            self.pending_requests.pop(request_id, None)
            return False, f"Cannot approve: {reason}"
        
        request.status = ApprovalStatus.APPROVED
        request.approved_by = approver
        request.approved_at = datetime.utcnow()
        
        await self._log_approval(request, approver, notes)
        
        return True, f"Request {request_id} approved by {approver}"
    
    async def reject_request(self, request_id: str, rejector: str, reason: str) -> Tuple[bool, str]:
        request = self.pending_requests.get(request_id)
        
        if not request:
            return False, f"Request {request_id} not found"
        
        request.status = ApprovalStatus.REJECTED
        request.rejection_reason = reason
        
        self.pending_requests.pop(request_id, None)
        
        await self._log_rejection(request, rejector, reason)
        
        return True, f"Request {request_id} rejected"
    
    async def _cleanup_expired_requests(self):
        while True:
            try:
                now = datetime.utcnow()
                expired_ids = []
                
                for req_id, request in self.pending_requests.items():
                    if request.expires_at < now:
                        request.status = ApprovalStatus.EXPIRED
                        expired_ids.append(req_id)
                
                for req_id in expired_ids:
                    self.pending_requests.pop(req_id, None)
                
                if expired_ids:
                    await self._notify_expirations(expired_ids)
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Cleanup task error: {e}")
                await asyncio.sleep(60)
    
    async def _notify_dashboard(self, request: ApprovalRequest):
        logger.info(f"New approval request: {request.request_id} for {request.adjustment_details.get('action')}")
    
    async def _log_approval(self, request: ApprovalRequest, approver: str, notes: str):
        logger.info(f"Approval granted: {request.request_id} by {approver}. Notes: {notes}")
    
    async def _log_rejection(self, request: ApprovalRequest, rejector: str, reason: str):
        logger.info(f"Approval rejected: {request.request_id} by {rejector}. Reason: {reason}")
    
    async def _notify_expirations(self, expired_ids: List[str]):
        for req_id in expired_ids:
            logger.warning(f"Approval request expired: {req_id}")
    
    def get_pending_requests(self, include_invalid: bool = False) -> List[ApprovalRequest]:
        if include_invalid:
            return list(self.pending_requests.values())
        else:
            now = datetime.utcnow()
            return [
                req for req in self.pending_requests.values()
                if req.expires_at > now and req.status == ApprovalStatus.PENDING
            ]
    
    def get_approval_stats(self) -> Dict:
        now = datetime.utcnow()
        last_24h = [r for r in self.request_history if (now - r.created_at) < timedelta(hours=24)]
        
        return {
            "pending_count": len(self.get_pending_requests()),
            "total_last_24h": len(last_24h),
            "approved_last_24h": len([r for r in last_24h if r.status == ApprovalStatus.APPROVED]),
            "rejected_last_24h": len([r for r in last_24h if r.status == ApprovalStatus.REJECTED]),
            "expired_last_24h": len([r for r in last_24h if r.status == ApprovalStatus.EXPIRED]),
            "invalidated_last_24h": len([r for r in last_24h if r.status == ApprovalStatus.INVALIDATED]),
            "avg_approval_time_seconds": self._calculate_avg_approval_time(last_24h)
        }
    
    def _calculate_avg_approval_time(self, requests: List[ApprovalRequest]) -> float:
        approved = [r for r in requests if r.status == ApprovalStatus.APPROVED and r.approved_at]
        if not approved:
            return 0.0
        
        total_seconds = sum([
            (r.approved_at - r.created_at).total_seconds()
            for r in approved
        ])
        
        return total_seconds / len(approved)
