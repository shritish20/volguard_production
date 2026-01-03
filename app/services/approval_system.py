import asyncio
from typing import Dict, List
from datetime import datetime, timedelta
import uuid
import logging

logger = logging.getLogger(__name__)

class ManualApprovalSystem:
    def __init__(self):
        self.pending = {}
        
    async def request_approval(self, adjustment: Dict, market: Dict):
        req_id = str(uuid.uuid4())
        self.pending[req_id] = {
            "id": req_id,
            "adjustment": adjustment,
            "timestamp": datetime.now(),
            "expires": datetime.now() + timedelta(minutes=2),
            "status": "PENDING"
        }
        logger.info(f"APPROVAL REQUESTED [{req_id}]: {adjustment.get('action')}")
        return req_id

    def get_approval_stats(self) -> Dict:
        return {"pending": len(self.pending)}
