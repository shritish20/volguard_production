# app/core/auth/token_manager.py

import logging
import httpx
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class TokenManager:
    """
    VolGuard Authentication Guard (VolGuard 3.0)
    
    Responsibilities:
    1. Validates Access Token on Boot.
    2. Auto-Refreshes Token using Refresh Token (if provided).
    3. Central Authority for 'current_access_token'.
    """

    def __init__(self, access_token: str, refresh_token: Optional[str] = None, client_id: str = "", client_secret: str = ""):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.upstox.com/v2/login/authorization/token"

    def validate_token(self) -> bool:
        """
        Hits a lightweight endpoint (User Profile) to check if token is alive.
        """
        url = "https://api.upstox.com/v2/user/profile"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(url, headers=headers)
                if resp.status_code == 200:
                    logger.info(f"Token Validated. User: {resp.json().get('data', {}).get('user_name')}")
                    return True
                elif resp.status_code == 401:
                    logger.warning("Token Expired (401). Attempting Refresh...")
                    return self._perform_refresh()
                else:
                    logger.error(f"Token Validation Failed: {resp.status_code}")
                    return False
        except Exception as e:
            logger.error(f"Token Check Exception: {e}")
            return False

    def _perform_refresh(self) -> bool:
        """
        Exchanges Refresh Token for new Access Token.
        """
        if not self.refresh_token or not self.client_id:
            logger.error("Cannot Refresh: Missing refresh_token or client credentials.")
            return False
            
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }
        
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(self.base_url, data=payload) # Content-Type: x-www-form-urlencoded
                
                if resp.status_code == 200:
                    data = resp.json()
                    self.access_token = data.get("access_token")
                    logger.info("Token Refreshed Successfully!")
                    # Ideally, save this new token to disk/env so next restart picks it up
                    return True
                else:
                    logger.error(f"Token Refresh Failed: {resp.text}")
                    return False
        except Exception as e:
            logger.error(f"Token Refresh Exception: {e}")
            return False

    def get_token(self) -> str:
        return self.access_token
