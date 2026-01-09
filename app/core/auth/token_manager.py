# app/core/auth/token_manager.py

import logging
import httpx
import time
import os
import redis
from typing import Dict, Optional, Tuple
from upstox_client import Configuration
from app.config import settings

logger = logging.getLogger(__name__)

class TokenManager:
    """
    VolGuard Authentication Guard (VolGuard 5.0 - Split-Brain Fix)
    
    NEW LOGIC: Dual-layer token management with Redis as primary source
    Prevents split-brain scenarios between environment variables and live state
    
    Responsibilities:
    1. Redis-Primary, Env-Fallback token storage
    2. Validates Access Token on Boot
    3. Auto-Refreshes Token using Refresh Token (if provided)
    4. Central Authority for 'current_access_token'
    5. Updates Upstox SDK Configuration
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """
        Initialize TokenManager with Redis support
        
        Args:
            redis_client: Optional Redis client for token persistence
        """
        self.redis = redis_client
        self.redis_key = "volguard:access_token"
        self.redis_refresh_key = "volguard:refresh_token"
        
        # Environment fallback (for cold starts)
        self.env_access_token = os.getenv("UPSTOX_ACCESS_TOKEN")
        self.env_refresh_token = os.getenv("UPSTOX_REFRESH_TOKEN", "")
        self.client_id = os.getenv("UPSTOX_CLIENT_ID", "")
        self.client_secret = os.getenv("UPSTOX_CLIENT_SECRET", "")
        
        self.base_url = "https://api.upstox.com/v2/login/authorization/token"
        
        # Initialize token from best available source
        self._initialize_token()
        
        # Track last validation time
        self.last_validation = 0
        self.validation_interval = 300  # Validate every 5 minutes
        
    def _initialize_token(self):
        """Initialize token from Redis (preferred) or Environment"""
        # 1. Try Redis first (Live state)
        redis_token = self._get_from_redis(self.redis_key)
        if redis_token and self._validate_token_format(redis_token):
            self.access_token = redis_token
            logger.info("✅ Token loaded from Redis (Live State)")
            return
            
        # 2. Fallback to Environment (Startup/Backup)
        if self.env_access_token and self._validate_token_format(self.env_access_token):
            self.access_token = self.env_access_token
            logger.info("⚠️ Token loaded from Environment (Fallback)")
            
            # Store to Redis for next time
            if self.redis:
                self._save_to_redis(self.redis_key, self.access_token)
            return
            
        # 3. No valid token found
        raise ValueError("CRITICAL: No valid Access Token found in Redis or Environment")

    def get_token(self) -> str:
        """
        Get current access token with validation
        
        Returns:
            Valid access token string
        """
        # Periodic validation (non-blocking in background)
        current_time = time.time()
        if current_time - self.last_validation > self.validation_interval:
            asyncio.create_task(self._async_validate_token())
            self.last_validation = current_time
            
        return self.access_token

    def get_headers(self) -> Dict[str, str]:
        """
        Get standardized headers for API requests
        
        Returns:
            Dictionary with Authorization header
        """
        return {
            'Authorization': f'Bearer {self.get_token()}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def update_sdk_config(self, config: Configuration) -> Configuration:
        """
        Update Upstox SDK configuration with current token
        
        Args:
            config: Upstox Configuration object
            
        Returns:
            Updated Configuration object
        """
        config.access_token = self.get_token()
        return config

    async def validate_token(self) -> bool:
        """
        Async validation of access token
        
        Returns:
            True if token is valid, False otherwise
        """
        url = "https://api.upstox.com/v2/user/profile"
        headers = self.get_headers()
        
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url, headers=headers)
                
                if resp.status_code == 200:
                    user_data = resp.json().get('data', {})
                    logger.info(f"✅ Token Validated. User: {user_data.get('user_name')}")
                    return True
                    
                elif resp.status_code == 401:
                    logger.warning("Token Expired (401). Attempting Refresh...")
                    if await self._perform_refresh_async():
                        return True
                    else:
                        logger.critical("Token refresh failed!")
                        return False
                        
                else:
                    logger.error(f"Token Validation Failed: {resp.status_code}")
                    return False
                    
        except Exception as e:
            logger.error(f"Token Check Exception: {e}")
            return False

    async def _async_validate_token(self):
        """Async background token validation"""
        try:
            await self.validate_token()
        except Exception as e:
            logger.error(f"Background validation failed: {e}")

    async def _perform_refresh_async(self) -> bool:
        """
        Async token refresh using refresh token
        
        Returns:
            True if refresh successful, False otherwise
        """
        if not self.env_refresh_token or not self.client_id:
            logger.error("Cannot Refresh: Missing refresh_token or client credentials.")
            return False
            
        # Get refresh token from Redis or Environment
        refresh_token = self._get_from_redis(self.redis_refresh_key) or self.env_refresh_token
        if not refresh_token:
            logger.error("No refresh token available")
            return False
            
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token"
        }
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(self.base_url, data=payload)
                
                if resp.status_code == 200:
                    data = resp.json()
                    new_access_token = data.get("access_token")
                    new_refresh_token = data.get("refresh_token", refresh_token)  # May get new refresh token
                    
                    if not new_access_token:
                        logger.error("Refresh succeeded but no access token returned")
                        return False
                    
                    # Update tokens
                    self.access_token = new_access_token
                    
                    # Store in Redis
                    self._save_to_redis(self.redis_key, new_access_token)
                    if new_refresh_token != refresh_token:
                        self._save_to_redis(self.redis_refresh_key, new_refresh_token)
                        self.env_refresh_token = new_refresh_token
                    
                    logger.info("✅ Token Refreshed Successfully!")
                    return True
                    
                else:
                    logger.error(f"Token Refresh Failed: {resp.text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Token Refresh Exception: {e}")
            return False

    def update_token(self, new_token: str, store_in_env: bool = False):
        """
        Update access token (e.g., from manual refresh or OAuth flow)
        
        Args:
            new_token: New access token
            store_in_env: Whether to update environment variable (for persistence)
        """
        if not self._validate_token_format(new_token):
            logger.error("Invalid token format")
            return
            
        self.access_token = new_token
        
        # Store in Redis
        self._save_to_redis(self.redis_key, new_token)
        
        # Optionally update environment
        if store_in_env:
            os.environ["UPSTOX_ACCESS_TOKEN"] = new_token
            logger.info("Token updated in environment")
            
        logger.info("Token updated successfully")

    def _validate_token_format(self, token: str) -> bool:
        """
        Basic token format validation
        
        Args:
            token: Token string to validate
            
        Returns:
            True if token appears valid
        """
        if not token or not isinstance(token, str):
            return False
            
        # Basic checks
        if len(token) < 20:
            return False
            
        # Check if it looks like a JWT (contains dots)
        if '.' not in token:
            logger.warning("Token doesn't appear to be JWT format")
            # Still might be valid, just warn
            
        return True

    def _save_to_redis(self, key: str, value: str, ttl: int = 86400):
        """
        Save value to Redis with TTL (default 24 hours)
        
        Args:
            key: Redis key
            value: Value to store
            ttl: Time to live in seconds
        """
        if not self.redis:
            return
            
        try:
            self.redis.setex(key, ttl, value)
        except Exception as e:
            logger.error(f"Redis save failed for key {key}: {e}")

    def _get_from_redis(self, key: str) -> Optional[str]:
        """
        Get value from Redis
        
        Args:
            key: Redis key
            
        Returns:
            Value if found, None otherwise
        """
        if not self.redis:
            return None
            
        try:
            value = self.redis.get(key)
            return value.decode('utf-8') if value else None
        except Exception as e:
            logger.error(f"Redis get failed for key {key}: {e}")
            return None

    def health_check(self) -> Dict[str, any]:
        """
        Return token manager health status
        
        Returns:
            Dictionary with health information
        """
        redis_available = self.redis is not None and self.redis.ping()
        token_valid = self._validate_token_format(self.access_token)
        
        return {
            "redis_available": redis_available,
            "token_valid_format": token_valid,
            "source": "redis" if self._get_from_redis(self.redis_key) else "environment",
            "last_validation": self.last_validation
        }

# For backward compatibility
async def get_token_manager(redis_client: Optional[redis.Redis] = None) -> TokenManager:
    """
    Factory function for dependency injection
    
    Args:
        redis_client: Optional Redis client
        
    Returns:
        TokenManager instance
    """
    return TokenManager(redis_client)
