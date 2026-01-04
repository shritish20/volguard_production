# app/config.py

import os
from typing import Optional, Dict
from enum import Enum
from pydantic_settings import BaseSettings

class Environment(str, Enum):
    DEV = "development"
    SHADOW = "shadow"
    SEMI_AUTO = "production_semi"
    FULL_AUTO = "production_live"

class Settings(BaseSettings):
    # ==== Project Info ====
    PROJECT_NAME: str = "VolGuard Algorithmic Trading"
    VERSION: str = "3.1.0"
    API_V1_STR: str = "/api/v1"
    ENVIRONMENT: Environment = Environment.SHADOW
    DEBUG: bool = False

    # ==== Security ====
    ADMIN_SECRET: Optional[str] = None
    
    # ==== Broker: Upstox ====
    UPSTOX_ACCESS_TOKEN: Optional[str] = None
    UPSTOX_REFRESH_TOKEN: Optional[str] = None
    UPSTOX_CLIENT_ID: Optional[str] = None
    UPSTOX_CLIENT_SECRET: Optional[str] = None
    UPSTOX_BASE_V2: str = "https://api.upstox.com/v2"
    UPSTOX_BASE_V3: str = "https://api.upstox.com/v3"

    # ==== Database & State ====
    POSTGRES_USER: str = "volguard"
    POSTGRES_PASSWORD: str = "volguard_secure"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_DB: str = "volguard_production"
    
    # Computed at runtime
    DATABASE_URL: Optional[str] = None
    
    # REDIS (New Requirement)
    REDIS_URL: str = "redis://localhost:6379/0"

    # ==== Capital & Risk ====
    BASE_CAPITAL: float = 1_000_000.0
    MAX_DAILY_LOSS: float = 20_000.0
    MAX_POSITIONS: int = 6
    
    # Strategy Limits
    DEFAULT_LOT_SIZE: int = 50
    REGIME_MAX_LOTS: Dict[str, int] = {
        "DEFENSIVE": 1,
        "MODERATE_SHORT": 2,
        "AGGRESSIVE_SHORT": 3,
        "ULTRA_AGGRESSIVE": 2,
        "LONG_VOL": 1,
        "CASH": 0
    }

    # Supervisor Config
    SUPERVISOR_LOOP_INTERVAL: float = 3.0
    SUPERVISOR_WEBSOCKET_ENABLED: bool = True
    
    # Alerts
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None
    SLACK_WEBHOOK_URL: Optional[str] = None

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

    def model_post_init(self, __context):
        if not self.DATABASE_URL:
            self.DATABASE_URL = (
                f"postgresql+asyncpg://{self.POSTGRES_USER}:"
                f"{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_SERVER}/{self.POSTGRES_DB}"
            )

settings = Settings()
