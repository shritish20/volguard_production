import os
from pydantic_settings import BaseSettings
from typing import Optional, List, Union

class Settings(BaseSettings):
    # Project Info
    PROJECT_NAME: str = "VolGuard Algorithmic Trading"
    VERSION: str = "1.1.0"
    API_V1_STR: str = "/api/v1"
    ENVIRONMENT: str = "production"
    DEBUG: bool = False

    # Security
    ADMIN_SECRET: str = "change_this_to_something_secure"

    # Telegram Alerts
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    # Upstox Credentials & Auto-Login
    UPSTOX_ACCESS_TOKEN: str
    UPSTOX_API_KEY: Optional[str] = None
    UPSTOX_API_SECRET: Optional[str] = None
    UPSTOX_REDIRECT_URI: Optional[str] = None
    UPSTOX_CLIENT_ID: Optional[str] = None
    UPSTOX_CLIENT_SECRET: Optional[str] = None
    
    # Upstox Endpoints
    UPSTOX_BASE_V2: str = "https://api.upstox.com/v2"
    UPSTOX_BASE_V3: str = "https://api.upstox.com/v3"

    # Database
    POSTGRES_USER: str = "volguard"
    POSTGRES_PASSWORD: str = "volguard_secure"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_DB: str = "volguard_production"
    DATABASE_URL: Optional[str] = None

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    # Trading Configuration & Capital Limits
    BASE_CAPITAL: float = 1000000.0
    MAX_DAILY_LOSS: float = 20000.0
    MAX_POSITIONS: int = 10
    MAX_POSITION_SIZE: int = 100
    
    # Margins
    MARGIN_SELL: float = 120000.0
    MARGIN_BUY: float = 30000.0
    
    # Risk Limits
    MAX_NET_DELTA: float = 0.40
    MAX_SINGLE_LEG_DELTA: float = 0.60
    MAX_GAMMA: float = 0.15
    MAX_VEGA: float = 1000.0
    MARGIN_BUFFER: float = 0.20

    # Supervisor Configuration
    SUPERVISOR_LOOP_INTERVAL: float = 3.0
    SUPERVISOR_MAX_CYCLE_TIME: float = 10.0
    SUPERVISOR_WEBSOCKET_ENABLED: bool = True
    
    # Adjustment Limits
    MIN_ADJUSTMENT_INTERVAL_MINUTES: int = 5
    MAX_ADJUSTMENTS_PER_HOUR: int = 10

    # Monitoring & Alerting
    SLACK_WEBHOOK_URL: Optional[str] = None
    SENTRY_DSN: Optional[str] = None

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore" 

    def model_post_init(self, __context):
        if not self.DATABASE_URL:
            self.DATABASE_URL = f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}/{self.POSTGRES_DB}"

settings = Settings()
