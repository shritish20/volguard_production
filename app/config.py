import os
from typing import Dict, Any, Optional, List
from pydantic_settings import BaseSettings
from pydantic import PostgresDsn, validator, Field

class Settings(BaseSettings):
    """Production configuration with validation"""

    # API
    API_TITLE: str = "VolGuard Trading System"
    API_VERSION: str = "1.0.0"
    API_PREFIX: str = "/api/v1"
    DEBUG: bool = False
    ENVIRONMENT: str = "production"

    # Security
    SECRET_KEY: str = Field(..., min_length=32)
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # CORS
    ALLOWED_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:8000",
        "https://dashboard.volguard.com"
    ]

    # Database
    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    DATABASE_URL: Optional[PostgresDsn] = None

    @validator("DATABASE_URL", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme="postgresql+asyncpg",
            user=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            host=values.get("POSTGRES_SERVER"),
            path=f"/{values.get('POSTGRES_DB') or ''}",
        )

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    # Upstox
    UPSTOX_ACCESS_TOKEN: str
    UPSTOX_BASE_V2: str = "https://api.upstox.com/v2"
    UPSTOX_BASE_V3: str = "https://api.upstox.com/v3"
    UPSTOX_TIMEOUT: int = 10

    # Trading & Capital (CRITICAL for RegimeEngine)
    BASE_CAPITAL: float = 10_00_000
    MARGIN_SELL: float = 1_20_000
    MARGIN_BUY: float = 30_000
    MAX_DAILY_LOSS: float = 20_000
    MAX_POSITION_SIZE: int = 100

    # Risk Limits
    MAX_NET_DELTA: float = 0.4
    MAX_SINGLE_LEG_DELTA: float = 0.6
    MAX_GAMMA: float = 0.15
    MAX_VEGA: float = 1000

    # Supervisor
    SUPERVISOR_LOOP_INTERVAL: float = 3.0
    SUPERVISOR_MAX_CYCLE_TIME: float = 10.0
    SUPERVISOR_WEBSOCKET_ENABLED: bool = True

    # WebSocket
    WEBSOCKET_RECONNECT_DELAY: float = 5.0
    WEBSOCKET_MAX_INSTRUMENTS: int = 50

    # Adjustment
    MIN_ADJUSTMENT_INTERVAL_MINUTES: int = 5
    MAX_ADJUSTMENTS_PER_HOUR: int = 10

    # Journaling
    JOURNAL_EVERY_CYCLE: bool = True
    JOURNAL_RETENTION_DAYS: int = 30

    # Monitoring
    SENTRY_DSN: Optional[str] = None
    PROMETHEUS_PORT: int = 9090

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

settings = Settings()
