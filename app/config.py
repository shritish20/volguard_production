# app/config.py

from pydantic_settings import BaseSettings
from typing import Optional, Dict
from enum import Enum


class Environment(str, Enum):
    DEV = "development"
    SHADOW = "shadow"
    PROD = "production"


class Settings(BaseSettings):
    # ==========================================================
    # Project Info
    # ==========================================================
    PROJECT_NAME: str = "VolGuard Algorithmic Trading"
    VERSION: str = "2.0.0"
    API_V1_STR: str = "/api/v1"

    ENVIRONMENT: Environment = Environment.PROD
    DEBUG: bool = False

    # ==========================================================
    # Security (NO DEFAULT SECRETS)
    # ==========================================================
    ADMIN_SECRET: Optional[str] = None

    # ==========================================================
    # Alerts & Monitoring
    # ==========================================================
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None
    SLACK_WEBHOOK_URL: Optional[str] = None
    SENTRY_DSN: Optional[str] = None

    # ==========================================================
    # Broker: Upstox
    # ==========================================================
    UPSTOX_ACCESS_TOKEN: Optional[str] = None
    UPSTOX_API_KEY: Optional[str] = None
    UPSTOX_API_SECRET: Optional[str] = None
    UPSTOX_REDIRECT_URI: Optional[str] = None

    UPSTOX_BASE_V2: str = "https://api.upstox.com/v2"
    UPSTOX_BASE_V3: str = "https://api.upstox.com/v3"

    # ==========================================================
    # Database
    # ==========================================================
    POSTGRES_USER: str = "volguard"
    POSTGRES_PASSWORD: str = "volguard_secure"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_DB: str = "volguard_production"
    DATABASE_URL: Optional[str] = None

    REDIS_URL: str = "redis://localhost:6379"

    # ==========================================================
    # Capital & Trading Limits (LOTS BASED)
    # ==========================================================
    BASE_CAPITAL: float = 1_000_000.0

    MAX_DAILY_LOSS: float = 20_000.0
    MAX_POSITIONS: int = 6

    DEFAULT_LOT_SIZE: int = 50
    MAX_LOTS_PER_STRATEGY: int = 3
    MAX_TOTAL_LOTS: int = 6

    # ==========================================================
    # Regime Allocation Limits (CEILINGS)
    # ==========================================================
    REGIME_MAX_LOTS: Dict[str, int] = {
        "DEFENSIVE": 1,
        "MODERATE_SHORT": 2,
        "AGGRESSIVE_SHORT": 3,
        "ULTRA_AGGRESSIVE": 2,
        "LONG_VOL": 1
    }

    # ==========================================================
    # Margin Model (Approximate)
    # ==========================================================
    MARGIN_SELL_PER_LOT: float = 120_000.0
    MARGIN_BUY_PER_LOT: float = 30_000.0
    MARGIN_BUFFER_PCT: float = 0.20

    # ==========================================================
    # Risk Limits (Portfolio Level)
    # ==========================================================
    MAX_NET_DELTA: float = 0.40
    MAX_GAMMA: float = 0.15
    MAX_VEGA: float = 1_000.0

    # ==========================================================
    # Strategy Feature Flags (CRITICAL)
    # ==========================================================
    ENABLE_DEFINED_RISK_STRATEGIES: bool = True
    ENABLE_UNDEFINED_RISK_STRATEGIES: bool = False
    ENABLE_RATIO_STRATEGIES: bool = False
    ENABLE_LONG_VOL_STRATEGIES: bool = True

    # ==========================================================
    # Supervisor Configuration
    # ==========================================================
    SUPERVISOR_LOOP_INTERVAL: float = 3.0
    SUPERVISOR_MAX_CYCLE_TIME: float = 10.0
    SUPERVISOR_WEBSOCKET_ENABLED: bool = True

    MIN_ENTRY_INTERVAL_SECONDS: int = 300
    REGIME_STABILITY_CYCLES: int = 5

    # ==========================================================
    # Execution Safety
    # ==========================================================
    MAX_ADJUSTMENTS_PER_HOUR: int = 10
    MAX_EXITS_PER_CYCLE: int = 5

    # ==========================================================
    # Validation Hooks
    # ==========================================================
    REQUIRE_UPSTOX_TOKEN_IN_PROD: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

    def model_post_init(self, __context):
        # Database URL
        if not self.DATABASE_URL:
            self.DATABASE_URL = (
                f"postgresql+asyncpg://{self.POSTGRES_USER}:"
                f"{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_SERVER}/{self.POSTGRES_DB}"
            )

        # Security enforcement
        if self.ENVIRONMENT == Environment.PROD:
            if self.REQUIRE_UPSTOX_TOKEN_IN_PROD and not self.UPSTOX_ACCESS_TOKEN:
                raise RuntimeError("UPSTOX_ACCESS_TOKEN is required in production")

            if not self.ADMIN_SECRET:
                raise RuntimeError("ADMIN_SECRET must be set in production")


settings = Settings()
