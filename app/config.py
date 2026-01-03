import os
from pydantic_settings import BaseSettings
from typing import Optional, List

class Settings(BaseSettings):
    # Project Info
    PROJECT_NAME: str = "VolGuard Algorithmic Trading"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"

    # Upstox Credentials
    UPSTOX_ACCESS_TOKEN: str
    UPSTOX_API_KEY: Optional[str] = None
    UPSTOX_API_SECRET: Optional[str] = None
    UPSTOX_REDIRECT_URI: Optional[str] = None
    
    # Upstox Endpoints
    UPSTOX_BASE_V2: str = "https://api.upstox.com/v2"
    UPSTOX_BASE_V3: str = "https://api.upstox.com/v3"

    # Database
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_DB: str = "volguard"
    DATABASE_URL: Optional[str] = None

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    # Risk & Capital
    BASE_CAPITAL: float = 1000000.0
    MAX_DAILY_LOSS: float = 20000.0
    MAX_POSITIONS: int = 10
    MAX_NET_DELTA: float = 0.40
    MAX_GAMMA: float = 0.15
    MAX_VEGA: float = 1000.0
    MARGIN_BUFFER: float = 0.20

    # Supervisor
    SUPERVISOR_LOOP_INTERVAL: float = 3.0
    SUPERVISOR_WEBSOCKET_ENABLED: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True

    def model_post_init(self, __context):
        if not self.DATABASE_URL:
            self.DATABASE_URL = f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}/{self.POSTGRES_DB}"

settings = Settings()
