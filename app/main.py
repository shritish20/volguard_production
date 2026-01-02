"""
FastAPI application entry point.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import logging
import uvicorn

from app.config import settings
from app.database import engine
from app.api.v1.router import router as api_router
from app.utils.logging import setup_logging

# Setup logging
logger = setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for startup/shutdown"""
    # Startup
    logger.info("🚀 Starting VolGuard Trading System...")
    
    # Initialize database
    await engine.connect()
    
    # Create tables (in production, use Alembic)
    from app.database import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    logger.info("✅ VolGuard Trading System started successfully")
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down VolGuard Trading System...")
    await engine.disconnect()
    logger.info("✅ Shutdown complete")

def create_app() -> FastAPI:
    """Application factory"""
    app = FastAPI(
        title=settings.API_TITLE,
        version=settings.API_VERSION,
        lifespan=lifespan,
        docs_url="/docs" if settings.DEBUG else None,
        redoc_url="/redoc" if settings.DEBUG else None,
        openapi_url="/openapi.json" if settings.DEBUG else None
    )
    
    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"] if settings.DEBUG else settings.ALLOWED_ORIGINS
    )
    
    # Routers
    app.include_router(api_router, prefix=settings.API_PREFIX)
    
    # Health check
    @app.get("/health")
    async def health_check():
        return {
            "status": "healthy",
            "service": "volguard",
            "environment": settings.ENVIRONMENT,
            "timestamp": "now"
        }
    
    # Metrics endpoint
    @app.get("/metrics")
    async def metrics():
        return {"message": "Prometheus metrics endpoint"}
    
    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info" if not settings.DEBUG else "debug"
    )
