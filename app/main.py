# app/main.py

import logging
from fastapi import FastAPI

from app.config import settings
from app.api.v1.router import api_router

# 🔑 AUTHORITATIVE REGISTRY
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.API_TITLE,
        version=settings.API_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/api/v1/openapi.json",
    )

    # --------------------------------------------------
    # ROUTERS
    # --------------------------------------------------
    app.include_router(api_router, prefix="/api/v1")

    # --------------------------------------------------
    # STARTUP: LOAD INSTRUMENT MASTER
    # --------------------------------------------------
    @app.on_event("startup")
    async def startup_event():
        """
        Runs ONCE when FastAPI starts.
        Loads Upstox Instrument Master into memory.
        """
        logger.info("🚀 VolGuard starting up...")
        registry.load_master(force_refresh=False)
        logger.info("✅ Instrument master loaded and ready")

    # --------------------------------------------------
    # SHUTDOWN
    # --------------------------------------------------
    @app.on_event("shutdown")
    async def shutdown_event():
        logger.info("🛑 VolGuard shutting down")

    return app


app = create_app()
