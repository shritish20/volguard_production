from fastapi import APIRouter
from app.api.v1.endpoints import admin, dashboard, supervisor

router = APIRouter()

# 1. Admin (Emergency Stop, Config)
router.include_router(admin.router, prefix="/admin", tags=["Admin"])

# 2. Dashboard (Analytics, Visuals) - WAS MISSING
router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])

# 3. Supervisor (Health, State) - WAS MISSING
router.include_router(supervisor.router, prefix="/supervisor", tags=["Supervisor"])
