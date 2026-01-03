from fastapi import APIRouter
from app.api.v1.endpoints import admin
# If you have a dashboard endpoint, uncomment the lines below
# from app.api.v1.endpoints import dashboard

# Create the main API router
router = APIRouter()

# Register the Admin/Emergency endpoints
router.include_router(admin.router, prefix="/admin", tags=["Admin"])

# Register Dashboard endpoints (Optional/Future)
# router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
