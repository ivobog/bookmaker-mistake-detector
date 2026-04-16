from fastapi import APIRouter

from .admin_routes import router as admin_router
from .routes import router as health_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(health_router)
api_router.include_router(admin_router)
