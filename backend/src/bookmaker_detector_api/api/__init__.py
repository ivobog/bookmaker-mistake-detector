from fastapi import APIRouter

from .analyst_backtests import router as analyst_backtests_router
from .analyst_opportunities import router as analyst_opportunities_router
from .analyst_patterns import router as analyst_patterns_router
from .analyst_trends import router as analyst_trends_router
from .admin_routes import router as admin_router
from .routes import router as health_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(health_router)
api_router.include_router(analyst_backtests_router)
api_router.include_router(analyst_opportunities_router)
api_router.include_router(analyst_patterns_router)
api_router.include_router(analyst_trends_router)
api_router.include_router(admin_router)
