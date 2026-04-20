from fastapi import APIRouter

from bookmaker_detector_api.config import settings

from .admin_diagnostics_routes import router as admin_diagnostics_router
from .admin_feature_routes import router as admin_feature_router
from .admin_market_board_routes import router as admin_market_board_router
from .admin_model_routes import router as admin_model_router
from .admin_opportunity_routes import router as admin_opportunity_router
from .admin_scoring_routes import router as admin_scoring_router
from .analyst_backtests import router as analyst_backtests_router
from .analyst_opportunities import router as analyst_opportunities_router
from .analyst_patterns import router as analyst_patterns_router
from .analyst_trends import router as analyst_trends_router
from .routes import router as health_router
from .test_routes import router as test_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(health_router)
api_router.include_router(analyst_backtests_router)
api_router.include_router(analyst_opportunities_router)
api_router.include_router(analyst_patterns_router)
api_router.include_router(analyst_trends_router)
api_router.include_router(admin_diagnostics_router)
api_router.include_router(admin_feature_router)
api_router.include_router(admin_market_board_router)
api_router.include_router(admin_model_router)
api_router.include_router(admin_opportunity_router)
api_router.include_router(admin_scoring_router)
if settings.allow_test_helpers:
    api_router.include_router(test_router)
