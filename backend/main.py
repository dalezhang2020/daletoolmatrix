from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.tools.imagelingo.routes import auth, translate, webhook, products
from backend.tools.fitness import routes as fitness_routes
from backend.shared.s3_router import router as s3_router
from backend.tools.shopline_zendesk.routes import (
    include_oauth_routes,
    include_shopline_frontend_routes,
    include_zaf_frontend_routes,
)
from backend.tools.shopline_zendesk.routes.zendesk.app.database import (
    create_tables as sz_v2_create_tables,
)
from backend.tools.shopline_zendesk.routes.zendesk.app.middleware.auth import (
    AuthMiddleware as SzV2AuthMiddleware,
)
from backend.tools.shopline_zendesk.routes.zendesk.app.middleware.tenant import (
    TenantMiddleware as SzV2TenantMiddleware,
)
from backend.tools.omnigatech.mounts import include_omnigatech_routes
from backend.tools.omnigatech.database import (
    create_omnigatech_tables,
)
from backend.tools.omnigatech.middleware.auth import (
    OmnigaTechAuthMiddleware,
)
from backend.tools.omnigatech.middleware.tenant import (
    OmnigaTechTenantMiddleware,
)
from backend.tools.shopline_zendesk.services.token_refresh_job import (
    start_refresh_job,
    stop_refresh_job,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="DaleToolMatrix API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        os.getenv("FRONTEND_URL", ""),
        os.getenv("SHOPLINE_ZD_FRONTEND_URL", ""),
        "https://zendesk.omnigatech.com",
        "https://stripepage-shoplinebyomnigatech.vercel.app",
    ],
    allow_origin_regex=(
        r"^https://.*\.vercel\.app$|"
        r"^https://.*\.zendesk\.com$|"
        r"^https://.*\.apps\.zdusercontent\.com$|"
        r"^http://localhost:\d+$"
    ),
    allow_methods=["*"],
    allow_headers=["*"],
)

# -- Middleware: OmnigaTech (must be added before SZ so it's evaluated first)
app.add_middleware(OmnigaTechTenantMiddleware)
app.add_middleware(OmnigaTechAuthMiddleware)

# -- Middleware: Shopline-Zendesk v2
# Tenant middleware only applies to Shopline-Zendesk v2 endpoints by path filter.
app.add_middleware(SzV2TenantMiddleware)
app.add_middleware(SzV2AuthMiddleware)

# -- Tool: ImageLingo
app.include_router(auth.router, prefix="/api/imagelingo/auth")
app.include_router(translate.router, prefix="/api/imagelingo/translate")
app.include_router(products.router, prefix="/api/imagelingo/products")
app.include_router(webhook.router, prefix="/api/imagelingo/webhooks")

app.include_router(fitness_routes.router, prefix="/api")

# -- Shared: S3 upload (used by all tools)
app.include_router(s3_router, prefix="/api/shared/s3")

# -- Future tools
# app.include_router(tool2.router, prefix="/api/tool2")

# -- Tool: Shopline-Zendesk
# Route registration is grouped by frontend ownership for clarity:
# 1) Shopline App frontend
# 2) Zendesk ZAF frontend (legacy + v2)
include_shopline_frontend_routes(app)
include_zaf_frontend_routes(app)
include_oauth_routes(app)

# -- Tool: OmnigaTech
include_omnigatech_routes(app)

@app.on_event("startup")
async def _startup_env_check():
    logger.info("DaleToolMatrix starting up...")
    try:
        await sz_v2_create_tables()
        logger.info("Shopline-Zendesk v2 tables are ready")
    except Exception:
        logger.exception("Failed to initialize Shopline-Zendesk v2 tables")

    try:
        await create_omnigatech_tables()
        logger.info("OmnigaTech tables are ready")
    except Exception:
        logger.exception("Failed to initialize OmnigaTech tables")

    # Start Shopline token refresh background job (every 60 min)
    try:
        start_refresh_job(interval_minutes=60)
        logger.info("Shopline token refresh job started")
    except Exception:
        logger.exception("Failed to start token refresh job")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "daletoolmatrix"}


@app.on_event("shutdown")
async def _shutdown():
    stop_refresh_job()
    logger.info("Token refresh job stopped")
