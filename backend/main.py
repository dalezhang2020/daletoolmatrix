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
from backend.tools.shopline_zendesk.routes.shopline import install as sz_install
from backend.tools.shopline_zendesk.routes.shopline import binding as sz_binding
from backend.tools.shopline_zendesk.routes.shopline import session as sz_session
from backend.tools.shopline_zendesk.routes.shopline import webhook as sz_webhook
from backend.tools.shopline_zendesk.routes.shopline import customers as sz_customers
from backend.tools.shopline_zendesk.routes.shopline import zendesk_oauth as sz_zendesk_oauth
from backend.tools.shopline_zendesk.routes.zendesk import customer as sz_customer

logger = logging.getLogger(__name__)

app = FastAPI(title="DaleToolMatrix API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        os.getenv("FRONTEND_URL", ""),
        os.getenv("SHOPLINE_ZD_FRONTEND_URL", ""),
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_methods=["*"],
    allow_headers=["*"],
)

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
app.include_router(sz_install.router,  prefix="/api/shopline-zendesk/shopline")
app.include_router(sz_binding.router,  prefix="/api/shopline-zendesk/shopline")
app.include_router(sz_session.router,  prefix="/api/shopline-zendesk/shopline")
app.include_router(sz_webhook.router,    prefix="/api/shopline-zendesk/shopline")
app.include_router(sz_customers.router,     prefix="/api/shopline-zendesk/shopline")
app.include_router(sz_zendesk_oauth.router, prefix="/api/shopline-zendesk/shopline")
app.include_router(sz_customer.router,      prefix="/api/shopline-zendesk/zendesk")

@app.on_event("startup")
async def _startup_env_check():
    logger.info("DaleToolMatrix starting up...")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "daletoolmatrix"}
