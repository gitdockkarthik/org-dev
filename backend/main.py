import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models  # noqa: F401 — registers all ORM models with Base metadata
from core.config import settings
from core.database import AsyncSessionLocal, Base, engine
from core.encryption import decrypt, encrypt, is_secret_key
from core.platform_cache import set_anthropic_key, set_backend_api_key
from models.platform_config import PlatformConfig
from orchestrator.platform import router as platform_router
from orchestrator.router import router as orchestrator_router
from registry.router import router as registry_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


async def _seed_platform_config() -> None:
    """Seed env-var credentials into DB (if not already present), then populate cache."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        for key, value in [
            ("anthropic_api_key", settings.anthropic_api_key),
            ("backend_api_key", settings.backend_api_key),
        ]:
            if value:
                stored = encrypt(value) if is_secret_key(key) else value
                stmt = (
                    pg_insert(PlatformConfig)
                    .values(key=key, value=stored, updated_at=now)
                    .on_conflict_do_nothing(index_elements=["key"])
                )
                await session.execute(stmt)
        await session.commit()

        rows = (await session.execute(select(PlatformConfig))).scalars().all()
        db_cfg = {
            r.key: (decrypt(r.value) if is_secret_key(r.key) else r.value)
            for r in rows
        }

    if db_cfg.get("anthropic_api_key"):
        set_anthropic_key(db_cfg["anthropic_api_key"])
        logger.info("Platform config: Anthropic API key loaded from DB")
    if db_cfg.get("backend_api_key"):
        set_backend_api_key(db_cfg["backend_api_key"])
        logger.info("Platform config: backend API key loaded from DB")
    if not db_cfg:
        logger.info("Platform config: no config in DB — setup wizard required")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Step 1: create tables from ORM metadata ──────────────────────────────
    try:
        logger.info("Startup [1/3]: running Base.metadata.create_all")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Startup [1/3]: schema ready")
    except Exception:
        logger.exception("Startup [1/2] FAILED: create_all raised an exception")
        sys.exit(1)

    # ── Step 2: verify connectivity with a live connection ───────────────────
    try:
        logger.info("Startup [2/3]: verifying database connectivity")
        async with engine.connect() as conn:
            logger.info("Startup [2/3]: connection acquired — %r", conn)
        logger.info("Startup [2/3]: database connectivity OK")
    except Exception:
        logger.exception("Startup [2/3] FAILED: engine could not connect to the database")
        sys.exit(1)

    # ── Step 3: seed env-var credentials into DB, then load cache ───────────────
    try:
        logger.info("Startup [3/3]: seeding and loading platform config")
        await _seed_platform_config()
        logger.info("Startup [3/3]: platform config ready")
    except Exception:
        logger.exception("Startup [3/3] WARNING: platform config seed/load failed (non-fatal)")

    logger.info("Startup complete — application is ready to serve requests")
    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    try:
        logger.info("Shutdown: disposing SQLAlchemy engine")
        await engine.dispose()
        logger.info("Shutdown: engine disposed cleanly")
    except Exception:
        logger.exception("Shutdown error: engine.dispose() raised an exception")


app = FastAPI(
    title="Unified Agent Platform — Backend",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(registry_router)
app.include_router(orchestrator_router)
app.include_router(platform_router)
