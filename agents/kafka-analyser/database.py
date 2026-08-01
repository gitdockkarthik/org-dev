from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import settings


def _async_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


if settings.database_url:
    engine = create_async_engine(
        _async_url(settings.database_url), echo=False,
        pool_size=20, max_overflow=20, pool_timeout=30,
    )
    SessionLocal: async_sessionmaker[AsyncSession] | None = async_sessionmaker(
        engine, expire_on_commit=False
    )

    # Dedicated pool for dashboard/UI reads -- kept separate from the pool above
    # (used by background collector jobs) so heavy concurrent collection work can
    # never starve interactive dashboard queries. See BACKLOG.md for the incident
    # this fixes: confirmed via live testing that 4-6 concurrent collector jobs
    # caused dashboard reads to wait 30+ seconds for a free connection.
    dashboard_engine = create_async_engine(
        _async_url(settings.database_url), echo=False,
        pool_size=10, max_overflow=10, pool_timeout=30,
    )
    DashboardSessionLocal: async_sessionmaker[AsyncSession] | None = async_sessionmaker(
        dashboard_engine, expire_on_commit=False
    )
else:
    engine = None
    SessionLocal = None
    dashboard_engine = None
    DashboardSessionLocal = None
