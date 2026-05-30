from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import settings


def _async_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


if settings.database_url:
    engine = create_async_engine(_async_url(settings.database_url), echo=False)
    SessionLocal: async_sessionmaker[AsyncSession] | None = async_sessionmaker(
        engine, expire_on_commit=False
    )
else:
    engine = None
    SessionLocal = None
