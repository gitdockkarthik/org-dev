"""
In-memory cache for platform credentials loaded from DB on startup.
Falls back to env vars (settings) when cache is empty, so existing .env-based
deployments keep working without any DB rows.
"""
from core.config import settings

_anthropic_key: str = ""
_backend_api_key: str = ""


def get_anthropic_key() -> str:
    return _anthropic_key or settings.anthropic_api_key


def set_anthropic_key(key: str) -> None:
    global _anthropic_key
    _anthropic_key = key


def get_backend_api_key() -> str:
    return _backend_api_key or settings.backend_api_key


def set_backend_api_key(key: str) -> None:
    global _backend_api_key
    _backend_api_key = key
