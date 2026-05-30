from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


async def require_api_key(api_key: str = Security(_api_key_header)) -> None:
    from core.platform_cache import get_backend_api_key

    effective_key = get_backend_api_key()
    if not effective_key or api_key != effective_key:
        raise HTTPException(status_code=403, detail="Invalid API key")
