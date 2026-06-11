import logging
import os

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

_key = os.getenv("ENCRYPTION_KEY")
_fernet = Fernet(_key.encode()) if _key else None


def encrypt(value: str) -> str:
    if not _fernet:
        return value
    return _fernet.encrypt(value.encode()).decode()


def decrypt(value: str) -> str:
    if not _fernet:
        logger.debug("decrypt: ENCRYPTION_KEY not set, returning value as-is")
        return value
    try:
        return _fernet.decrypt(value.encode()).decode()
    except InvalidToken:
        logger.warning(
            "decrypt: InvalidToken — ENCRYPTION_KEY mismatch or value was not encrypted; "
            "returning raw value (json.loads may fail)"
        )
        return value
    except Exception as exc:
        logger.warning("decrypt: unexpected error (%s); returning raw value", exc)
        return value


def is_secret_key(key: str) -> bool:
    secrets = {"api_key", "token", "password", "secret", "url", "cloud_id", "teams_webhook_url"}
    key_lower = key.lower()
    return any(s in key_lower for s in secrets)
