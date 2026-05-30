import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ── Invoke contract (mirrors shared/schemas.py — kept local so the backend
#    Dockerfile can build from the backend/ directory without needing shared/) ──

class InvokeRequest(BaseModel):
    session_id: str
    user_message: str
    context: dict[str, Any] = Field(default_factory=dict)
    history: list[dict[str, Any]] = Field(default_factory=list)


class InvokeResponse(BaseModel):
    session_id: str
    response: str
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Chat history ───────────────────────────────────────────────────────────────

class ChatMessageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    session_id: uuid.UUID
    role: str
    content: str
    tokens_used: int | None
    created_at: datetime


class SessionHistoryResponse(BaseModel):
    session_id: uuid.UUID
    agent_slug: str | None
    messages: list[ChatMessageResponse]
    created_at: datetime
