import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as pgUUID
from sqlalchemy.orm import Mapped, mapped_column

from core.database import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AgentStatus(str, Enum):
    draft = "draft"
    published = "published"
    deprecated = "deprecated"


class Agent(Base):
    __tablename__ = "agents"

    id: Mapped[uuid.UUID] = mapped_column(
        pgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    description: Mapped[str] = mapped_column(String, nullable=False, default="")
    version: Mapped[str] = mapped_column(String, nullable=False, default="0.1.0")
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=AgentStatus.draft
    )
    invoke_url: Mapped[str | None] = mapped_column(String, nullable=True)
    landing_page_url: Mapped[str | None] = mapped_column(String, nullable=True)
    uses_uap_llm: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    system_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    model: Mapped[str] = mapped_column(
        String, nullable=False, default="claude-sonnet-4-6"
    )
    temperature: Mapped[float] = mapped_column(Float, nullable=False, default=0.7)
    tools: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )
