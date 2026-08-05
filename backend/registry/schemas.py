import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from models.agent import AgentStatus
from shared.llm import DEFAULT_MODEL


class AgentCreate(BaseModel):
    name: str
    slug: str
    description: str = ""
    version: str = "0.1.0"
    invoke_url: str | None = None
    landing_page_url: str | None = None
    uses_uap_llm: bool = False
    system_prompt: str = ""
    model: str = DEFAULT_MODEL
    temperature: float = Field(default=0.7, ge=0.0, le=1.0)
    tools: list[dict[str, Any]] = Field(default_factory=list)


class AgentUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    version: str | None = None
    invoke_url: str | None = None
    landing_page_url: str | None = None
    uses_uap_llm: bool | None = None
    system_prompt: str | None = None
    model: str | None = None
    temperature: float | None = Field(default=None, ge=0.0, le=1.0)
    tools: list[dict[str, Any]] | None = None


class AgentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    slug: str
    description: str
    version: str
    status: AgentStatus
    invoke_url: str | None
    landing_page_url: str | None
    uses_uap_llm: bool
    system_prompt: str
    model: str
    temperature: float
    tools: list[Any]
    created_at: datetime
    updated_at: datetime


class AgentVersionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    agent_id: uuid.UUID
    version: str
    config_snapshot: dict[str, Any]
    created_at: datetime


class AccessAssign(BaseModel):
    user_email: str
