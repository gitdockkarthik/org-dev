import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from config import settings
from llm_gateway import chat

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)


class InvokeRequest(BaseModel):
    session_id: str
    user_message: str
    context: dict[str, Any] = Field(default_factory=dict)
    history: list[dict[str, Any]] = Field(default_factory=list)


class InvokeResponse(BaseModel):
    response: str
    session_id: str
    agent_id: str
    tokens_used: int


async def _register_self() -> None:
    if not settings.registry_url or not settings.backend_api_key:
        logger.info("Self-registration skipped")
        return
    headers = {"X-API-Key": settings.backend_api_key}
    base = settings.registry_url.rstrip("/")
    async with httpx.AsyncClient(timeout=10.0) as client:
        reg_resp = await client.post(
            f"{base}/api/registry/agents",
            json={
                "name": settings.agent_name,
                "slug": settings.agent_slug,
                "description": "Mock agent — demonstrates UAP LLM gateway integration.",
                "version": "0.1.0",
                "invoke_url": f"http://mock-agent:{settings.port}",
                "uses_uap_llm": True,
            },
            headers=headers,
        )
        if reg_resp.status_code in (201, 409):
            list_resp = await client.get(f"{base}/api/registry/agents", headers=headers)
            match = next((a for a in list_resp.json() if a["slug"] == settings.agent_slug), None)
            if match:
                await client.post(f"{base}/api/registry/agents/{match['id']}/publish", headers=headers)
                logger.info("Self-registration: published as %s", match['id'])


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await _register_self()
    except Exception:
        logger.exception("Self-registration failed (agent will still start)")
    yield


app = FastAPI(title=settings.agent_name, version="0.1.0", lifespan=lifespan)

app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "agent": settings.agent_slug}


@app.post("/invoke", response_model=InvokeResponse)
async def invoke(body: InvokeRequest) -> InvokeResponse:
    messages = []
    for item in body.history:
        if item.get("role") in ("user", "assistant"):
            messages.append({"role": item["role"], "content": item["content"]})
    messages.append({"role": "user", "content": body.user_message})

    response_text, tokens = await chat(messages, system=settings.agent_system_prompt)
    return InvokeResponse(
        response=response_text,
        session_id=body.session_id,
        agent_id=settings.agent_id,
        tokens_used=tokens,
    )


@app.post("/chat")
async def chat_endpoint(body: dict):
    history = body.get("history", [])
    message = body.get("message", "")
    messages = [{"role": m["role"], "content": m["content"]} for m in history if m.get("role") in ("user","assistant")]
    messages.append({"role": "user", "content": message})
    response_text, tokens = await chat(messages, system=settings.agent_system_prompt)
    return {"response": response_text, "tokens_used": tokens}
