"""UAP LLM Gateway client — fetches short-lived token and proxies LLM calls.
Fallback: if UAP_URL/BACKEND_API_KEY not configured, calls Anthropic directly
using ANTHROPIC_API_KEY from env — enables standalone deployment without UAP.
"""
import os
import httpx
import anthropic
from config import settings


async def chat(messages: list[dict], system: str = "") -> tuple[str, int]:
    # ── UAP gateway path (preferred when running inside UAP platform) ──────────
    if settings.uap_url and settings.backend_api_key:
        async with httpx.AsyncClient(timeout=60.0) as client:
            token_resp = await client.post(
                f"{settings.uap_url}/api/llm/token",
                headers={"X-API-Key": settings.backend_api_key},
                json={"agent_slug": settings.agent_slug},
            )
            token_resp.raise_for_status()
            token = token_resp.json()["token"]

            invoke_resp = await client.post(
                f"{settings.uap_url}/api/llm/invoke",
                headers={"X-API-Key": settings.backend_api_key},
                json={"token": token, "messages": messages, "system": system},
            )
            invoke_resp.raise_for_status()
            data = invoke_resp.json()
            usage = data.get("usage", {})
            tokens = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
            return data["response"], tokens

    # ── Standalone fallback path (uses ANTHROPIC_API_KEY from env) ────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "No LLM credentials available. Set UAP_URL+BACKEND_API_KEY "
            "for UAP-managed LLM, or ANTHROPIC_API_KEY for standalone."
        )
    client = anthropic.AsyncAnthropic(api_key=api_key)
    model = os.environ.get("LLM_MODEL", "claude-sonnet-4-6")
    kwargs = {"model": model, "max_tokens": 4096, "messages": messages}
    if system:
        kwargs["system"] = system
    response = await client.messages.create(**kwargs)
    await client.close()
    text = next((b.text for b in response.content if hasattr(b, "text")), "")
    tokens = response.usage.input_tokens + response.usage.output_tokens
    return text, tokens
