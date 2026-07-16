"""UAP LLM Gateway client — fetches short-lived token and proxies LLM calls."""
import httpx
from config import settings

async def chat(messages: list[dict], system: str = "") -> tuple[str, int]:
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Step 1: fetch token
        token_resp = await client.post(
            f"{settings.uap_url}/api/llm/token",
            headers={"X-API-Key": settings.backend_api_key},
            json={"agent_slug": settings.agent_slug},
        )
        token_resp.raise_for_status()
        token = token_resp.json()["token"]

        # Step 2: invoke LLM via UAP proxy
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
