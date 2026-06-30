from __future__ import annotations
import os
from typing import Any

def _provider() -> str:
    return os.environ.get("LLM_PROVIDER", "anthropic").lower().strip()

def _model() -> str:
    return os.environ.get("LLM_MODEL", "claude-sonnet-4-6")

async def create_message(
    *,
    model: str | None = None,
    max_tokens: int = 4096,
    system: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    api_key: str | None = None,
    provider: str | None = None,
) -> Any:
    """Provider-agnostic wrapper around the Anthropic messages API.

    Provider resolution order:
      1. ``provider`` argument (explicit override)
      2. ``LLM_PROVIDER`` env var (platform/agent level)
      3. Default: ``anthropic``

    Model resolution order:
      1. ``model`` argument
      2. ``LLM_MODEL`` env var
      3. Default: ``claude-sonnet-4-6``
    """
    resolved_provider = (provider or _provider()).lower()
    # LLM_MODEL env var takes precedence when explicitly set;
    # falls back to the model argument (from agent config / DB), then the default.
    env_model = os.environ.get("LLM_MODEL", "").strip()
    resolved_model = env_model if env_model else (model or "claude-sonnet-4-6")

    kwargs: dict[str, Any] = {
        "model": resolved_model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
    }
    if tools:
        kwargs["tools"] = tools

    if resolved_provider == "anthropic":
        import anthropic
        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not resolved_key:
            raise RuntimeError("Set ANTHROPIC_API_KEY env var.")
        client = anthropic.AsyncAnthropic(api_key=resolved_key)
        return await client.messages.create(**kwargs)

    if resolved_provider == "bedrock":
        import anthropic
        # Bedrock model IDs require the 'anthropic.' prefix.
        if not resolved_model.startswith("anthropic."):
            resolved_model = f"anthropic.{resolved_model}"
        kwargs["model"] = resolved_model
        # AsyncAnthropicBedrockMantle uses the Messages API endpoint.
        # Resolves region from AWS_REGION / AWS_DEFAULT_REGION env vars automatically.
        # Picks up AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY from env.
        client = anthropic.AsyncAnthropicBedrockMantle()
        return await client.messages.create(**kwargs)

    if resolved_provider == "vertex":
        import anthropic
        project_id = os.environ.get("VERTEX_PROJECT_ID", "")
        region = os.environ.get("VERTEX_REGION", "us-east5")
        client = anthropic.AsyncAnthropicVertex(project_id=project_id, region=region)
        return await client.messages.create(**kwargs)

    raise ValueError(
        f"Unsupported LLM_PROVIDER '{resolved_provider}'. "
        "Supported: anthropic, bedrock, vertex."
    )


async def stream_message(
    *,
    model: str | None = None,
    max_tokens: int = 8192,
    system: str | None = None,
    messages: list[dict[str, Any]],
    api_key: str | None = None,
    provider: str | None = None,
):
    resolved_provider = (provider or _provider()).lower()
    env_model = os.environ.get("LLM_MODEL", "").strip()
    resolved_model = env_model if env_model else (model or "claude-sonnet-4-6")
    kwargs: dict[str, Any] = {"model": resolved_model, "max_tokens": max_tokens, "messages": messages}
    if system:
        kwargs["system"] = system

    if resolved_provider == "anthropic":
        import anthropic
        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not resolved_key:
            raise RuntimeError("Set ANTHROPIC_API_KEY env var.")
        client = anthropic.AsyncAnthropic(api_key=resolved_key)
        async with client.messages.stream(**kwargs) as stream:
            async for text in stream.text_stream:
                yield text
            try:
                final = await stream.get_final_message()
                yield f"[STOP_REASON] {final.stop_reason}"
            except Exception:
                yield "[STOP_REASON] end_turn"

    elif resolved_provider == "bedrock":
        import anthropic
        if not resolved_model.startswith("anthropic."):
            resolved_model = f"anthropic.{resolved_model}"
        kwargs["model"] = resolved_model
        client = anthropic.AsyncAnthropicBedrockMantle()
        async with client.messages.stream(**kwargs) as stream:
            async for text in stream.text_stream:
                yield text
            try:
                final = await stream.get_final_message()
                yield f"[STOP_REASON] {final.stop_reason}"
            except Exception:
                yield "[STOP_REASON] end_turn"

    elif resolved_provider == "vertex":
        import anthropic
        project_id = os.environ.get("VERTEX_PROJECT_ID", "")
        region = os.environ.get("VERTEX_REGION", "us-east5")
        client = anthropic.AsyncAnthropicVertex(project_id=project_id, region=region)
        async with client.messages.stream(**kwargs) as stream:
            async for text in stream.text_stream:
                yield text
            try:
                final = await stream.get_final_message()
                yield f"[STOP_REASON] {final.stop_reason}"
            except Exception:
                yield "[STOP_REASON] end_turn"

    else:
        raise ValueError(f"Unsupported LLM_PROVIDER '{resolved_provider}'.")
