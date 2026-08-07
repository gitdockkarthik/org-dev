from __future__ import annotations
import logging
import os
from typing import Any

DEFAULT_MODEL = os.environ.get("LLM_MODEL", "us.anthropic.claude-sonnet-5")

# ── Langfuse tracing (optional — disabled if not configured) ─────────────────
def _get_langfuse():
    pk = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
    sk = os.environ.get("LANGFUSE_SECRET_KEY", "")
    host = os.environ.get("LANGFUSE_HOST", "http://langfuse:3000")
    if not pk or not sk:
        return None
    try:
        from langfuse import Langfuse
        return Langfuse(public_key=pk, secret_key=sk, host=host)
    except Exception:
        return None

_langfuse = None

def _lf():
    global _langfuse
    if _langfuse is None:
        _langfuse = _get_langfuse()
    return _langfuse

def _lf_trace(response: Any, model: str, provider: str, messages: list, user_id: str | None = None, session_id: str | None = None, agent_slug_override: str | None = None) -> None:
    """Send LLM call trace to Langfuse via direct SDK ingestion."""
    logger.debug("_lf_trace called: model=%s session=%s", model, session_id)
    try:
        lf = _lf()
        if not lf:
            return
        agent_slug = agent_slug_override or os.environ.get("AGENT_SLUG", "unknown")
        output_text = ""
        for block in (response.content or []):
            if hasattr(block, 'text'):
                output_text = block.text
                break
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        obs = lf.start_observation(
            as_type="generation",
            name=f"{agent_slug}.llm_call",
            model=model,
            input=messages,
            output=output_text,
            usage_details={
                "input": input_tokens,
                "output": output_tokens,
                "total": input_tokens + output_tokens,
            },
            metadata={"provider": provider, "session_id": session_id, "user_email": user_id},
        )
        obs.end()
        try:
            lf.flush()
        except Exception:
            pass
    except Exception as e:
        logger.debug("Langfuse trace failed: %s", e)

logger = logging.getLogger(__name__)

def _provider() -> str:
    return os.environ.get("LLM_PROVIDER", "anthropic").lower().strip()

def _model() -> str:
    return os.environ.get("LLM_MODEL", DEFAULT_MODEL)

async def create_message(
    *,
    model: str | None = None,
    max_tokens: int = 4096,
    system: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    api_key: str | None = None,
    provider: str | None = None,
    user_id: str | None = None,
    session_id: str | None = None,
    agent_slug_override: str | None = None,
) -> Any:
    """Provider-agnostic wrapper around the Anthropic messages API.

    Provider resolution order:
      1. ``provider`` argument (explicit override)
      2. ``LLM_PROVIDER`` env var (platform/agent level)
      3. Default: ``anthropic``

    Model resolution order:
      1. ``model`` argument
      2. ``LLM_MODEL`` env var
      3. Default: ``us.anthropic.claude-sonnet-5``
    """
    resolved_provider = (provider or _provider()).lower()
    # LLM_MODEL env var takes precedence when explicitly set;
    # falls back to the model argument (from agent config / DB), then the default.
    env_model = os.environ.get("LLM_MODEL", "").strip()
    resolved_model = env_model if env_model else (model or DEFAULT_MODEL)
    if not env_model and not model:
        logger.warning("LLM_MODEL not set; falling back to DEFAULT_MODEL=%s", DEFAULT_MODEL)

    kwargs: dict[str, Any] = {
        "model": resolved_model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
    }
    if tools:
        kwargs["tools"] = tools

    # Langfuse tracing handled via @observe decorator below
    pass

    if resolved_provider == "anthropic":
        import anthropic
        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not resolved_key:
            raise RuntimeError("Set ANTHROPIC_API_KEY env var.")
        client = anthropic.AsyncAnthropic(api_key=resolved_key)
        try:
            response = await client.messages.create(**kwargs)
            _lf_trace(response, resolved_model, resolved_provider, messages, user_id=user_id, session_id=session_id, agent_slug_override=agent_slug_override)
            return response
        finally:
            await client.close()

    if resolved_provider == "bedrock":
        import anthropic
        # Bedrock model IDs require an 'anthropic.' or region-prefixed
        # (e.g. 'us.anthropic.') prefix for inference profiles.
        if not resolved_model.startswith("anthropic.") and not resolved_model.startswith("us.anthropic.") and not resolved_model.startswith("eu.anthropic.") and not resolved_model.startswith("apac.anthropic."):
            resolved_model = f"anthropic.{resolved_model}"
        kwargs["model"] = resolved_model
        # AsyncAnthropicBedrock uses the Messages API endpoint.
        # Resolves region from AWS_REGION / AWS_DEFAULT_REGION env vars automatically.
        # Picks up AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY from env.
        client = anthropic.AsyncAnthropicBedrock()
        try:
            response = await client.messages.create(**kwargs)
            _lf_trace(response, resolved_model, resolved_provider, messages, user_id=user_id, session_id=session_id, agent_slug_override=agent_slug_override)
            return response
        finally:
            await client.close()

    if resolved_provider == "vertex":
        import anthropic
        project_id = os.environ.get("VERTEX_PROJECT_ID", "")
        region = os.environ.get("VERTEX_REGION", "us-east5")
        client = anthropic.AsyncAnthropicVertex(project_id=project_id, region=region)
        try:
            return await client.messages.create(**kwargs)
        finally:
            await client.close()

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
    tools: list[dict[str, Any]] | None = None,
    tool_executor: Any = None,
    api_key: str | None = None,
    provider: str | None = None,
    session_id: str | None = None,
    agent_slug_override: str | None = None,
):
    """Stream messages with optional tool_use support.

    Args:
        tool_executor: Optional async callable(name: str, input: dict) -> str
                       that executes tools. If provided and stop_reason is
                       'tool_use', automatically executes tools and continues.
    """
    resolved_provider = (provider or _provider()).lower()
    env_model = os.environ.get("LLM_MODEL", "").strip()
    resolved_model = env_model if env_model else (model or DEFAULT_MODEL)
    if not env_model and not model:
        logger.warning("LLM_MODEL not set; falling back to DEFAULT_MODEL=%s", DEFAULT_MODEL)

    async def _stream_with_tools(client: Any, model_id: str, msg_list: list[dict[str, Any]]):
        """Shared streaming loop with tool_use support across all providers."""
        while True:
            kwargs: dict[str, Any] = {"model": model_id, "max_tokens": max_tokens, "messages": msg_list}
            if system:
                kwargs["system"] = system
            if tools:
                kwargs["tools"] = tools

            async with client.messages.stream(**kwargs) as stream:
                async for text in stream.text_stream:
                    yield text
                try:
                    final = await stream.get_final_message()
                except Exception as _fe:
                    logger.debug("get_final_message failed: %s", _fe)
                    yield "[STOP_REASON] end_turn"
                    return

            if final.stop_reason == "tool_use" and tool_executor is not None:
                msg_list.append({"role": "assistant", "content": final.content})

                tool_results = []
                for content_block in final.content:
                    if content_block.type == "tool_use":
                        try:
                            result = await tool_executor(content_block.name, content_block.input)
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": content_block.id,
                                "content": result
                            })
                        except Exception as e:
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": content_block.id,
                                "content": f"Error: {str(e)}",
                                "is_error": True
                            })

                if tool_results:
                    msg_list.append({"role": "user", "content": tool_results})
            else:
                try:
                    _lf_trace(final, model_id, resolved_provider, msg_list,
                              session_id=session_id, agent_slug_override=agent_slug_override)
                except Exception:
                    pass
                yield f"[STOP_REASON] {final.stop_reason}"
                return

    if resolved_provider == "anthropic":
        import anthropic
        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not resolved_key:
            raise RuntimeError("Set ANTHROPIC_API_KEY env var.")
        client = anthropic.AsyncAnthropic(api_key=resolved_key)
        try:
            async for chunk in _stream_with_tools(client, resolved_model, messages):
                yield chunk
        finally:
            await client.close()

    elif resolved_provider == "bedrock":
        import anthropic
        # Bedrock model IDs require an 'anthropic.' or region-prefixed
        # (e.g. 'us.anthropic.') prefix for inference profiles.
        model_id = resolved_model
        if not model_id.startswith("anthropic.") and not model_id.startswith("us.anthropic.") and not model_id.startswith("eu.anthropic.") and not model_id.startswith("apac.anthropic."):
            model_id = f"anthropic.{model_id}"
        client = anthropic.AsyncAnthropicBedrock()
        try:
            async for chunk in _stream_with_tools(client, model_id, messages):
                yield chunk
        finally:
            await client.close()

    elif resolved_provider == "vertex":
        import anthropic
        project_id = os.environ.get("VERTEX_PROJECT_ID", "")
        region = os.environ.get("VERTEX_REGION", "us-east5")
        client = anthropic.AsyncAnthropicVertex(project_id=project_id, region=region)
        try:
            async for chunk in _stream_with_tools(client, resolved_model, messages):
                yield chunk
        finally:
            await client.close()

    else:
        raise ValueError(f"Unsupported LLM_PROVIDER '{resolved_provider}'.")
