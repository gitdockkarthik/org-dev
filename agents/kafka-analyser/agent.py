import json
from typing import Any

from shared.llm import create_message as llm_create_message

from config import settings
from tools.base import ToolExecutor


class AgentRunner:
    """Stateless Claude invocation wrapper with tool-use loop."""

    def __init__(self, tools: list[ToolExecutor] | None = None) -> None:
        self._tool_map: dict[str, ToolExecutor] = {t.name: t for t in (tools or [])}

    @property
    def _anthropic_tools(self) -> list[dict[str, Any]]:
        return [t.to_anthropic_tool() for t in self._tool_map.values()]

    def _build_system(self, context: dict[str, Any]) -> str:
        system = settings.agent_system_prompt
        if context:
            system += f"\n\n## Request context\n{json.dumps(context, indent=2)}"
        return system

    def _build_messages(
        self, history: list[dict[str, Any]], user_message: str
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for item in history:
            role = item.get("role")
            if role in ("user", "assistant"):
                messages.append({"role": role, "content": item["content"]})
        messages.append({"role": "user", "content": user_message})
        return messages

    async def run(
        self,
        user_message: str,
        context: dict[str, Any],
        history: list[dict[str, Any]],
        api_key: str | None = None,
    ) -> tuple[str, int]:
        resolved_key = api_key or settings.anthropic_api_key
        if not resolved_key:
            raise RuntimeError(
                "No Anthropic API key available. "
                "Set ANTHROPIC_API_KEY env var or pass X-Anthropic-Key header."
            )
        system = self._build_system(context)
        messages = self._build_messages(history, user_message)
        total_tokens = 0

        while True:
            response = await llm_create_message(
                model=settings.model,
                max_tokens=4096,
                system=system,
                messages=messages,
                tools=self._anthropic_tools if self._tool_map else None,
                api_key=resolved_key,
            )
            total_tokens += response.usage.input_tokens + response.usage.output_tokens

            if response.stop_reason != "tool_use":
                text = next(
                    (b.text for b in response.content if hasattr(b, "text")), ""
                )
                return text, total_tokens

            messages.append({"role": "assistant", "content": response.content})

            tool_results: list[dict[str, Any]] = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                executor = self._tool_map.get(block.name)
                if executor is None:
                    content = f"Unknown tool '{block.name}'"
                else:
                    try:
                        result = await executor.execute(**block.input)
                        content = (
                            result
                            if isinstance(result, str)
                            else json.dumps(result, default=str)
                        )
                    except Exception as exc:
                        content = f"Tool '{block.name}' error: {exc}"

                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": content,
                    }
                )

            messages.append({"role": "user", "content": tool_results})
