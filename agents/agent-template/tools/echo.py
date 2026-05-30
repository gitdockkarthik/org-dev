from typing import Any

from tools.base import ToolExecutor


class EchoTool(ToolExecutor):
    """Example tool — returns the input text unchanged.

    Use this to verify tool-use wiring works end-to-end, then replace or
    extend it with tools that actually do something (API calls, SQL queries,
    file lookups, etc.).
    """

    name = "echo"
    description = (
        "Returns the input text unchanged. "
        "Useful for confirming that tool-use is wired up correctly."
    )
    input_schema = {
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The text to echo back.",
            }
        },
        "required": ["text"],
    }

    async def execute(self, **kwargs: Any) -> str:
        return kwargs.get("text", "")
