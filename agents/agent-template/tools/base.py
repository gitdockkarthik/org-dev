from abc import ABC, abstractmethod
from typing import Any, ClassVar


class ToolExecutor(ABC):
    """Base class for all agent tools.

    Subclass this, set the three class variables, and implement execute().
    The platform passes **block.input from Claude's tool_use response directly
    to execute(), so parameter names must match your input_schema properties.
    """

    name: ClassVar[str]
    description: ClassVar[str]
    input_schema: ClassVar[dict[str, Any]]

    @abstractmethod
    async def execute(self, **kwargs: Any) -> Any:
        """Run the tool. Return a str or a JSON-serialisable value."""
        ...

    def to_anthropic_tool(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }
