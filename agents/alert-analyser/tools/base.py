from abc import ABC, abstractmethod
from typing import Any, ClassVar


class ToolExecutor(ABC):
    name: ClassVar[str]
    description: ClassVar[str]
    input_schema: ClassVar[dict[str, Any]]

    @abstractmethod
    async def execute(self, **kwargs: Any) -> Any:
        ...

    def to_anthropic_tool(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }
