from abc import ABC, abstractmethod
from typing import Any, ClassVar


class KafkaCollector(ABC):
    """Abstract base for all Kafka data collectors."""

    @abstractmethod
    async def collect(self) -> dict[str, Any]:
        """Collect current cluster state.

        Returns a dict with keys:
        cluster, brokers, consumer_groups, topics, connectors, anomalies
        """
        ...


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
