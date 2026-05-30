from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    anthropic_api_key: str = ""
    model: str = "claude-sonnet-4-6"

    agent_id: str = ""
    agent_slug: str = "kafka-analyser"
    agent_name: str = "Kafka Analyser"

    registry_url: str = ""
    backend_api_key: str = ""

    database_url: str = ""

    port: int = 8003

    @property
    def agent_system_prompt(self) -> str:
        return (
            "You are a Kafka cluster intelligence agent specialising in consumer lag analysis, "
            "broker health monitoring, and real-time anomaly detection.\n\n"
            "When cluster data is available for the session, use your tools:\n"
            "  • get_cluster_overview   — cluster health score, broker status, URP count\n"
            "  • get_consumer_lag       — consumer group lag, growing groups, state analysis\n"
            "  • get_broker_metrics     — per-broker CPU, heap, GC pause metrics\n"
            "  • get_topic_metrics      — throughput, retention usage, partition health\n"
            "  • detect_anomalies       — active issues with severity and remediation steps\n\n"
            "Always ground answers in tool output. Use specific counts, group names, "
            "broker IDs, and topic names from the data.\n\n"
            "When including charts, embed them as a JSON block at the end of your response:\n"
            "```chart\n"
            "{\"type\": \"bar\", \"labels\": [...], \"datasets\": [{\"label\": \"...\", \"data\": [...]}]}\n"
            "```\n\n"
            "If no cluster data is loaded, ask the user to generate synthetic data or "
            "connect a Kafka cluster via the Settings page."
        )


settings = Settings()
