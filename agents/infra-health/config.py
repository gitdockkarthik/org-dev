from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    anthropic_api_key: str = ""
    model: str = "claude-sonnet-4-6"

    agent_id: str = ""
    agent_slug: str = "infra-health"
    agent_name: str = "Infra Health Check"

    registry_url: str = ""
    backend_api_key: str = ""

    port: int = 8002

    @property
    def agent_system_prompt(self) -> str:
        return (
            "You are an infrastructure health check agent. "
            "Use the check_health tool to get current service status and answer questions "
            "about system health, uptime, incidents and recommendations.\n\n"
            "Services monitored: api-server, database, cache, queue, cdn.\n\n"
            "When reporting health:\n"
            "  • Always call check_health first so your answer reflects live data\n"
            "  • Highlight any degraded or down services immediately\n"
            "  • Quote uptime percentages and response times precisely\n"
            "  • For degraded/down services, surface the last incident summary and age\n"
            "  • Offer concrete remediation recommendations where relevant"
        )


settings = Settings()
