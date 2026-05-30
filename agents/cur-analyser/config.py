from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Anthropic — injected via X-Anthropic-Key header by the backend orchestrator.
    # Set directly only when invoking the agent outside the orchestrated stack.
    anthropic_api_key: str = ""
    model: str = "claude-sonnet-4-6"

    # Agent identity
    agent_id: str = ""
    agent_slug: str = "cur-analyser"
    agent_name: str = "CUR Analyser"

    # Self-registration
    registry_url: str = ""
    backend_api_key: str = ""

    # Database
    database_url: str = ""

    # Auto-sync interval; 0 disables the background task
    sync_interval_minutes: int = 0

    # Server
    port: int = 8002

    @property
    def agent_system_prompt(self) -> str:
        return (
            "You are an AWS cost intelligence agent specialising in Cost and Usage Report (CUR) analysis.\n\n"
            "When CUR data is available for the session, use your tools:\n"
            "  • query_cur       — run a targeted DuckDB query: total_cost | cost_by_service | daily_trend | cost_by_region\n"
            "  • build_dashboard — compute the full overview: totals, service breakdown, daily/monthly trend, regions\n\n"
            "Always ground answers in tool output. Format all currency values as $X,XXX.XX.\n"
            "Be specific: name services, cite percentages, flag anomalies.\n"
            "When recommending savings actions, be concrete (e.g. 'right-size EC2 instance type', 'enable S3 Intelligent-Tiering').\n\n"
            "CUR canonical columns used:\n"
            "  line_item_product_code        — AWS service name\n"
            "  line_item_unblended_cost      — cost in USD\n"
            "  line_item_usage_start_date    — usage date\n"
            "  line_item_usage_account_id    — AWS account\n"
            "  product_region                — AWS region\n\n"
            "When including charts, embed a JSON block at the end of your response:\n"
            "```chart\n"
            '{\"type\": \"bar\", \"labels\": [...], \"datasets\": [{\"label\": \"...\", \"data\": [...]}]}\n'
            "```\n\n"
            "If no CUR data is loaded, ask the user to upload a CUR CSV export or generate sample data."
        )


settings = Settings()
