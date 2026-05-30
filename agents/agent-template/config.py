from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    anthropic_api_key: str
    model: str = "claude-sonnet-4-6"

    agent_id: str = ""
    agent_slug: str
    agent_name: str
    agent_system_prompt: str = "You are a helpful assistant."

    registry_url: str = ""
    backend_api_key: str = ""

    port: int = 8001


settings = Settings()
