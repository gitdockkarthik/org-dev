from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # UAP gateway config — no LLM key stored here
    uap_url: str = "http://backend:8000"
    backend_api_key: str = ""

    agent_id: str = ""
    agent_slug: str = "mock-agent"
    agent_name: str = "Mock Agent"
    agent_system_prompt: str = "You are a helpful assistant built on the UAP platform."

    registry_url: str = ""
    port: int = 8099

settings = Settings()
