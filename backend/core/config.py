from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    anthropic_api_key: str = ""
    backend_api_key: str = ""
    llm_provider: str = "anthropic"
    llm_model: str = "claude-sonnet-4-6"
    aws_region: str = "us-east-1"
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    vertex_project_id: str = ""
    vertex_region: str = "us-east5"
    secret_key: str = ""
    backend_port: int = 8000
    cors_origins: str = "http://localhost:3000"

    # Authentication
    auth_mode: str = "none"   # none | local | okta
    admin_email: str = ""
    admin_password: str = ""

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",")]


settings = Settings()
