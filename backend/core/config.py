from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    anthropic_api_key: str = ""
    backend_api_key: str = ""
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
