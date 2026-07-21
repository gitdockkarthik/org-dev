import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = ""
    api_key: str = Field(default_factory=lambda: os.environ.get("API_KEY", ""), alias="API_KEY")
    port: int = 8020


settings = Settings()
