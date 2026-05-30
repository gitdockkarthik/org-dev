from pydantic import BaseModel, HttpUrl


class AgentManifest(BaseModel):
    name: str
    slug: str
    description: str
    version: str
    invoke_url: str
    capabilities: list[str] = []
