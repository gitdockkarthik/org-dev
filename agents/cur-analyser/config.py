import os
import sys
sys.path.insert(0, "/app")
from shared.llm import DEFAULT_MODEL

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Anthropic — injected via X-Anthropic-Key header by the backend orchestrator.
    # Set directly only when invoking the agent outside the orchestrated stack.
    anthropic_api_key: str = ""
    model: str = Field(default_factory=lambda: os.environ.get("LLM_MODEL", DEFAULT_MODEL), alias="LLM_MODEL")

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

    # ── Cache backend ────────────────────────────────────────────────────────
    cache_backend: str = Field(default="none", alias="CACHE_BACKEND")
    cache_url: str = Field(default="", alias="CACHE_URL")

    # ── Vector backend ───────────────────────────────────────────────────────
    vector_backend: str = Field(default="none", alias="VECTOR_BACKEND")
    vector_url: str = Field(default="", alias="VECTOR_URL")
    vector_api_key: str = Field(default="", alias="VECTOR_API_KEY")
    vector_index: str = Field(default="cur-analyser", alias="VECTOR_INDEX")

    # ── Graph backend ────────────────────────────────────────────────────────
    graph_backend: str = Field(default="none", alias="GRAPH_BACKEND")
    graph_url: str = Field(default="", alias="GRAPH_URL")
    graph_username: str = Field(default="neo4j", alias="GRAPH_USERNAME")
    graph_password: str = Field(default="", alias="GRAPH_PASSWORD")

    # ── Storage backend ──────────────────────────────────────────────────────
    storage_backend: str = Field(default="postgres", alias="STORAGE_BACKEND")
    storage_url: str = Field(default="", alias="STORAGE_URL")

    # ── Data-source abstraction / inventory enrichment ────────────────────────
    # Opt-in feature flag. When False (default) the inventory enrichment layer is
    # completely inert and the existing CUR flow is unaffected.
    enable_inventory_enrichment: bool = Field(
        default=False, alias="ENABLE_INVENTORY_ENRICHMENT"
    )
    # Inventory freshness threshold (hours) before a staleness warning is raised.
    inventory_stale_threshold_hours: int = Field(
        default=26, alias="INVENTORY_STALE_THRESHOLD_HOURS"
    )

    @property
    def agent_system_prompt(self) -> str:
        return (
            "You are an AWS cost intelligence agent specialising in Cost and Usage Report (CUR) analysis.\n\n"
            "IMPORTANT — PERFORMANCE RULE: When the request context contains pre_aggregated_cache data, "
            "use it DIRECTLY to answer questions WITHOUT calling any tools. The pre-aggregated data contains "
            "all tab breakdowns (overview, services, accounts, environments, tags) already computed.\n\n"
            "Only call build_dashboard if pre_aggregated_cache is NOT in the context.\n\n"
            "DATA AVAILABLE IN PRE-AGGREGATED CACHE:\n"
            "  overview    — total_cost, total_gross, total_net, credits_discounts, taxes, marketplace_total,\n"
            "                service_breakdown (top AWS services), daily_trend, monthly_trend\n"
            "  services    — line_item_breakdown (aws_services, marketplace, reserved_instances,\n"
            "                savings_plans, taxes, credits_discounts, total_gross, total_net)\n"
            "  accounts    — account_breakdown (account_id, account_name, cost, row_count)\n"
            "  environments — lifecycle_breakdown, hosting_env_breakdown\n"
            "  tags        — tag_application, tag_layer, tag_function, tag_budget_code\n\n"
            "ANSWERING RULES:\n"
            "  • Always cite exact figures from the data — never estimate or guess\n"
            "  • Format currency as $X,XXX.XX\n"
            "  • If a question needs data not in pre-aggregated cache, say what IS available and answer partially\n"
            "  • If enrichment is OFF, do not reference inventory or vendor tags (CrowdStrike, etc.)\n"
            "  • Keep responses concise — bullet points for breakdowns, prose for explanations\n"
            "  • For cross-dimensional questions (e.g. 'untagged EC2 spend'), call build_dashboard as fallback\n\n"
            "CUR CONTEXT:\n"
            "  Billing data is from AWS Cost & Usage Report (CUR 2.0)\n"
            "  line_item_product_code = AWS service name\n"
            "  line_item_unblended_cost = cost in USD\n"
            "  Marketplace charges appear as opaque product codes — shown as readable descriptions\n"
            "  Day 1 of month spike = RI/SP recurring fees billed monthly (expected behaviour)\n\n"
            "If no CUR data is loaded, ask the user to upload a CUR CSV or sync from S3."
        )


settings = Settings()
