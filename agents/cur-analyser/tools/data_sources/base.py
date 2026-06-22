"""Base interfaces for the CUR Analyser data-source abstraction.

These are pure declarations — dataclasses + abstract base classes — with no
runtime side effects on import. Concrete providers live alongside this module
(``file_providers.py`` for Phase 1; Cost Explorer / S3 / script providers for
Phase 2) and only ever need to implement the small surface defined here.

Design notes
------------
* ``CURDataProvider`` yields raw CUR **CSV text** — the exact contract the rest
  of the agent already consumes (``duckdb_engine``, ``report_store``). A new
  provider type is therefore transparent to every downstream consumer.
* ``InventoryDataProvider`` yields an **enrichment lookup** keyed by
  ``(account_id, resource_id)`` plus per-sheet metadata. The enricher
  (``inventory_enricher``) is the sole consumer.
* ``supports_auto_sync()`` lets the registry decide whether a background
  scheduler (APScheduler) needs to be instantiated at all. File-upload
  providers return ``False``; Phase-2 API/S3 providers will return ``True``.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional

# Sync cadence of a data source.
SyncMode = Literal["manual", "scheduled", "realtime"]

# Lifecycle status of a data source.
SourceStatus = Literal["active", "stale", "error", "syncing"]


@dataclass
class DataSourceMeta:
    """Lightweight, serialisable description of a single data source.

    Persisted (as part of the registry blob) and returned by the
    ``/data-sources/status`` API. Times are ISO-8601 strings (UTC) so the blob
    is trivially JSON-serialisable and timezone-unambiguous.
    """

    source_id: str
    source_type: str                       # e.g. "file_cur", "file_inventory", "cost_explorer"
    label: str
    last_synced: Optional[str] = None       # ISO-8601 UTC, or None if never synced
    sync_mode: SyncMode = "manual"
    date_range_start: Optional[str] = None   # ISO date (CUR usage window start)
    date_range_end: Optional[str] = None     # ISO date (CUR usage window end)
    record_count: int = 0
    status: SourceStatus = "active"
    stale_threshold_hours: int = 26
    # Free-form provider-specific details (filename, per-sheet counts, errors…).
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataSourceMeta":
        known = {f: data.get(f) for f in cls.__dataclass_fields__ if f in data}
        # `extra` must never be None
        if known.get("extra") is None:
            known["extra"] = {}
        return cls(**known)  # type: ignore[arg-type]


# ── CUR provider interface ────────────────────────────────────────────────────


class CURDataProvider(ABC):
    """A source of AWS CUR data.

    Implementations return CUR rows as raw **CSV text** — the canonical format
    already consumed across the agent. This keeps every downstream consumer
    (DuckDB engine, report store, dashboard) provider-agnostic.
    """

    #: Stable identifier for the provider *type* (not the instance).
    source_type: str = "cur"

    @abstractmethod
    async def fetch(self) -> str:
        """Return the CUR data as raw CSV text."""
        ...

    @abstractmethod
    def get_date_ranges(self) -> tuple[Optional[str], Optional[str]]:
        """Return ``(start_iso_date, end_iso_date)`` of the CUR usage window.

        Either element may be ``None`` if the range cannot be determined.
        """
        ...

    @abstractmethod
    def supports_auto_sync(self) -> bool:
        """Whether this provider can be refreshed on a schedule without user
        interaction. File uploads return ``False``; API/S3 providers ``True``."""
        ...

    @abstractmethod
    def get_meta(self) -> DataSourceMeta:
        """Return a current ``DataSourceMeta`` snapshot for this provider."""
        ...


# ── Inventory provider interface ───────────────────────────────────────────────


class InventoryDataProvider(ABC):
    """A source of resource inventory used to enrich CUR cost rows.

    Implementations build an enrichment lookup keyed by
    ``(account_id, resource_id)`` — see ``inventory_enricher`` for how the
    lookup is consumed.
    """

    source_type: str = "inventory"

    @abstractmethod
    async def fetch(self) -> dict[tuple[str, str], dict[str, Any]]:
        """Return the enrichment lookup:
        ``{(account_id, resource_id): {enrichment fields}}``."""
        ...

    @abstractmethod
    def get_resource_count(self) -> int:
        """Total number of inventory resources loaded across all sheets."""
        ...

    @abstractmethod
    def supports_auto_sync(self) -> bool:
        """Whether this provider can be refreshed on a schedule. File uploads
        return ``False``."""
        ...

    @abstractmethod
    def get_meta(self) -> DataSourceMeta:
        """Return a current ``DataSourceMeta`` snapshot for this provider."""
        ...
