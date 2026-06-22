"""Pluggable Data Source abstraction for the CUR Analyser.

Phase 1 (current): file-upload providers for CUR (CSV / CSV.zip / Parquet) and
Inventory (XLSX).

Phase 2 (future): AWS Cost Explorer API, S3 sync, and script-endpoint providers
plug into the same `CURDataProvider` / `InventoryDataProvider` interfaces with no
changes required to consumers (registry, enricher, routes).

Nothing here has side effects on import — providers are instantiated explicitly
by the registry and routes layer.
"""

from tools.data_sources.base import (
    CURDataProvider,
    DataSourceMeta,
    InventoryDataProvider,
    SourceStatus,
    SyncMode,
)

__all__ = [
    "CURDataProvider",
    "InventoryDataProvider",
    "DataSourceMeta",
    "SyncMode",
    "SourceStatus",
]
