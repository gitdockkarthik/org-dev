"""Data-source registry — the single place that tracks which CUR and inventory
sources exist, which are active, and persists that state durably.

Persistence model (all via the existing key→value storage layer, new keys only):

* ``data_source_registry`` — JSON blob: CUR source metadata, ``active_cur_ids``,
  current inventory metadata, and inventory archive metadata.
* ``cur_files``            — JSON list mirroring the CUR source metadata (kept in
  sync for an at-a-glance listing / future tooling).
* ``inventory_file``       — base64 of the current inventory XLSX.
* ``inventory_file_archive_<source_id>`` — base64 of an archived inventory XLSX
  (the last 5 are retained).

CUR *content* is not duplicated here — it continues to live in the ``cur_report``
table via ``report_store``; the registry references it by ``report_id`` and
rehydrates providers on demand.

The registry is provider-agnostic: it stores ``DataSourceMeta`` and rebuilds the
appropriate provider. Phase-2 providers (Cost Explorer, S3, script) slot in by
extending ``_rehydrate_cur`` / adding their own ``register_*`` helpers.
"""
from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from tools.data_sources.base import (
    CURDataProvider,
    DataSourceMeta,
    InventoryDataProvider,
)
from tools.data_sources.file_providers import (
    FileUploadCURProvider,
    FileUploadInventoryProvider,
)

logger = logging.getLogger(__name__)

REGISTRY_KEY = "data_source_registry"
CUR_FILES_KEY = "cur_files"
INVENTORY_FILE_KEY = "inventory_file"
INVENTORY_ARCHIVE_PREFIX = "inventory_file_archive_"
MAX_INVENTORY_ARCHIVES = 5


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


class DataSourceRegistry:
    """In-memory state with durable write-through to the storage backend."""

    def __init__(self, storage) -> None:
        self._storage = storage
        # Persisted metadata.
        self._cur_meta: list[DataSourceMeta] = []
        self._active_cur_ids: list[str] = []
        self._inventory_meta: Optional[DataSourceMeta] = None
        self._inventory_archives: list[DataSourceMeta] = []
        # Live provider caches (rebuilt lazily).
        self._inventory_provider: Optional[InventoryDataProvider] = None

    # ── persistence ──────────────────────────────────────────────────────────

    async def load(self) -> None:
        """Hydrate registry state from storage. Never raises."""
        try:
            blob = await self._storage.get(REGISTRY_KEY)
        except Exception:
            logger.exception("DataSourceRegistry.load: storage.get failed")
            blob = None
        if not blob:
            logger.info("DataSourceRegistry.load: no saved state — starting empty")
            return
        try:
            state = json.loads(blob)
            self._cur_meta = [DataSourceMeta.from_dict(d) for d in state.get("cur_sources", [])]
            self._active_cur_ids = list(state.get("active_cur_ids", []))
            inv = state.get("inventory")
            self._inventory_meta = DataSourceMeta.from_dict(inv) if inv else None
            self._inventory_archives = [
                DataSourceMeta.from_dict(d) for d in state.get("inventory_archives", [])
            ]
            logger.info(
                "DataSourceRegistry.load: %d CUR source(s), inventory=%s, %d archive(s)",
                len(self._cur_meta),
                bool(self._inventory_meta),
                len(self._inventory_archives),
            )
        except Exception:
            logger.exception("DataSourceRegistry.load: failed to parse state")

    async def save(self) -> None:
        """Persist registry metadata. Never raises."""
        state = {
            "cur_sources": [m.to_dict() for m in self._cur_meta],
            "active_cur_ids": self._active_cur_ids,
            "inventory": self._inventory_meta.to_dict() if self._inventory_meta else None,
            "inventory_archives": [m.to_dict() for m in self._inventory_archives],
        }
        try:
            await self._storage.set(REGISTRY_KEY, json.dumps(state))
            await self._storage.set(
                CUR_FILES_KEY, json.dumps([m.to_dict() for m in self._cur_meta])
            )
        except Exception:
            logger.exception("DataSourceRegistry.save: storage.set failed")

    # ── CUR sources ────────────────────────────────────────────────────────────

    async def register_cur(self, provider: CURDataProvider, *, make_active: bool = True) -> DataSourceMeta:
        """Register a new CUR source. By default it becomes the sole active source."""
        meta = provider.get_meta()
        # Replace any existing entry with the same id.
        self._cur_meta = [m for m in self._cur_meta if m.source_id != meta.source_id]
        self._cur_meta.insert(0, meta)
        if make_active:
            self._active_cur_ids = [meta.source_id]
        elif meta.source_id not in self._active_cur_ids:
            self._active_cur_ids.append(meta.source_id)
        await self.save()
        return meta

    def get_active_cur(self) -> list[DataSourceMeta]:
        """Return active CUR source metadata (single, or multiple in comparison mode)."""
        if self._active_cur_ids:
            by_id = {m.source_id: m for m in self._cur_meta}
            active = [by_id[i] for i in self._active_cur_ids if i in by_id]
            if active:
                return active
        # Fallback: most-recent source if no explicit active selection survived.
        return self._cur_meta[:1]

    def list_cur(self) -> list[DataSourceMeta]:
        return list(self._cur_meta)

    async def set_active_cur(self, source_ids: list[str]) -> None:
        valid = {m.source_id for m in self._cur_meta}
        self._active_cur_ids = [s for s in source_ids if s in valid]
        await self.save()

    async def get_active_cur_providers(self) -> list[CURDataProvider]:
        """Rehydrate live providers for the active CUR sources."""
        providers: list[CURDataProvider] = []
        for meta in self.get_active_cur():
            prov = self._rehydrate_cur(meta)
            if prov is not None:
                providers.append(prov)
        return providers

    async def delete_cur(self, source_id: str) -> bool:
        before = len(self._cur_meta)
        was_active = source_id in self._active_cur_ids
        self._cur_meta = [m for m in self._cur_meta if m.source_id != source_id]
        self._active_cur_ids = [i for i in self._active_cur_ids if i != source_id]
        removed = len(self._cur_meta) != before
        # If we deleted the (only) active source, promote the next available file
        # so the dashboard always has an active CUR to work with.
        if removed and was_active and not self._active_cur_ids and self._cur_meta:
            self._active_cur_ids = [self._cur_meta[0].source_id]
        if removed:
            await self.save()
        return removed

    def _rehydrate_cur(self, meta: DataSourceMeta) -> Optional[CURDataProvider]:
        """Rebuild a CUR provider from persisted metadata.

        Phase 1: file uploads — CSV content lives in ``report_store``.
        """
        if meta.source_type == "file_cur":
            report_id = meta.extra.get("report_id")
            csv_text: Optional[str] = None
            if report_id is not None:
                try:
                    from report_store import get_report_csv
                    csv_text = get_report_csv(int(report_id))
                except Exception:
                    logger.exception("registry: failed to read report %s", report_id)
            if csv_text is None:
                logger.warning(
                    "registry: CUR source %s has no recoverable CSV (report_id=%s)",
                    meta.source_id, report_id,
                )
                return None
            return FileUploadCURProvider(
                source_id=meta.source_id,
                filename=meta.label,
                csv_text=csv_text,
                record_count=meta.record_count,
                total_cost=meta.extra.get("total_cost", 0.0),
                file_size=meta.extra.get("file_size", 0),
                report_id=report_id,
                uploaded_at=meta.last_synced,
                date_range=(meta.date_range_start, meta.date_range_end),
            )
        logger.warning("registry: unknown CUR source_type %r — cannot rehydrate", meta.source_type)
        return None

    # ── Inventory source ─────────────────────────────────────────────────────────

    async def set_inventory(
        self, provider: FileUploadInventoryProvider, raw_bytes: bytes
    ) -> DataSourceMeta:
        """Replace the active inventory, archiving the previous one (keep last 5)."""
        # Archive current inventory (metadata + bytes) before replacing.
        if self._inventory_meta is not None:
            old = self._inventory_meta
            try:
                cur_b64 = await self._storage.get(INVENTORY_FILE_KEY)
                if cur_b64:
                    await self._storage.set(
                        INVENTORY_ARCHIVE_PREFIX + old.source_id, cur_b64
                    )
                    self._inventory_archives.insert(0, old)
            except Exception:
                logger.exception("registry: failed to archive previous inventory")
            await self._trim_archives()

        meta = provider.get_meta()
        self._inventory_meta = meta
        self._inventory_provider = provider
        try:
            await self._storage.set(
                INVENTORY_FILE_KEY, base64.b64encode(raw_bytes).decode("ascii")
            )
        except Exception:
            logger.exception("registry: failed to persist inventory file")
        await self.save()
        return meta

    def get_inventory(self) -> Optional[DataSourceMeta]:
        """Current inventory metadata, or None if no inventory is loaded.

        ``None`` is the explicit signal that enrichment must be skipped.
        """
        return self._inventory_meta

    async def get_inventory_provider(self) -> Optional[InventoryDataProvider]:
        """Return the live inventory provider, rebuilding it from stored bytes
        if it isn't cached in memory (e.g. after a restart)."""
        if self._inventory_provider is not None:
            return self._inventory_provider
        if self._inventory_meta is None:
            return None
        try:
            b64 = await self._storage.get(INVENTORY_FILE_KEY)
        except Exception:
            logger.exception("registry: failed to read inventory file from storage")
            return None
        if not b64:
            logger.warning("registry: inventory metadata present but file bytes missing")
            return None
        try:
            raw = base64.b64decode(b64)
            self._inventory_provider = FileUploadInventoryProvider.from_upload(
                self._inventory_meta.source_id, self._inventory_meta.label, raw
            )
            return self._inventory_provider
        except Exception:
            logger.exception("registry: failed to rebuild inventory provider")
            return None

    async def delete_inventory(self) -> bool:
        if self._inventory_meta is None:
            return False
        sid = self._inventory_meta.source_id
        self._inventory_meta = None
        self._inventory_provider = None
        try:
            await self._storage.delete(INVENTORY_FILE_KEY)
        except Exception:
            logger.exception("registry: failed to delete inventory file")
        await self.save()
        logger.info("registry: deleted inventory source %s", sid)
        return True

    def is_inventory_stale(self) -> bool:
        """True when the inventory's last sync is older than its stale threshold."""
        if self._inventory_meta is None:
            return False
        last = _parse_iso(self._inventory_meta.last_synced)
        if last is None:
            return False
        age_hours = (_now() - last).total_seconds() / 3600.0
        return age_hours > self._inventory_meta.stale_threshold_hours

    async def _trim_archives(self) -> None:
        """Keep only the most recent MAX_INVENTORY_ARCHIVES archives."""
        while len(self._inventory_archives) > MAX_INVENTORY_ARCHIVES:
            dropped = self._inventory_archives.pop()
            try:
                await self._storage.delete(INVENTORY_ARCHIVE_PREFIX + dropped.source_id)
            except Exception:
                logger.exception("registry: failed to delete archived inventory %s", dropped.source_id)

    # ── Auto-sync ────────────────────────────────────────────────────────────────

    async def refresh_stale(self) -> list[str]:
        """Refresh sources that support auto-sync (Phase 2). Phase-1 file uploads
        are manual-only, so this is a no-op for them.

        Returns the list of source_ids that were refreshed.
        """
        refreshed: list[str] = []
        for prov in await self.get_active_cur_providers():
            if prov.supports_auto_sync():
                try:
                    await prov.fetch()
                    refreshed.append(prov.get_meta().source_id)
                except Exception:
                    logger.exception("registry.refresh_stale: CUR fetch failed")
        inv = await self.get_inventory_provider()
        if inv is not None and inv.supports_auto_sync():
            try:
                await inv.fetch()
                refreshed.append(inv.get_meta().source_id)
            except Exception:
                logger.exception("registry.refresh_stale: inventory fetch failed")
        return refreshed

    def has_auto_sync_sources(self) -> bool:
        """Whether any registered source supports auto-sync — used to decide
        whether a background scheduler needs to be instantiated at all."""
        # Phase 1: file uploads never auto-sync. Check metadata sync_mode for
        # forward compatibility with Phase-2 providers.
        if any(m.sync_mode in ("scheduled", "realtime") for m in self._cur_meta):
            return True
        if self._inventory_meta and self._inventory_meta.sync_mode in ("scheduled", "realtime"):
            return True
        return False

    # ── Status snapshot ────────────────────────────────────────────────────────

    def status(self) -> dict[str, Any]:
        """A JSON-serialisable snapshot for the ``/data-sources/status`` endpoint."""
        active_ids = {m.source_id for m in self.get_active_cur()}
        return {
            "cur_sources": [
                {**m.to_dict(), "active": m.source_id in active_ids}
                for m in self._cur_meta
            ],
            "active_cur_ids": self._active_cur_ids,
            "inventory": self._inventory_meta.to_dict() if self._inventory_meta else None,
            "inventory_loaded": self._inventory_meta is not None,
            "inventory_stale": self.is_inventory_stale(),
            "inventory_archives": [m.to_dict() for m in self._inventory_archives],
            "has_auto_sync_sources": self.has_auto_sync_sources(),
        }


# ── Module-level singleton ───────────────────────────────────────────────────

_registry: Optional[DataSourceRegistry] = None


async def init_registry(storage) -> DataSourceRegistry:
    """Build (or rebuild) the singleton registry and load persisted state."""
    global _registry
    _registry = DataSourceRegistry(storage)
    await _registry.load()
    return _registry


def get_registry() -> Optional[DataSourceRegistry]:
    """Return the registry singleton, or None if not initialised."""
    return _registry
