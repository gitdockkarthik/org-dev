"""Phase 1 concrete data-source providers: file uploads.

* ``FileUploadCURProvider``       — CUR as ``.csv`` / ``.csv.zip`` / ``.parquet``.
* ``FileUploadInventoryProvider`` — resource inventory as ``.xlsx`` (openpyxl,
  read-only / streaming to keep memory bounded for files up to ~10 MB).

Both providers are *manual* (``supports_auto_sync() -> False``). Phase-2
providers (Cost Explorer API, S3 sync, script endpoint) implement the same
interfaces from ``base.py`` and will return ``True``.

The CUR format-conversion logic mirrors the existing, battle-tested
``routes_reports.upload_report`` path so behaviour is identical — the existing
upload route is left untouched.
"""
from __future__ import annotations

import io
import logging
import zipfile
from datetime import datetime, timezone
from typing import Any, Optional

from tools.data_sources.base import (
    CURDataProvider,
    DataSourceMeta,
    InventoryDataProvider,
)

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── CUR file upload ────────────────────────────────────────────────────────────


def cur_bytes_to_csv(filename: str, raw: bytes) -> tuple[str, str]:
    """Convert an uploaded CUR file to raw CSV text.

    Returns ``(csv_text, resolved_filename)``. Supports ``.csv``,
    ``.zip`` (first inner CSV) and ``.parquet`` — matching the existing
    ``/reports/upload`` behaviour. Raises ``ValueError`` on unsupported or
    malformed input.
    """
    fname_lower = (filename or "").lower()

    if fname_lower.endswith(".csv"):
        return raw.decode("utf-8-sig", errors="replace"), filename

    if fname_lower.endswith(".zip"):
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                csv_names = [
                    n for n in zf.namelist()
                    if n.lower().endswith(".csv") and not n.startswith("__MACOSX")
                ]
                if not csv_names:
                    raise ValueError("No CSV file found inside the ZIP archive")
                with zf.open(csv_names[0]) as f:
                    csv_text = f.read().decode("utf-8-sig", errors="replace")
                return csv_text, csv_names[0].split("/")[-1]
        except zipfile.BadZipFile as exc:
            raise ValueError("Invalid ZIP file") from exc

    if fname_lower.endswith(".parquet"):
        import os
        import tempfile

        import duckdb

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp.write(raw)
            tmp_path = tmp.name
        try:
            con = duckdb.connect()
            csv_text = con.execute(
                f"SELECT * FROM read_parquet('{tmp_path}')"
            ).df().to_csv(index=False)
            con.close()
            return csv_text, filename
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Failed to read Parquet file: {exc}") from exc
        finally:
            os.unlink(tmp_path)

    raise ValueError(
        "Unsupported file format. Supported: .csv, .zip (containing CSV), .parquet"
    )


class UploadTooLarge(Exception):
    """Raised by ``stream_upload_to_temp`` when an upload exceeds its byte limit."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        super().__init__(f"Upload exceeds limit of {limit} bytes")


async def stream_upload_to_temp(
    file: Any,
    *,
    suffix: str = "",
    max_bytes: Optional[int] = None,
    chunk_size: int = 8 * 1024 * 1024,
) -> tuple[str, int]:
    """Stream an ``UploadFile`` to a temp file on disk in chunks.

    Returns ``(temp_path, total_bytes)``. Keeps memory bounded (only one
    ``chunk_size`` slice is resident at a time) instead of materialising the
    whole upload via ``await file.read()``. Enforces ``max_bytes`` while
    streaming — on overflow the partial temp file is removed and
    ``UploadTooLarge`` is raised. The caller is responsible for deleting the
    returned path once finished.
    """
    import os
    import tempfile

    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    size = 0
    try:
        with os.fdopen(fd, "wb") as out:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                size += len(chunk)
                if max_bytes is not None and size > max_bytes:
                    raise UploadTooLarge(max_bytes)
                out.write(chunk)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return tmp_path, size


def cur_file_to_csv(filename: str, path: str) -> tuple[str, str]:
    """Disk-based counterpart of :func:`cur_bytes_to_csv`.

    Reads an uploaded CUR file from ``path`` (instead of an in-memory ``bytes``
    blob) and returns ``(csv_text, resolved_filename)``. CSV is read in text
    mode so only the decoded string is held — never the raw bytes alongside it.
    Supports ``.csv``, ``.zip`` (first inner CSV) and ``.parquet``; raises
    ``ValueError`` on unsupported or malformed input.
    """
    fname_lower = (filename or "").lower()

    if fname_lower.endswith(".csv"):
        with open(path, encoding="utf-8-sig", errors="replace") as f:
            return f.read(), filename

    if fname_lower.endswith(".zip"):
        try:
            with zipfile.ZipFile(path) as zf:
                csv_names = [
                    n for n in zf.namelist()
                    if n.lower().endswith(".csv") and not n.startswith("__MACOSX")
                ]
                if not csv_names:
                    raise ValueError("No CSV file found inside the ZIP archive")
                with zf.open(csv_names[0]) as f:
                    csv_text = io.TextIOWrapper(
                        f, encoding="utf-8-sig", errors="replace"
                    ).read()
                return csv_text, csv_names[0].split("/")[-1]
        except zipfile.BadZipFile as exc:
            raise ValueError("Invalid ZIP file") from exc

    if fname_lower.endswith(".parquet"):
        import duckdb

        try:
            con = duckdb.connect()
            csv_text = con.execute(
                f"SELECT * FROM read_parquet('{path}')"
            ).df().to_csv(index=False)
            con.close()
            return csv_text, filename
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Failed to read Parquet file: {exc}") from exc

    raise ValueError(
        "Unsupported file format. Supported: .csv, .zip (containing CSV), .parquet"
    )


class FileUploadCURProvider(CURDataProvider):
    """CUR data sourced from an uploaded file (CSV / CSV.zip / Parquet)."""

    source_type = "file_cur"

    def __init__(
        self,
        *,
        source_id: str,
        filename: str,
        csv_text: str,
        record_count: int = 0,
        total_cost: float = 0.0,
        file_size: int = 0,
        report_id: Optional[int] = None,
        uploaded_at: Optional[str] = None,
        date_range: Optional[tuple[Optional[str], Optional[str]]] = None,
    ) -> None:
        self.source_id = source_id
        self.filename = filename
        self._csv_text = csv_text
        self.record_count = record_count
        self.total_cost = total_cost
        self.file_size = file_size
        self.report_id = report_id
        self.uploaded_at = uploaded_at or _now_iso()
        self._date_range = date_range

    # -- construction ---------------------------------------------------------

    @classmethod
    def from_upload(cls, source_id: str, filename: str, raw: bytes) -> "FileUploadCURProvider":
        """Build a provider by detecting the format and converting to CSV."""
        csv_text, resolved = cur_bytes_to_csv(filename, raw)
        return cls(
            source_id=source_id,
            filename=resolved,
            csv_text=csv_text,
            file_size=len(raw),
        )

    # -- interface ------------------------------------------------------------

    async def fetch(self) -> str:
        return self._csv_text

    def get_date_ranges(self) -> tuple[Optional[str], Optional[str]]:
        if self._date_range is not None:
            return self._date_range
        self._date_range = self._compute_date_range()
        return self._date_range

    def supports_auto_sync(self) -> bool:
        return False

    def get_meta(self) -> DataSourceMeta:
        start, end = self.get_date_ranges()
        return DataSourceMeta(
            source_id=self.source_id,
            source_type=self.source_type,
            label=self.filename,
            last_synced=self.uploaded_at,
            sync_mode="manual",
            date_range_start=start,
            date_range_end=end,
            record_count=self.record_count,
            status="active",
            extra={
                "filename": self.filename,
                "total_cost": self.total_cost,
                "file_size": self.file_size,
                "report_id": self.report_id,
            },
        )

    # -- helpers --------------------------------------------------------------

    def _compute_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """Best-effort min/max of the CUR usage-start date column via DuckDB."""
        try:
            import duckdb
            import pandas as pd

            from tools.duckdb_engine import _detect_date_col

            df = pd.read_csv(io.StringIO(self._csv_text))
            date_col = _detect_date_col(list(df.columns))
            if not date_col:
                return (None, None)
            con = duckdb.connect(database=":memory:")
            try:
                con.register("cur_data", df)
                row = con.execute(
                    f'SELECT MIN(CAST("{date_col}" AS DATE)), '
                    f'MAX(CAST("{date_col}" AS DATE)) FROM cur_data'
                ).fetchone()
            finally:
                con.close()
            start = str(row[0]) if row and row[0] is not None else None
            end = str(row[1]) if row and row[1] is not None else None
            return (start, end)
        except Exception:
            logger.exception("FileUploadCURProvider: failed to compute date range")
            return (None, None)


# ── Inventory file upload ───────────────────────────────────────────────────────

# Per-sheet join key — maps the inventory sheet to the column whose value is the
# resource identifier that should match the CUR ``resource_id``.
SHEET_JOIN_KEYS: dict[str, str] = {
    "EC2": "Instance ID",
    "EBS": "VolumeId",
    "RDS": "DBInstanceIdentifier",
    "S3": "BucketName",
    "Lambda": "FunctionName",
    "ALB": "LoadBalancerName",
    "ELB": "LoadBalancerName",
    "Redis": "ClusterId",
    "DynamoDB": "TableName",
    "EKS": "ClusterName",
}

# Enrichment fields to lift out of each inventory row.
ENRICHMENT_FIELDS: list[str] = [
    "Customer", "Application", "Environment", "Budget_Code", "Grade",
    "Layer", "Function", "ManagedBy", "Project", "LifeCycle",
]

# Candidate column names (normalised) that carry the AWS account id.
_ACCOUNT_HEADER_HINTS = (
    "accountid", "account", "awsaccount", "awsaccountid", "accountnumber",
)


def _norm(s: Any) -> str:
    """Normalise a header/value for tolerant matching: lowercased, no spaces,
    underscores or hyphens."""
    return str(s or "").strip().lower().replace(" ", "").replace("_", "").replace("-", "")


class FileUploadInventoryProvider(InventoryDataProvider):
    """Resource inventory sourced from an uploaded XLSX workbook.

    Each recognised sheet contributes resources keyed by
    ``(account_id, <join-key value>)``. Reading is streamed (``read_only=True``)
    so a ~10 MB workbook stays memory-bounded.
    """

    source_type = "file_inventory"

    def __init__(
        self,
        *,
        source_id: str,
        filename: str,
        lookup: dict[tuple[str, str], dict[str, Any]],
        per_sheet_counts: dict[str, int],
        unmatched_count: int = 0,
        file_size: int = 0,
        uploaded_at: Optional[str] = None,
        stale_threshold_hours: int = 26,
    ) -> None:
        self.source_id = source_id
        self.filename = filename
        self._lookup = lookup
        self.per_sheet_counts = per_sheet_counts
        self.unmatched_count = unmatched_count
        self.file_size = file_size
        self.uploaded_at = uploaded_at or _now_iso()
        self.stale_threshold_hours = stale_threshold_hours

    # -- construction ---------------------------------------------------------

    @classmethod
    def from_upload(cls, source_id: str, filename: str, raw: bytes) -> "FileUploadInventoryProvider":
        lookup, per_sheet, skipped = cls._parse_workbook(raw)
        return cls(
            source_id=source_id,
            filename=filename,
            lookup=lookup,
            per_sheet_counts=per_sheet,
            unmatched_count=skipped,
            file_size=len(raw),
        )

    @staticmethod
    def _parse_workbook(
        raw: bytes,
    ) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, int], int]:
        """Stream the XLSX and build the enrichment lookup.

        Returns ``(lookup, per_sheet_counts, skipped_rows)`` where
        ``skipped_rows`` counts rows missing an account id or join-key value.
        """
        import openpyxl

        lookup: dict[tuple[str, str], dict[str, Any]] = {}
        per_sheet: dict[str, int] = {}
        skipped = 0

        # Normalised lookups for sheet matching.
        norm_join_keys = {_norm(name): (name, key) for name, key in SHEET_JOIN_KEYS.items()}
        norm_enrich = {_norm(f): f for f in ENRICHMENT_FIELDS}

        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        try:
            for ws in wb.worksheets:
                matched = _match_sheet(ws.title, norm_join_keys)
                if matched is None:
                    continue
                canonical_sheet, join_key = matched

                rows = ws.iter_rows(values_only=True)
                try:
                    header = next(rows)
                except StopIteration:
                    continue
                if not header:
                    continue

                # Map normalised header -> column index.
                col_idx: dict[str, int] = {}
                for i, h in enumerate(header):
                    if h is None:
                        continue
                    col_idx[_norm(h)] = i

                account_col = _find_account_col(col_idx)
                join_col = col_idx.get(_norm(join_key))
                if join_col is None:
                    # Sheet recognised but missing its join column — skip whole sheet.
                    logger.warning(
                        "Inventory sheet %r missing join column %r — skipped",
                        ws.title, join_key,
                    )
                    continue

                # Pre-resolve enrichment field column indexes.
                enrich_cols: dict[str, int] = {}
                for norm_h, idx in col_idx.items():
                    if norm_h in norm_enrich:
                        enrich_cols[norm_enrich[norm_h]] = idx

                count = 0
                for row in rows:
                    if row is None:
                        continue
                    resource_val = _cell(row, join_col)
                    account_val = _cell(row, account_col) if account_col is not None else ""
                    if not resource_val or not account_val:
                        skipped += 1
                        continue
                    entry: dict[str, Any] = {
                        "_sheet": canonical_sheet,
                        "_join_key": join_key,
                    }
                    for field_name, idx in enrich_cols.items():
                        val = _cell(row, idx)
                        if val:
                            entry[field_name] = val
                    lookup[(str(account_val), str(resource_val))] = entry
                    count += 1

                per_sheet[canonical_sheet] = per_sheet.get(canonical_sheet, 0) + count
        finally:
            wb.close()

        return lookup, per_sheet, skipped

    # -- interface ------------------------------------------------------------

    async def fetch(self) -> dict[tuple[str, str], dict[str, Any]]:
        return self._lookup

    def get_resource_count(self) -> int:
        return len(self._lookup)

    def supports_auto_sync(self) -> bool:
        return False

    def get_meta(self) -> DataSourceMeta:
        return DataSourceMeta(
            source_id=self.source_id,
            source_type=self.source_type,
            label=self.filename,
            last_synced=self.uploaded_at,
            sync_mode="manual",
            record_count=self.get_resource_count(),
            status="active",
            stale_threshold_hours=self.stale_threshold_hours,
            extra={
                "filename": self.filename,
                "file_size": self.file_size,
                "per_sheet_counts": self.per_sheet_counts,
                "unmatched_count": self.unmatched_count,
            },
        )


def _match_sheet(
    title: str, norm_join_keys: dict[str, tuple[str, str]]
) -> Optional[tuple[str, str]]:
    """Match a worksheet title to a canonical sheet + join key, tolerantly."""
    nt = _norm(title)
    if nt in norm_join_keys:
        return norm_join_keys[nt]
    # Substring match (e.g. "EC2 Instances" -> "EC2").
    for norm_name, value in norm_join_keys.items():
        if norm_name and (norm_name in nt or nt in norm_name):
            return value
    return None


def _find_account_col(col_idx: dict[str, int]) -> Optional[int]:
    for norm_h, idx in col_idx.items():
        if any(hint in norm_h for hint in _ACCOUNT_HEADER_HINTS):
            return idx
    return None


def _cell(row: tuple, idx: Optional[int]) -> str:
    if idx is None or idx >= len(row):
        return ""
    val = row[idx]
    if val is None:
        return ""
    return str(val).strip()
