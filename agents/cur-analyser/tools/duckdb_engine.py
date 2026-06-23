"""DuckDB-backed CUR query engine.

All public query functions accept raw CSV text and return plain Python structures.
The CurQueryTool wraps them as an Anthropic-callable ToolExecutor that reads from
the per-session CUR cache populated by main.py.
"""
from __future__ import annotations

import io
import json
import logging
import re
import threading
import time
from typing import Any, ClassVar, Literal

import duckdb
import pandas as pd

from tools.base import ToolExecutor

logger = logging.getLogger(__name__)

# ── Column detection helpers ──────────────────────────────────────────────────

QueryType = Literal["total_cost", "cost_by_service", "daily_trend", "cost_by_region"]


# Ordered column-name candidates per logical column. Real AWS CUR 2.0 uses the
# slash format (lineItem/..., product/...); normalised exports use underscores;
# legacy CUR uses bare CamelCase names; synthetic/test data uses bare snake_case.
# Tried in order, case-insensitively.
COST_COL_CANDIDATES = [
    "lineItem/UnblendedCost",   # real CUR 2.0
    "line_item_unblended_cost", # normalised
    "UnBlendedCost",            # legacy CUR
    "BlendedCost",              # fallback
    "unblended_cost",           # synthetic
]
SERVICE_COL_CANDIDATES = [
    "lineItem/ProductCode",     # real CUR 2.0
    "line_item_product_code",   # normalised
    "ProductName",              # legacy CUR
    "product/ProductName",      # real CUR 2.0 product name
    "product_name",             # synthetic
]
DATE_COL_CANDIDATES = [
    "lineItem/UsageStartDate",    # real CUR 2.0
    "line_item_usage_start_date", # normalised
    "UsageStartDate",             # legacy CUR
    "usage_start_date",           # synthetic
]
REGION_COL_CANDIDATES = [
    "product/region",   # real CUR 2.0
    "product_region",   # normalised
    "AvailabilityZone", # legacy CUR fallback (AZ not region but closest)
    "region",           # synthetic
]
ACCOUNT_COL_CANDIDATES = [
    "lineItem/UsageAccountId",      # real AWS CUR (current format)
    "line_item_usage_account_id",   # normalised format
    "LinkedAccountId",              # legacy CUR (per-resource / usage account)
    "account_id",                   # synthetic / test data
    "PayerAccountId",               # legacy CUR payer (master) — lowest priority
]
RESOURCE_COL_CANDIDATES = [
    "lineItem/ResourceId",          # real AWS CUR (current format)
    "line_item_resource_id",        # normalised format
    "ResourceId",                   # legacy CUR (no lineItem/ prefix)
    "resource_id",                  # synthetic / test data
]


def _match_candidate(columns: list[str], candidates: list[str]) -> str | None:
    """Return the actual column matching the first candidate present
    (case-insensitive), or ``None`` when no candidate matches."""
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def resolve_col(df, candidates: list[str], required: bool = True) -> str | None:
    """Central column resolver. Tries each candidate against ``df``'s columns
    case-insensitively, in order.

    When ``required`` is True and no candidate matches, raises ``ValueError``
    naming what was tried versus the columns that actually exist (to make a
    misconfigured / unexpected CUR format easy to diagnose). When ``required``
    is False, returns ``None`` instead.
    """
    col = _match_candidate(list(df.columns), candidates)
    if col is None and required:
        raise ValueError(
            f"Required column not found. Tried candidates {candidates}; "
            f"available CUR columns: {list(df.columns)}"
        )
    return col


def _detect_cost_col(columns: list[str]) -> str | None:
    """Prefer the unblended-cost candidates; fall back to any cost column."""
    col = _match_candidate(columns, COST_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "unblended" in c.lower() and "cost" in c.lower():
            return c
    for c in columns:
        if "cost" in c.lower():
            return c
    return None


def _detect_service_col(columns: list[str]) -> str | None:
    """Prefer the product/service candidates; accept productname / servicename / service."""
    col = _match_candidate(columns, SERVICE_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        cl = c.lower()
        if any(k in cl for k in ("productname", "product_code", "servicename")) or cl == "service":
            return c
    return None


def _detect_date_col(columns: list[str]) -> str | None:
    """Detect the usage start-date column across slash / underscore / legacy /
    bare formats, then fall back to any usage-date-like column."""
    col = _match_candidate(columns, DATE_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "usagestart" in c.lower():
            return c
    for c in columns:
        if "usagedate" in c.lower():
            return c
    return None


def _detect_region_col(columns: list[str]) -> str | None:
    """Detect the region column; legacy CUR has no region so AvailabilityZone is
    used as the closest fallback (handled via the candidate list)."""
    col = _match_candidate(columns, REGION_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "region" in c.lower():
            return c
    return None


def _detect_account_col(columns: list[str]) -> str | None:
    """Detect the usage-account-id column across formats. Tries the explicit
    candidates in order, then falls back to any column containing ``account``."""
    col = _match_candidate(columns, ACCOUNT_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "account" in c.lower():
            return c
    return None


def _detect_resource_col(columns: list[str]) -> str | None:
    """Detect the resource-id column across formats by trying the explicit
    candidates in order, case-insensitively."""
    return _match_candidate(columns, RESOURCE_COL_CANDIDATES)


# ── Tag column detection ──────────────────────────────────────────────────────
# Real AWS CUR exposes user tags as ``resourceTags/user:<Name>`` columns, while
# synthetic / normalised data uses a ``tag_<Name>`` prefix. Streamed rows are
# normalised by report_store._normalise_key, which rewrites the slash form to
# ``resource_tags_user_<Name>`` — all variants are listed so detection works on
# raw CSV columns and normalised rows alike. Matched case-insensitively.
TAG_COL_PREFIXES = [
    "tag_",
    "resourceTags/user:",
    "resource_tags_user_",
    "resource_tags_user:",
]

# Display-name aliases so a requested tag resolves across naming variants
# (e.g. synthetic "CostCentre" vs real CUR "CostCenter", or synthetic
# "Product" vs real CUR "Project" — same logical "application" dimension).
_TAG_DISPLAY_ALIASES = {
    "costcentre": {"costcentre", "costcenter"},
    "costcenter": {"costcentre", "costcenter"},
    "product": {"product", "project"},
    "project": {"product", "project"},
}


def detect_tag_columns(df) -> dict[str, str]:
    """Returns ``{display_name: actual_column_name}`` for every tag column,
    across the ``tag_`` / ``resourceTags/user:`` / ``resource_tags_user_`` /
    ``resource_tags_user:`` prefixes (matched case-insensitively)."""
    found: dict[str, str] = {}
    for col in df.columns:
        cl = str(col).lower()
        for prefix in TAG_COL_PREFIXES:
            if cl.startswith(prefix.lower()):
                display = str(col)[len(prefix):]  # strip prefix for display
                if display:
                    found[display] = col
                break
    return found


def _resolve_tag_col(df, requested: str) -> str | None:
    """Resolve a requested tag column (e.g. ``tag_Environment``) to the actual
    column present, tolerating prefix and display-name variants (incl. the
    CostCentre/CostCenter alias). Returns ``None`` when the tag is absent."""
    if requested in df.columns:
        return requested  # exact native match (synthetic / normalised)
    display = requested
    for p in TAG_COL_PREFIXES:
        if requested.startswith(p):
            display = requested[len(p):]
            break
    target = display.lower()
    targets = _TAG_DISPLAY_ALIASES.get(target, {target})
    for disp, actual in detect_tag_columns(df).items():
        if disp.lower() in targets:
            return actual
    return None


# ── Service categorisation & region helpers ───────────────────────────────────
# Keyword rules for CUR formats without a precomputed service_category column
# (e.g. legacy CUR). Matched case-insensitively against the service name; first
# matching group wins. Handles both short ("Amazon EC2") and legacy long
# ("Amazon Elastic Compute Cloud") names.
_SERVICE_CATEGORY_RULES: list[tuple[tuple[str, ...], str]] = [
    (("elastic compute", "ec2", "lambda", "elastic container", "ecs", "fargate",
      "batch", "lightsail", "elastic kubernetes", "eks", "app runner"), "Compute"),
    (("relational database", "rds", "aurora", "dynamodb", "elasticache",
      "redshift", "neptune", "documentdb", "memorydb", "timestream"), "Database"),
    (("simple storage", "s3", "elastic block", "ebs", "glacier", "elastic file",
      "efs", "fsx", "storage gateway", "backup", "container registry", "ecr"), "Storage"),
    (("cloudfront", "route 53", "route53", "virtual private cloud", "vpc",
      "elastic load", "load balancing", "elb", "api gateway", "direct connect",
      "global accelerator", "transit gateway", "simple notification", "sns",
      "simple queue", "sqs"), "Network"),
    (("waf", "secrets manager", "key management", "kms", "guardduty", "shield",
      "identity and access", "cognito", "certificate manager", "acm", "inspector",
      "security hub", "macie"), "Security"),
    (("cloudwatch", "cloudtrail", "x-ray", "xray", "config", "systems manager",
      "grafana", "prometheus"), "Observability"),
    (("kinesis", "msk", "managed streaming", "kafka"), "Streaming"),
    (("opensearch", "elasticsearch", "athena", "glue", "emr", "quicksight",
      "lake formation", "data pipeline"), "Analytics"),
]


def _categorise_service(service: str) -> str:
    """Map an AWS service name to a coarse category, tolerant of both short and
    legacy long names. Returns ``"Other"`` when nothing matches."""
    s = (service or "").lower()
    for keywords, category in _SERVICE_CATEGORY_RULES:
        if any(k in s for k in keywords):
            return category
    return "Other"


def _region_from_az(value: str) -> str:
    """Reduce an availability-zone id to its region prefix
    (``us-east-1a`` -> ``us-east-1``); returns the value unchanged when it is
    not an AZ (e.g. an actual region)."""
    v = str(value or "")
    m = re.match(r"^([a-z]{2}-[a-z]+-\d+)[a-z]$", v)
    return m.group(1) if m else v


def _service_category_from_names(con, cost_col: str, svc_col: str | None) -> list[dict]:
    """Build the service-category breakdown by mapping service names to
    categories — for CUR formats lacking a precomputed service_category column.
    Shape matches get_cost_by_service_category (owner fields blank)."""
    if not svc_col:
        return []
    total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
    rows = con.execute(
        f'SELECT "{svc_col}", SUM("{cost_col}") AS cost FROM cur_data GROUP BY "{svc_col}"'
    ).fetchall()
    cat_totals: dict[str, float] = {}
    for r in rows:
        cat = _categorise_service(str(r[0] or ""))
        cat_totals[cat] = cat_totals.get(cat, 0.0) + float(r[1] or 0)
    return [
        {
            "category": cat,
            "owner_team": "",
            "owner_email": "",
            "cost": round(c, 4),
            "pct_of_total": round(c / total * 100, 2) if total else 0.0,
        }
        for cat, c in sorted(cat_totals.items(), key=lambda kv: kv[1], reverse=True)
    ]


# ── Core query functions (migrated from engine.py) ────────────────────────────

def _apply_filters(df: pd.DataFrame, filters: dict | None) -> pd.DataFrame:
    """Apply dashboard filters to the loaded frame *before* any aggregation, so
    every downstream query (which reads the registered ``cur_data``) sees the
    filtered subset. Columns are resolved per-format via the same detectors used
    everywhere else, so filtering works across CUR 2.0 / legacy / normalised /
    synthetic exports. Unknown / missing columns are skipped (never error).

    Supported keys: ``date_from``, ``date_to`` (ISO dates), and the list filters
    ``accounts``, ``environments``, ``services``, ``regions``, ``pricing_terms``,
    ``tag_products``, ``tag_teams`` — matching the dashboard filter dropdowns.
    """
    if not filters:
        return df
    cols = list(df.columns)
    mask = pd.Series(True, index=df.index)

    def _isin(col: str, values, include_untagged: bool = False):
        s = df[col].astype(str)
        m = s.isin(set(values))
        if include_untagged:
            blank = df[col].isna() | s.str.strip().isin(["", "nan", "NaN", "None"])
            m = m | blank
        return m

    # ── Date range (server-side; replaces the client-side fDate filter) ──
    date_col = _detect_date_col(cols)
    if date_col and (filters.get("date_from") or filters.get("date_to")):
        dates = pd.to_datetime(df[date_col], errors="coerce")
        if filters.get("date_from"):
            mask &= dates >= pd.to_datetime(filters["date_from"])
        if filters.get("date_to"):
            # Inclusive end date — cover the whole day.
            mask &= dates < pd.to_datetime(filters["date_to"]) + pd.Timedelta(days=1)

    # ── Accounts (dropdown value is the account NAME when present, else the id) ──
    if filters.get("accounts"):
        vals = set(filters["accounts"])
        acct_id = _detect_account_col(cols)
        name_col = "line_item_usage_account_name" if "line_item_usage_account_name" in cols else None
        am = pd.Series(False, index=df.index)
        if acct_id:
            am = am | df[acct_id].astype(str).isin(vals)
        if name_col:
            am = am | df[name_col].astype(str).isin(vals)
        if acct_id or name_col:
            mask &= am

    # ── Services ──
    if filters.get("services"):
        svc = _detect_service_col(cols)
        if svc:
            mask &= df[svc].astype(str).isin(set(filters["services"]))

    # ── Environments (native tag_Environment; "Untagged" → blank/NULL) ──
    if filters.get("environments"):
        env = _resolve_tag_col(df, "tag_Environment")
        if env:
            vals = set(filters["environments"])
            mask &= _isin(env, vals, include_untagged=("Untagged" in vals))

    # ── Regions (fold AZ → region prefix to match the breakdown values) ──
    if filters.get("regions"):
        reg = _detect_region_col(cols)
        if reg:
            folded = df[reg].astype(str).map(_region_from_az)
            mask &= folded.isin(set(filters["regions"]))

    # ── Pricing terms ──
    if filters.get("pricing_terms"):
        if "pricing_term" in cols:
            mask &= df["pricing_term"].astype(str).isin(set(filters["pricing_terms"]))

    # ── Tag: Product / Team ("Untagged" → blank/NULL) ──
    if filters.get("tag_products"):
        col = _resolve_tag_col(df, "tag_Product")
        if col:
            vals = set(filters["tag_products"])
            mask &= _isin(col, vals, include_untagged=("Untagged" in vals))
    if filters.get("tag_teams"):
        col = _resolve_tag_col(df, "tag_Team")
        if col:
            vals = set(filters["tag_teams"])
            mask &= _isin(col, vals, include_untagged=("Untagged" in vals))

    return df[mask]


# ── Loaded-DataFrame cache ─────────────────────────────────────────────────────
# A dashboard build calls ~14 query functions, each of which loads the CUR via
# _load_df. Without a cache that re-reads (and re-parses) the file ~14× per
# request — ~10s+ for a large CUR — and every filter change pays it again. We
# cache the raw loaded DataFrame keyed by file_path so the disk read happens once
# (first request), then all queries — filtered or not — operate in memory.
#
# Sharing the cached frame by reference is safe because the only in-place mutator
# is the inventory enricher (it adds inv_* columns); filtering (``df[mask]``) and
# DuckDB registration are non-mutating. So we copy only on the enricher path.
_df_cache: dict[str, tuple[pd.DataFrame, float]] = {}
_DF_CACHE_TTL_SECS = 600  # 10 minutes

# Per-file load lock. When the dashboard runs its queries in parallel and the
# cache is cold, every worker would otherwise read the same (multi-GB) file at
# once — N concurrent reads = N× peak memory. The lock makes the first worker
# read+cache while the rest wait and then hit the cache.
_df_load_locks: dict[str, threading.Lock] = {}
_df_load_locks_guard = threading.Lock()


def _df_load_lock(file_path: str) -> threading.Lock:
    with _df_load_locks_guard:
        lock = _df_load_locks.get(file_path)
        if lock is None:
            lock = threading.Lock()
            _df_load_locks[file_path] = lock
        return lock


def _get_cached_df(file_path: str) -> pd.DataFrame | None:
    entry = _df_cache.get(file_path)
    if entry is not None:
        df, ts = entry
        if time.time() - ts < _DF_CACHE_TTL_SECS:
            logger.info(
                "[df-cache] HIT key=%r (cache size=%d, keys=%s)",
                file_path, len(_df_cache), list(_df_cache.keys()),
            )
            return df
        _df_cache.pop(file_path, None)
        logger.info(
            "[df-cache] EXPIRED key=%r (cache size=%d)", file_path, len(_df_cache),
        )
        return None
    logger.info(
        "[df-cache] MISS key=%r (cache size=%d, keys=%s)",
        file_path, len(_df_cache), list(_df_cache.keys()),
    )
    return None


def _cache_df(file_path: str, df: pd.DataFrame) -> None:
    _df_cache[file_path] = (df, time.time())
    logger.info(
        "[df-cache] STORED key=%r shape=%s (cache size=%d)",
        file_path, getattr(df, "shape", None), len(_df_cache),
    )


def invalidate_df_cache(file_path: str | None = None) -> None:
    """Drop a cached DataFrame (or all of them when ``file_path`` is None)."""
    if file_path is None:
        _df_cache.clear()
    else:
        _df_cache.pop(file_path, None)


def _load_df(
    csv_text: str | None = None, enricher=None, file_path: str | None = None,
    filters: dict | None = None,
) -> tuple[pd.DataFrame, duckdb.DuckDBPyConnection]:
    """Load CUR data into a DataFrame + DuckDB connection.

    The source is either ``csv_text`` (the legacy in-memory pipeline) or
    ``file_path`` (the file-path pipeline used for large CUR files, which reads
    straight from disk instead of materialising a multi-GB CSV string). A
    ``.parquet`` path is read via DuckDB; any other path is read with
    ``pd.read_csv`` directly — no ``StringIO`` intermediate.

    File-path reads are cached (see ``_df_cache``): the first request reads from
    disk and caches the frame; subsequent requests (filtered or not) reuse it in
    memory and just apply filters, which is what makes filtered dashboards fast.

    ``enricher`` is optional. When supplied, the inventory enricher adds
    ``inv_*`` virtual columns *before* the frame is registered. An inactive
    enricher (no inventory loaded) is a safe no-op.

    ``filters`` (optional) is applied to the frame before it is registered, so
    every aggregation that reads ``cur_data`` operates on the filtered subset.
    """
    con = duckdb.connect(database=":memory:")
    logger.info(
        "[df-cache] _load_df source=%s file_path=%r csv_text=%s enricher=%s filters=%s",
        "file_path" if file_path is not None else "csv_text",
        file_path,
        (f"{len(csv_text)} chars" if csv_text is not None else None),
        (enricher is not None),
        (sorted(filters.keys()) if filters else None),
    )
    if file_path is not None:
        df = _get_cached_df(file_path)
        if df is None:
            # Serialise the disk read per file so parallel queries on a cold
            # cache don't all read the (potentially multi-GB) file at once.
            with _df_load_lock(file_path):
                df = _get_cached_df(file_path)  # re-check: another worker may have loaded it
                if df is None:
                    if str(file_path).lower().endswith(".parquet"):
                        df = con.execute("SELECT * FROM read_parquet(?)", [file_path]).df()
                    else:
                        df = pd.read_csv(file_path)
                    _cache_df(file_path, df)
        # ``df`` may be the shared cached frame. The enricher mutates in place
        # (adds inv_* columns), so copy first to keep the cache pristine. Filters
        # and registration below never mutate, so they're safe on the shared frame.
        if enricher is not None:
            df = df.copy()
    else:
        df = pd.read_csv(io.StringIO(csv_text))
    if enricher is not None:
        df = enricher.enrich_dataframe(df)
    if filters:
        df = _apply_filters(df, filters)
    con.register("cur_data", df)
    return df, con


def get_total_cost(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> dict:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        # DEBUG: log the raw CUR column names once per dashboard build so the
        # actual header format (and any unexpected tag-column naming) is visible.
        logger.info("get_total_cost: raw CUR columns = %s", list(df.columns))
        cost_col = _detect_cost_col(list(df.columns))
        if not cost_col:
            return {"error": "No cost column found in CUR data."}
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        return {
            "total_cost": round(total, 4),
            "row_count": len(df),
            "cost_column": cost_col,
        }
    finally:
        con.close()


def get_cost_by_service(csv_text: str | None = None, limit: int = 15, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cost_col = _detect_cost_col(list(df.columns))
        svc_col = _detect_service_col(list(df.columns))
        if not cost_col or not svc_col:
            return []
        rows = con.execute(
            f'SELECT "{svc_col}", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "{svc_col}" ORDER BY cost DESC LIMIT {limit}'
        ).fetchall()
        return [{"service": r[0], "cost": round(float(r[1] or 0), 4)} for r in rows]
    finally:
        con.close()


def get_daily_trend(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cost_col = _detect_cost_col(list(df.columns))
        date_col = _detect_date_col(list(df.columns))
        if not cost_col or not date_col:
            return []
        rows = con.execute(
            f'SELECT CAST("{date_col}" AS DATE) AS day, SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY day ORDER BY day'
        ).fetchall()
        return [{"date": str(r[0]), "cost": round(float(r[1] or 0), 4)} for r in rows]
    finally:
        con.close()


def get_cost_by_region(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cost_col = _detect_cost_col(list(df.columns))
        region_col = _detect_region_col(list(df.columns))
        if not cost_col or not region_col:
            return []
        # Exclude rows with no region (blank / NULL AvailabilityZone) so they
        # don't surface as an empty-label region bucket in the breakdown.
        rows = con.execute(
            f'SELECT "{region_col}", SUM("{cost_col}") AS cost '
            f'FROM cur_data '
            f"WHERE \"{region_col}\" IS NOT NULL AND CAST(\"{region_col}\" AS VARCHAR) <> '' "
            f'GROUP BY "{region_col}"'
        ).fetchall()
        # Fold AZ ids to their region prefix (legacy CUR groups by
        # AvailabilityZone, e.g. us-east-1a) and re-aggregate.
        agg: dict[str, float] = {}
        for r in rows:
            region = _region_from_az(str(r[0]) if r[0] is not None else "")
            agg[region] = agg.get(region, 0.0) + float(r[1] or 0)
        return [
            {"region": reg, "cost": round(c, 4)}
            for reg, c in sorted(agg.items(), key=lambda kv: kv[1], reverse=True)
        ]
    finally:
        con.close()


def get_cost_by_account(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        acct_col = _detect_account_col(cols)
        if not cost_col or not acct_col:
            return []
        name_col = "line_item_usage_account_name" if "line_item_usage_account_name" in cols else None
        if name_col:
            rows = con.execute(
                f'SELECT "{acct_col}", "{name_col}", SUM("{cost_col}") AS cost, COUNT(*) AS rc '
                f'FROM cur_data GROUP BY "{acct_col}", "{name_col}" ORDER BY cost DESC'
            ).fetchall()
            return [
                {
                    "account_id": str(r[0]),
                    "account_name": str(r[1]) if r[1] is not None else "",
                    "cost": round(float(r[2] or 0), 4),
                    "row_count": int(r[3] or 0),
                }
                for r in rows
            ]
        rows = con.execute(
            f'SELECT "{acct_col}", SUM("{cost_col}") AS cost, COUNT(*) AS rc '
            f'FROM cur_data GROUP BY "{acct_col}" ORDER BY cost DESC'
        ).fetchall()
        return [
            {
                "account_id": str(r[0]),
                "account_name": "",
                "cost": round(float(r[1] or 0), 4),
                "row_count": int(r[2] or 0),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_org_unit(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col or "org_unit_name" not in cols:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT "org_unit_name", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "org_unit_name" ORDER BY cost DESC'
        ).fetchall()
        return [
            {
                "org_unit": str(r[0]) if r[0] is not None else "",
                "cost": round(float(r[1] or 0), 4),
                "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_environment(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        env_col = _resolve_tag_col(df, "tag_Environment")
        if not cost_col or not env_col:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        has_team = "env_owner_team" in cols
        has_email = "env_owner_email" in cols
        select_extra = ""
        group_extra = ""
        if has_team:
            select_extra += ', MAX("env_owner_team")'
        if has_email:
            select_extra += ', MAX("env_owner_email")'
        rows = con.execute(
            f'SELECT "{env_col}", SUM("{cost_col}") AS cost{select_extra} '
            f'FROM cur_data GROUP BY "{env_col}" ORDER BY cost DESC{group_extra}'
        ).fetchall()
        result = []
        for r in rows:
            env = r[0]
            env_label = str(env) if env is not None and str(env) != "" else "Untagged"
            idx = 2
            team = ""
            email = ""
            if has_team:
                team = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            if has_email:
                email = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            result.append({
                "environment": env_label,
                "cost": round(float(r[1] or 0), 4),
                "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
                "env_owner_team": team,
                "env_owner_email": email,
            })
        return result
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_service_category(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col:
            return []
        if "service_category" not in cols:
            # Legacy CUR has no precomputed category column — derive from name.
            return _service_category_from_names(con, cost_col, _detect_service_col(cols))
        has_team = "service_owner_team" in cols
        has_email = "service_owner_email" in cols
        group_cols = ['"service_category"']
        if has_team:
            group_cols.append('"service_owner_team"')
        if has_email:
            group_cols.append('"service_owner_email"')
        select_clause = ", ".join(group_cols)
        group_clause = ", ".join(group_cols)
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT {select_clause}, SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY {group_clause} ORDER BY cost DESC'
        ).fetchall()
        result = []
        for r in rows:
            idx = 1
            team = ""
            email = ""
            if has_team:
                team = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            if has_email:
                email = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            cost = float(r[idx] or 0)
            result.append({
                "category": str(r[0]) if r[0] is not None else "",
                "owner_team": team,
                "owner_email": email,
                "cost": round(cost, 4),
                "pct_of_total": round(cost / total * 100, 2) if total else 0.0,
            })
        return result
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_tag(csv_text: str | None = None, tag_col: str = "", file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        actual_tag = _resolve_tag_col(df, tag_col)
        logger.info("get_cost_by_tag: requested %r -> resolved %r", tag_col, actual_tag)
        if not cost_col or not actual_tag:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT "{actual_tag}", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "{actual_tag}" ORDER BY cost DESC'
        ).fetchall()
        result = []
        for r in rows:
            val = r[0]
            label = str(val) if val is not None and str(val) != "" else "Untagged"
            cost = float(r[1] or 0)
            result.append({
                "tag_value": label,
                "cost": round(cost, 4),
                "pct_of_total": round(cost / total * 100, 2) if total else 0.0,
            })
        return result
    except Exception:
        return []
    finally:
        con.close()


def get_untagged_resources(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> dict:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        total_rows = len(df)
        # Detect tag columns across tag_ and resourceTags/user: prefixes.
        tag_cols = detect_tag_columns(df)
        logger.info("get_untagged_resources: detected tag columns %s", tag_cols)
        coverage = []
        for tag in tag_cols.values():
            untagged_count = int(con.execute(
                f"SELECT COUNT(*) FROM cur_data "
                f"WHERE \"{tag}\" IS NULL OR CAST(\"{tag}\" AS VARCHAR) = ''"
            ).fetchone()[0] or 0)
            if cost_col:
                untagged_cost = float(con.execute(
                    f'SELECT SUM("{cost_col}") FROM cur_data '
                    f"WHERE \"{tag}\" IS NULL OR CAST(\"{tag}\" AS VARCHAR) = ''"
                ).fetchone()[0] or 0)
            else:
                untagged_cost = 0.0
            coverage_pct = (
                round((total_rows - untagged_count) / total_rows * 100, 1)
                if total_rows else 0.0
            )
            coverage.append({
                "tag": tag,
                "coverage_pct": coverage_pct,
                "untagged_count": untagged_count,
                "untagged_cost": round(untagged_cost, 4),
            })
        overall = (
            round(sum(c["coverage_pct"] for c in coverage) / len(coverage), 1)
            if coverage else 0.0
        )
        return {
            "total_rows": total_rows,
            "tag_coverage": coverage,
            "overall_coverage_pct": overall,
        }
    except Exception:
        return {"total_rows": 0, "tag_coverage": [], "overall_coverage_pct": 0.0}
    finally:
        con.close()


def get_cost_by_pricing_term(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col or "pricing_term" not in cols:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT "pricing_term", SUM("{cost_col}") AS cost, COUNT(*) AS rc '
            f'FROM cur_data GROUP BY "pricing_term" ORDER BY cost DESC'
        ).fetchall()
        return [
            {
                "pricing_term": str(r[0]) if r[0] is not None else "",
                "cost": round(float(r[1] or 0), 4),
                "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
                "row_count": int(r[2] or 0),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_mom_comparison(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        date_col = _detect_date_col(cols)
        svc_col = _detect_service_col(cols)
        if not cost_col or not date_col or not svc_col:
            return []
        rows = con.execute(
            f'SELECT strftime(CAST("{date_col}" AS DATE), \'%Y-%m\') AS month, '
            f'"{svc_col}" AS service, SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY month, service ORDER BY month, cost DESC'
        ).fetchall()
        return [
            {
                "month": str(r[0]),
                "service": str(r[1]) if r[1] is not None else "",
                "cost": round(float(r[2] or 0), 4),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_top_resources(csv_text: str | None = None, limit: int = 10, file_path: str | None = None, filters: dict | None = None) -> list[dict]:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        res_col = _detect_resource_col(cols)
        if not cost_col or not res_col:
            return []
        svc_col = _detect_service_col(cols)
        region_col = _detect_region_col(cols)
        env_col = _resolve_tag_col(df, "tag_Environment")
        team_col = _resolve_tag_col(df, "tag_Team")
        svc_sel = f'MAX("{svc_col}")' if svc_col else "''"
        region_sel = f'MAX("{region_col}")' if region_col else "''"
        env_sel = f'MAX("{env_col}")' if env_col else "''"
        team_sel = f'MAX("{team_col}")' if team_col else "''"
        rows = con.execute(
            f'SELECT "{res_col}", {svc_sel}, {region_sel}, {env_sel}, {team_sel}, '
            f'SUM("{cost_col}") AS cost '
            f'FROM cur_data '
            f"WHERE \"{res_col}\" IS NOT NULL AND CAST(\"{res_col}\" AS VARCHAR) <> '' "
            f'GROUP BY "{res_col}" ORDER BY cost DESC LIMIT {limit}'
        ).fetchall()
        return [
            {
                "resource_id": str(r[0]),
                "service": str(r[1]) if r[1] is not None else "",
                "region": str(r[2]) if r[2] is not None else "",
                "environment": str(r[3]) if r[3] is not None else "",
                "team": str(r[4]) if r[4] is not None else "",
                "cost": round(float(r[5] or 0), 4),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_savings_opportunities(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None) -> dict:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col:
            return {
                "on_demand_cost": 0.0,
                "on_demand_pct": 0.0,
                "top_on_demand_services": [],
                "untagged_cost": 0.0,
                "untagged_pct": 0.0,
                "savings_signals": [],
            }
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        svc_col = _detect_service_col(cols)

        # a) On-Demand cost
        on_demand_cost = 0.0
        top_on_demand_services: list[dict] = []
        if "pricing_term" in cols:
            on_demand_cost = float(con.execute(
                f'SELECT SUM("{cost_col}") FROM cur_data '
                f"WHERE \"pricing_term\" = 'OnDemand'"
            ).fetchone()[0] or 0)
            # b) Top 5 OnDemand services
            if svc_col:
                rows = con.execute(
                    f'SELECT "{svc_col}", SUM("{cost_col}") AS cost FROM cur_data '
                    f"WHERE \"pricing_term\" = 'OnDemand' "
                    f'GROUP BY "{svc_col}" ORDER BY cost DESC LIMIT 5'
                ).fetchall()
                top_on_demand_services = [
                    {"service": str(r[0]) if r[0] is not None else "",
                     "cost": round(float(r[1] or 0), 4)}
                    for r in rows
                ]
        on_demand_pct = round(on_demand_cost / total * 100, 1) if total else 0.0

        # d) Untagged (no owner) cost
        untagged_cost = 0.0
        owner_col = _resolve_tag_col(df, "tag_Owner")
        if owner_col:
            untagged_cost = float(con.execute(
                f'SELECT SUM("{cost_col}") FROM cur_data '
                f"WHERE \"{owner_col}\" IS NULL OR CAST(\"{owner_col}\" AS VARCHAR) = ''"
            ).fetchone()[0] or 0)
        untagged_pct = round(untagged_cost / total * 100, 1) if total else 0.0

        # c) Single-AZ heuristic — region with no trailing -[a-c] AZ suffix
        single_az_cost = 0.0
        region_col = _detect_region_col(cols)
        if region_col:
            single_az_cost = float(con.execute(
                f'SELECT SUM("{cost_col}") FROM cur_data '
                f"WHERE CAST(\"{region_col}\" AS VARCHAR) NOT SIMILAR TO '%-[abc]'"
            ).fetchone()[0] or 0)

        # Human-readable signals
        signals: list[str] = []
        if on_demand_pct > 0:
            signals.append(
                f"{on_demand_pct:.0f}% of spend is On-Demand — "
                f"consider Reserved Instances or Savings Plans"
            )
        if untagged_cost > 0:
            signals.append(
                f"${untagged_cost:,.0f} spend has no owner tag — "
                f"cost allocation at risk"
            )
        if single_az_cost > 0:
            signals.append(
                f"${single_az_cost:,.0f} spend lacks a clear AZ-level region — "
                f"review for resiliency/placement"
            )

        return {
            "on_demand_cost": round(on_demand_cost, 4),
            "on_demand_pct": on_demand_pct,
            "top_on_demand_services": top_on_demand_services,
            "untagged_cost": round(untagged_cost, 4),
            "untagged_pct": untagged_pct,
            "savings_signals": signals,
        }
    except Exception:
        return {
            "on_demand_cost": 0.0,
            "on_demand_pct": 0.0,
            "top_on_demand_services": [],
            "untagged_cost": 0.0,
            "untagged_pct": 0.0,
            "savings_signals": [],
        }
    finally:
        con.close()


def run_context_query(csv_text: str) -> dict:
    """Return the pre-computed data context Claude uses for cost Q&A.

    Mirrors the original run_natural_language_query() from engine.py.
    """
    summary = get_total_cost(csv_text)
    services = get_cost_by_service(csv_text, limit=10)
    trend = get_daily_trend(csv_text)
    return {
        "summary": summary,
        "top_services": services,
        "daily_trend": trend[-14:],
    }


def get_enrichment_summary(csv_text: str | None = None, enricher=None, file_path: str | None = None, filters: dict | None = None) -> dict:
    """Compute inventory-enrichment coverage for the CUR data.

    Returns ``{"active": False}`` when no enricher / inventory is in play, so the
    existing flow is never affected. When active, returns match counts/rates,
    cost-weighted coverage, per-service match rates, the top unmatched resources
    by cost (with a reason), and a before/after tag-coverage comparison.
    """
    if enricher is None or not getattr(enricher, "active", False):
        return {"active": False}

    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        account_col = _detect_account_col(cols)
        resource_col = _detect_resource_col(cols)
        if not account_col:
            return {
                "active": True,
                "joinable": False,
                "reason": "CUR data lacks an account id column",
            }
        # No resource column => account-level enrichment fallback. The match
        # rate then reflects account-level coverage, not resource-level.
        enrichment_level = "resource" if resource_col else "account"

        stats = enricher.get_match_stats()
        matched_mask = getattr(enricher, "_last_matched", [])
        reasons = getattr(enricher, "_last_reason", [])

        # Cost-weighted coverage.
        total_spend = 0.0
        matched_spend = 0.0
        if cost_col:
            costs = pd.to_numeric(df[cost_col], errors="coerce").fillna(0.0).tolist()
            total_spend = float(sum(costs))
            for i, c in enumerate(costs):
                if i < len(matched_mask) and matched_mask[i]:
                    matched_spend += float(c)
        spend_rate = round(matched_spend / total_spend * 100, 1) if total_spend else 0.0

        # Top unmatched resources by cost.
        unmatched_top: list[dict] = []
        if cost_col and resource_col:
            svc_col = _detect_service_col(cols)
            tmp = df.copy()
            tmp["__cost"] = pd.to_numeric(tmp[cost_col], errors="coerce").fillna(0.0)
            tmp["__matched"] = (matched_mask + [False] * len(tmp))[: len(tmp)]
            tmp["__reason"] = (reasons + [""] * len(tmp))[: len(tmp)]
            un = tmp[~tmp["__matched"]]
            grp = (
                un.groupby(resource_col)
                .agg(cost=("__cost", "sum"))
                .reset_index()
                .sort_values("cost", ascending=False)
                .head(20)
            )
            reason_by_res = (
                un.groupby(resource_col)["__reason"].first().to_dict()
            )
            svc_by_res = (
                un.groupby(resource_col)[svc_col].first().to_dict() if svc_col else {}
            )
            for _, r in grp.iterrows():
                rid = str(r[resource_col])
                unmatched_top.append({
                    "resource_id": rid,
                    "service": str(svc_by_res.get(rid, "")) if svc_by_res else "",
                    "cost": round(float(r["cost"]), 4),
                    "reason": str(reason_by_res.get(rid, REASON_LABEL_DEFAULT)),
                })

        # Before/after tag coverage (native CUR tag vs inventory-filled).
        total_rows = len(df)
        before_after = {}
        comparisons = [
            ("Customer", "tag_Customer", "inv_customer"),
            ("Application", "tag_Product", "inv_application"),
            ("Budget_Code", "tag_CostCentre", "inv_budget_code"),
        ]
        for label, native_col, inv_col in comparisons:
            before_pct = 0.0
            after_pct = 0.0
            if total_rows:
                native_nonblank = pd.Series([False] * total_rows)
                resolved_native = _resolve_tag_col(df, native_col)
                if resolved_native:
                    native_nonblank = df[resolved_native].astype(str).str.strip().ne("") & df[resolved_native].notna()
                before_pct = round(float(native_nonblank.mean()) * 100, 1)
                inv_nonblank = pd.Series([False] * total_rows)
                if inv_col in df.columns:
                    inv_nonblank = df[inv_col].astype(str).str.strip().ne("")
                after_nonblank = native_nonblank | inv_nonblank
                after_pct = round(float(after_nonblank.mean()) * 100, 1)
            before_after[label] = {
                "before_pct": before_pct,
                "after_pct": after_pct,
                "improvement_pct": round(after_pct - before_pct, 1),
            }

        return {
            "active": True,
            "joinable": True,
            "enrichment_level": enrichment_level,
            "has_resource_column": resource_col is not None,
            "matched_count": stats["matched_count"],
            "unmatched_count": stats["unmatched_count"],
            "match_rate_pct": stats["match_rate_pct"],
            "total_spend": round(total_spend, 4),
            "matched_spend": round(matched_spend, 4),
            "spend_match_rate_pct": spend_rate,
            "per_service": stats["per_service"],
            "unmatched_top": unmatched_top,
            "before_after": before_after,
        }
    except Exception:
        return {"active": True, "joinable": False, "reason": "enrichment summary failed"}
    finally:
        con.close()


# Default unmatched reason label (kept local to avoid importing enricher here).
REASON_LABEL_DEFAULT = "Not in inventory"


def get_enriched_summary(csv_text=None, enricher=None, file_path=None) -> dict:
    """Pre-computed enriched aggregations for the dashboard.

    Returns Environment / Customer / Application / Budget-Code spend breakdowns,
    per-attribute tag coverage and a native-vs-enriched before/after — all via
    DuckDB GROUP BY over the (optionally inventory-enriched) frame, using
    ``inv_*`` columns as primary with the native CUR tag as fallback. Lets the
    dashboard render the Environment tab and tag panels from one small payload
    instead of streaming every row to the browser.
    """
    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path)
    try:
        cols = list(df.columns)
        present = set(cols)
        cost_col = _detect_cost_col(cols)
        cost_sql = f'TRY_CAST("{cost_col}" AS DOUBLE)' if cost_col else "0"
        total_cost = float(
            con.execute(f"SELECT COALESCE(SUM({cost_sql}), 0) FROM cur_data").fetchone()[0] or 0
        )
        total_rows = int(con.execute("SELECT COUNT(*) FROM cur_data").fetchone()[0] or 0)

        if not getattr(enricher, "active", False):
            level = "none"
        else:
            level = "resource" if _detect_resource_col(cols) else "account"

        def _nz(colname: str) -> str:
            # Non-blank-or-NULL: trims and maps '' to NULL so coverage is honest.
            return f"NULLIF(TRIM(CAST(\"{colname}\" AS VARCHAR)), '')"

        def _exprs(inv_col: str, native_tag: str):
            """(effective, native) SQL exprs — inv_* primary, native CUR tag
            fallback. Either element may be None when its column is absent."""
            native = _resolve_tag_col(df, native_tag)
            native_expr = _nz(native) if native else None
            parts = []
            if inv_col in present:
                parts.append(_nz(inv_col))
            if native_expr:
                parts.append(native_expr)
            if not parts:
                return None, native_expr
            eff = parts[0] if len(parts) == 1 else "COALESCE(" + ", ".join(parts) + ")"
            return eff, native_expr

        def _breakdown(eff_expr, key: str) -> list[dict]:
            if eff_expr is None:
                return []
            rows = con.execute(
                f"SELECT COALESCE({eff_expr}, 'Untagged') AS label, "
                f"COALESCE(SUM({cost_sql}), 0) AS cost "
                f"FROM cur_data GROUP BY 1 ORDER BY cost DESC"
            ).fetchall()
            out = []
            for label, cost in rows:
                cost = float(cost or 0)
                out.append({
                    key: str(label),
                    "cost": round(cost, 2),
                    "pct": round(cost / total_cost * 100, 1) if total_cost else 0.0,
                })
            return out

        def _coverage(eff_expr, native_expr) -> dict:
            covered = 0
            untagged_cost = 0.0
            before_pct = 0.0
            after_pct = 0.0
            if eff_expr is not None and total_rows:
                covered = int(con.execute(
                    f"SELECT COUNT(*) FROM cur_data WHERE {eff_expr} IS NOT NULL"
                ).fetchone()[0] or 0)
                untagged_cost = float(con.execute(
                    f"SELECT COALESCE(SUM({cost_sql}), 0) FROM cur_data WHERE {eff_expr} IS NULL"
                ).fetchone()[0] or 0)
                after_pct = round(covered / total_rows * 100, 1)
            if native_expr is not None and total_rows:
                native_cov = int(con.execute(
                    f"SELECT COUNT(*) FROM cur_data WHERE {native_expr} IS NOT NULL"
                ).fetchone()[0] or 0)
                before_pct = round(native_cov / total_rows * 100, 1)
            return {
                "covered_pct": round(covered / total_rows * 100, 1) if total_rows else 0.0,
                "untagged_cost": round(untagged_cost, 2),
                "before_pct": before_pct,
                "after_pct": after_pct,
            }

        # inv_* output column, native CUR tag fallback.
        specs = {
            "Environment": ("inv_environment", "tag_Environment"),
            "Customer": ("inv_customer", "tag_Customer"),
            "Application": ("inv_application", "tag_Product"),
            "Budget_Code": ("inv_budget_code", "tag_CostCentre"),
        }
        eff = {label: _exprs(inv, nat) for label, (inv, nat) in specs.items()}

        tag_coverage: dict = {}
        before_after: dict = {}
        for label, (eff_expr, native_expr) in eff.items():
            cov = _coverage(eff_expr, native_expr)
            tag_coverage[label] = {
                "covered_pct": cov["covered_pct"],
                "untagged_cost": cov["untagged_cost"],
            }
            if label != "Environment":
                before_after[label] = {
                    "before_pct": cov["before_pct"],
                    "after_pct": cov["after_pct"],
                }

        return {
            "enrichment_level": level,
            "env_breakdown": _breakdown(eff["Environment"][0], "environment"),
            "customer_breakdown": _breakdown(eff["Customer"][0], "customer"),
            "application_breakdown": _breakdown(eff["Application"][0], "application"),
            "budget_code_breakdown": _breakdown(eff["Budget_Code"][0], "budget_code"),
            "tag_coverage": tag_coverage,
            "before_after": before_after,
        }
    finally:
        con.close()


# ── ToolExecutor wrapper ──────────────────────────────────────────────────────

class CurQueryTool(ToolExecutor):
    """Run a targeted DuckDB query against cached CUR data."""

    name: ClassVar[str] = "query_cur"
    description: ClassVar[str] = (
        "Run a targeted cost analysis query against the loaded AWS CUR data. "
        "query_type options: "
        "'total_cost' — total spend and row count; "
        "'cost_by_service' — spend breakdown by AWS service; "
        "'daily_trend' — day-by-day cost totals sorted by date; "
        "'cost_by_region' — spend breakdown by AWS region."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "session_id": {
                "type": "string",
                "description": "Session whose cached CUR CSV to query.",
            },
            "query_type": {
                "type": "string",
                "enum": ["total_cost", "cost_by_service", "daily_trend", "cost_by_region"],
                "description": "Which analysis to run.",
            },
        },
        "required": ["session_id", "query_type"],
    }

    def __init__(self, cache: dict[str, str]) -> None:
        self._cache = cache  # session_id → raw CSV text

    async def execute(self, session_id: str, query_type: QueryType) -> str:  # type: ignore[override]
        csv_text = self._cache.get(session_id)
        if not csv_text:
            return json.dumps({"error": "No CUR data loaded for this session."})

        if query_type == "total_cost":
            return json.dumps(get_total_cost(csv_text))
        if query_type == "cost_by_service":
            return json.dumps(get_cost_by_service(csv_text))
        if query_type == "daily_trend":
            return json.dumps(get_daily_trend(csv_text))
        if query_type == "cost_by_region":
            return json.dumps(get_cost_by_region(csv_text))
        return json.dumps({"error": f"Unknown query_type '{query_type}'."})
