"""DuckDB-backed CUR query engine.

All public query functions accept raw CSV text and return plain Python structures.
The CurQueryTool wraps them as an Anthropic-callable ToolExecutor that reads from
the per-session CUR cache populated by main.py.
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import threading
import time
from types import SimpleNamespace
from typing import Any, ClassVar, Literal

import duckdb
import pandas as pd

from tools.base import ToolExecutor
from tools.data_sources.registry import get_registry
from tools.inventory_enricher import build_enricher

logger = logging.getLogger(__name__)

# Line item types that represent billing credits/discounts — always shown in
# "Credits / Refunds" category regardless of the service name.
_CREDIT_LINE_ITEM_TYPES = {'EdpDiscount', 'SavingsPlanNegation', 'BundledDiscount', 'Refund', 'Credit'}

def ingest_to_duckdb(src_path: str, duckdb_path: str) -> None:
    """Ingest a CSV or gz file into a persistent DuckDB database file.
    Uses ZSTD compression for optimal storage and query performance.
    The source file is read via read_csv_auto — supports .csv and .csv.gz.
    Raises on any error so the caller can clean up."""
    path_sql = str(src_path).replace("'", "''")
    if str(src_path).lower().endswith(".parquet"):
        reader = f"read_parquet('{path_sql}')"
    else:
        reader = f"read_csv_auto('{path_sql}', ignore_errors=true)"
    con = duckdb.connect(database=duckdb_path)
    try:
        con.execute("PRAGMA threads=4")
        con.execute(f"CREATE TABLE cur_data AS SELECT * FROM {reader}")
    finally:
        con.close()

def get_per_service_match_rate(file_path: str, enricher) -> tuple[list[dict], list[dict]]:
    """Compute per-service resource match rate and top unmatched resources.
    Returns (per_service, unmatched_top) for the Inventory Enrichment panel."""
    if not file_path or enricher is None or not getattr(enricher, "active", False):
        return [], []
    try:
        con = duckdb.connect(":memory:")
        safe = str(file_path).replace("'", "''")
        if str(file_path).lower().endswith(".duckdb"):
            con.execute(f"ATTACH '{safe}' AS src (READ_ONLY)")
            con.execute("CREATE VIEW cur_data AS SELECT * FROM src.cur_data")
        else:
            con.execute(f"CREATE VIEW cur_data AS SELECT * FROM read_csv_auto('{safe}', ignore_errors=true)")
        _register_resource_lookup(con, enricher)
        _register_account_lookup(con, enricher)
        cols = [r[0] for r in con.execute("DESCRIBE cur_data").fetchall()]
        cost_col = _detect_cost_col(cols)
        svc_col = _detect_service_col(cols)
        res_col = _detect_resource_col(cols)
        if not cost_col or not svc_col or not res_col:
            return [], []
        # Per-service match rate
        rows = con.execute(
            f'SELECT c."{svc_col}" AS svc, '
            f'COUNT(*) AS in_cur, '
            f'COUNT(r.resource_id) AS matched, '
            f'SUM(c."{cost_col}") AS cost '
            f'FROM cur_data c '
            f'LEFT JOIN inv_resource_lookup r ON '
            f'SPLIT_PART(CAST(c."{res_col}" AS VARCHAR), \':\', -1) = r.resource_id '
            f'WHERE c."{res_col}" IS NOT NULL AND c."{res_col}" != \'\' '
            f'GROUP BY svc ORDER BY in_cur DESC LIMIT 20'
        ).fetchall()
        per_service = [
            {
                "service": str(r[0] or "Unknown"),
                "in_cur": int(r[1] or 0),
                "matched": int(r[2] or 0),
                "unmatched": int(r[1] or 0) - int(r[2] or 0),
                "match_rate_pct": round(int(r[2] or 0) / int(r[1]) * 100, 1) if r[1] else 0.0,
            }
            for r in rows
        ]
        # Top unmatched resources by cost
        unmatched_rows = con.execute(
            f'SELECT c."{res_col}", c."{svc_col}", SUM(c."{cost_col}") AS cost '
            f'FROM cur_data c '
            f'LEFT JOIN inv_resource_lookup r ON '
            f'SPLIT_PART(CAST(c."{res_col}" AS VARCHAR), \':\', -1) = r.resource_id '
            f'WHERE c."{res_col}" IS NOT NULL AND c."{res_col}" != \'\' '
            f'AND r.resource_id IS NULL '
            f'GROUP BY c."{res_col}", c."{svc_col}" '
            f'ORDER BY cost DESC LIMIT 10'
        ).fetchall()
        unmatched_top = [
            {
                "resource_id": str(r[0] or ""),
                "service": str(r[1] or ""),
                "cost": round(float(r[2] or 0), 4),
                "reason": "Not in inventory",
            }
            for r in unmatched_rows
        ]
        con.close()
        return per_service, unmatched_top
    except Exception:
        logger.exception("get_per_service_match_rate failed")
        return [], []

def get_before_after_coverage(file_path: str, enricher) -> dict:
    """Compute before/after cost attribution improvement from inventory enrichment.
    Returns dict keyed by attribute name with before_pct, after_pct, improvement_pct."""
    if not file_path or enricher is None or not getattr(enricher, "active", False):
        return {}
    try:
        con = duckdb.connect(":memory:")
        safe = str(file_path).replace("'", "''")
        if str(file_path).lower().endswith(".duckdb"):
            con.execute(f"ATTACH '{safe}' AS src (READ_ONLY)")
            con.execute("CREATE VIEW cur_data AS SELECT * FROM src.cur_data")
        else:
            con.execute(f"CREATE VIEW cur_data AS SELECT * FROM read_csv_auto('{safe}', ignore_errors=true)")
        _register_account_lookup(con, enricher)
        cols = [r[0] for r in con.execute("DESCRIBE cur_data").fetchall()]
        cost_col = _detect_cost_col(cols)
        acct_col = _detect_account_col(cols)
        if not cost_col or not acct_col:
            return {}
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        if not total:
            return {}
        # Check each INV_FIELD_MAP attribute
        from tools.inventory_enricher import INV_FIELD_MAP
        result = {}
        inv_attrs = {
            "Environment": "inv_environment",
            "Customer": "inv_customer",
            "Cost Centre": "inv_budget_code",
        }
        # Map label to AWS tag key suffix for JSON LIKE search
        _tag_key_map = {
            "Environment": "user:Environment",
            "Customer": "user:Customer",
            "Cost Centre": "user:CostCentre",
        }
        for label, inv_col in inv_attrs.items():
            # Before: cost with native AWS tag for this attribute
            tag_col = _resolve_tag_col(SimpleNamespace(columns=cols), f"tag_{label.replace(' ', '')}")
            before_cost = 0.0
            if tag_col:
                before_cost = float(con.execute(
                    f'SELECT SUM("{cost_col}") FROM cur_data '
                    f'WHERE "{tag_col}" IS NOT NULL AND TRIM("{tag_col}") != \'\''
                ).fetchone()[0] or 0)
            elif "tags" in cols:
                # CUR 2.0 — tags stored as JSON string; use LIKE to detect tag presence
                tag_key = _tag_key_map.get(label, f"user:{label}")
                try:
                    before_cost = float(con.execute(
                        f'SELECT SUM("{cost_col}") FROM cur_data '
                        f"WHERE tags IS NOT NULL AND tags LIKE '%{tag_key}%'"
                    ).fetchone()[0] or 0)
                except Exception:
                    pass
            # After: cost attributed via inventory JOIN
            try:
                after_cost = float(con.execute(
                    f'SELECT SUM(c."{cost_col}") FROM cur_data c '
                    f'LEFT JOIN inv_account_lookup i ON CAST(c."{acct_col}" AS VARCHAR) = i.account_id '
                    f'WHERE i."{inv_col}" IS NOT NULL AND TRIM(i."{inv_col}") != \'\''
                ).fetchone()[0] or 0)
            except Exception:
                after_cost = 0.0
            before_pct = round(before_cost / total * 100, 1)
            after_pct = round(after_cost / total * 100, 1)
            result[label] = {
                "before_pct": before_pct,
                "after_pct": after_pct,
                "improvement_pct": round(after_pct - before_pct, 1),
            }
        con.close()
        return result
    except Exception:
        logger.exception("get_before_after_coverage failed")
        return {}

# ── Column detection helpers ──────────────────────────────────────────────────

QueryType = Literal["total_cost", "cost_by_service", "daily_trend", "cost_by_region", "cost_by_environment", "cost_by_account", "cost_by_tag"]


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
    "product/region",        # real CUR 2.0 slash format
    "product_region_code",   # CUR 2.0 Athena/snake_case format
    "product_region",        # normalised
    "AvailabilityZone",      # legacy CUR fallback (AZ not region but closest)
    "region",                # synthetic
]
ACCOUNT_COL_CANDIDATES = [
    "lineItem/UsageAccountId",      # real AWS CUR (current format)
    "line_item_UsageAccountId",     # normalised CUR 2.0 slash format
    "line_item_usage_account_id",   # normalised format
    "LinkedAccountId",              # legacy CUR (per-resource / usage account)
    "account_id",                   # synthetic / test data
    "PayerAccountId",               # legacy CUR payer (master) — lowest priority
]
RESOURCE_COL_CANDIDATES = [
    "lineItem/ResourceId",          # real AWS CUR (current format)
    "line_item_ResourceId",         # normalised CUR 2.0 slash format
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
    gross_total = float(con.execute(
        f'SELECT SUM("{cost_col}") FROM cur_data WHERE TRY_CAST("{cost_col}" AS DOUBLE) > 0'
    ).fetchone()[0] or 0)
    total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
    rows = con.execute(
        f"SELECT COALESCE(NULLIF(TRIM(CAST(\"{svc_col}\" AS VARCHAR)), ''), 'Unallocated') AS svc, "
        f"COALESCE(line_item_line_item_type, '') AS item_type, "
        f'SUM("{cost_col}") AS cost FROM cur_data GROUP BY svc, item_type'
    ).fetchall()
    cat_totals: dict[str, float] = {}
    for r in rows:
        svc_name = str(r[0] or "")
        item_type = str(r[1] or "")
        cost = float(r[2] or 0)
        if item_type in _CREDIT_LINE_ITEM_TYPES:
            cat = "Credits / Refunds"
        elif svc_name == "Unallocated":
            cat = "Unallocated"
        else:
            cat = _categorise_service(svc_name)
        cat_totals[cat] = cat_totals.get(cat, 0.0) + cost
    denom = gross_total if gross_total else total
    return [
        {
            "category": cat,
            "owner_team": "",
            "owner_email": "",
            "cost": round(c, 4),
            "pct_of_total": round(c / denom * 100, 2) if denom else 0.0,
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


def _sql_lit(value) -> str:
    """Single-quote a Python value as a SQL string literal, escaping quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def _row_count(df, con: duckdb.DuckDBPyConnection) -> int:
    """Number of rows in ``cur_data``. ``len(df)`` in csv_text mode (df is a real
    DataFrame); ``COUNT(*)`` over the view in file-path mode (df is the
    columns-carrier returned by :func:`_load_df`, which holds no rows)."""
    if isinstance(df, pd.DataFrame):
        return len(df)
    return int(con.execute("SELECT COUNT(*) FROM cur_data").fetchone()[0] or 0)


def _build_filter_sql(filters: dict | None, con: duckdb.DuckDBPyConnection) -> str:
    """Translate the dashboard ``filters`` dict into a SQL WHERE-clause body
    (no leading ``WHERE``) for the native ``cur_data`` view used by the
    file-path pipeline. It mirrors :func:`_apply_filters` exactly, but emits SQL
    so DuckDB filters while streaming the file instead of us materialising a
    DataFrame first.

    Column names are resolved with the same detectors used everywhere else
    (via the view's column list), so it works across CUR 2.0 / legacy /
    normalised / synthetic exports. Missing columns are skipped (never error);
    returns ``""`` when nothing applies.

    Supported keys match the dropdowns: ``date_from``, ``date_to``, ``accounts``,
    ``environments``, ``services``, ``regions``, ``pricing_terms``,
    ``tag_products``, ``tag_teams``.
    """
    if not filters:
        return ""
    cols = [row[0] for row in con.execute("DESCRIBE cur_data").fetchall()]
    shim = SimpleNamespace(columns=cols)  # lets us reuse resolve_col / tag detectors
    clauses: list[str] = []

    def _in_clause(col: str, values, include_untagged: bool = False) -> str | None:
        listed = [v for v in values if v != "Untagged"]
        parts: list[str] = []
        if listed:
            inlist = ", ".join(_sql_lit(v) for v in listed)
            parts.append(f'CAST("{col}" AS VARCHAR) IN ({inlist})')
        if include_untagged:
            parts.append(
                f'("{col}" IS NULL OR '
                f"TRIM(CAST(\"{col}\" AS VARCHAR)) IN ('', 'nan', 'NaN', 'None'))"
            )
        return "(" + " OR ".join(parts) + ")" if parts else None

    # ── Date range (mirrors the pd.to_datetime coerce + inclusive end day) ──
    date_col = _detect_date_col(cols)
    if date_col and (filters.get("date_from") or filters.get("date_to")):
        if filters.get("date_from"):
            clauses.append(
                f'TRY_CAST("{date_col}" AS TIMESTAMP) >= {_sql_lit(filters["date_from"])}'
            )
        if filters.get("date_to"):
            clauses.append(
                f'TRY_CAST("{date_col}" AS TIMESTAMP) < '
                f"CAST({_sql_lit(filters['date_to'])} AS TIMESTAMP) + INTERVAL 1 DAY"
            )

    # ── Accounts (match the account id OR the account name) ──
    if filters.get("accounts"):
        inlist = ", ".join(_sql_lit(v) for v in filters["accounts"])
        acct_id = _detect_account_col(cols)
        name_col = (
            "line_item_usage_account_name"
            if "line_item_usage_account_name" in cols else None
        )
        ors: list[str] = []
        if acct_id:
            ors.append(f'CAST("{acct_id}" AS VARCHAR) IN ({inlist})')
        if name_col:
            ors.append(f'CAST("{name_col}" AS VARCHAR) IN ({inlist})')
        if ors:
            clauses.append("(" + " OR ".join(ors) + ")")

    # ── Services ──
    if filters.get("services"):
        svc = _detect_service_col(cols)
        if svc:
            c = _in_clause(svc, filters["services"])
            if c:
                clauses.append(c)

    # ── Environments (native tag_Environment; "Untagged" → blank/NULL) ──
    if filters.get("environments"):
        env = _resolve_tag_col(shim, "tag_Environment")
        if env:
            vals = filters["environments"]
            c = _in_clause(env, vals, include_untagged=("Untagged" in vals))
            if c:
                clauses.append(c)

    # ── Regions (fold AZ → region prefix to match the breakdown values) ──
    if filters.get("regions"):
        reg = _detect_region_col(cols)
        if reg:
            # Same fold as _region_from_az: strip the trailing AZ letter.
            folded = (
                f'regexp_replace(CAST("{reg}" AS VARCHAR), '
                f"'^([a-z]{{2}}-[a-z]+-[0-9]+)[a-z]$', '\\1')"
            )
            inlist = ", ".join(_sql_lit(v) for v in filters["regions"])
            clauses.append(f"{folded} IN ({inlist})")

    # ── Pricing terms ──
    if filters.get("pricing_terms"):
        if "pricing_term" in cols:
            c = _in_clause("pricing_term", filters["pricing_terms"])
            if c:
                clauses.append(c)

    # ── Tag: Product / Team ("Untagged" → blank/NULL) ──
    if filters.get("tag_products"):
        col = _resolve_tag_col(shim, "tag_Product")
        if col:
            vals = filters["tag_products"]
            c = _in_clause(col, vals, include_untagged=("Untagged" in vals))
            if c:
                clauses.append(c)
    if filters.get("tag_teams"):
        col = _resolve_tag_col(shim, "tag_Team")
        if col:
            vals = filters["tag_teams"]
            c = _in_clause(col, vals, include_untagged=("Untagged" in vals))
            if c:
                clauses.append(c)

    return " AND ".join(clauses)


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
            return df
        _df_cache.pop(file_path, None)
    return None


def _cache_df(file_path: str, df: pd.DataFrame) -> None:
    _df_cache[file_path] = (df, time.time())


def invalidate_df_cache(file_path: str | None = None) -> None:
    """Drop a cached DataFrame (or all of them when ``file_path`` is None)."""
    if file_path is None:
        _df_cache.clear()
    else:
        _df_cache.pop(file_path, None)


def _register_account_lookup(con: duckdb.DuckDBPyConnection, enricher) -> bool:
    """Register the enricher's account-level inventory lookup as a tiny DuckDB
    in-memory table ``inv_account_lookup`` on the given connection.

    Columns are named with the ``inv_*`` prefix (matching INV_FIELD_MAP) so
    query functions can reference them as ``i.inv_customer``, ``i.inv_environment``
    etc. in LEFT JOIN clauses.

    Returns True when the table was created, False when enricher is inactive or
    the lookup is empty.
    """
    if enricher is None or not getattr(enricher, "active", False):
        return False
    lookup: dict = getattr(enricher, "_account_lookup", {})
    if not lookup:
        return False
    # INV_FIELD_MAP: {inv_col_name -> raw_field_name} — invert to raw -> inv_col.
    from tools.inventory_enricher import INV_FIELD_MAP
    raw_to_inv: dict[str, str] = {v: k for k, v in INV_FIELD_MAP.items()}
    # Collect only the inv_* columns that have data in the lookup.
    inv_fields: list[str] = []
    for entry in lookup.values():
        for raw_field in entry:
            inv_col = raw_to_inv.get(raw_field)
            if inv_col and inv_col not in inv_fields:
                inv_fields.append(inv_col)
    if not inv_fields:
        return False
    col_defs = ", ".join(f'"{f}" VARCHAR' for f in inv_fields)
    con.execute(f'CREATE TABLE inv_account_lookup (account_id VARCHAR, {col_defs})')
    placeholders = ", ".join("?" * (1 + len(inv_fields)))
    for acct_id, entry in lookup.items():
        vals = [str(acct_id)] + [
            str(entry.get(INV_FIELD_MAP[f], "") or "") for f in inv_fields
        ]
        con.execute(f"INSERT INTO inv_account_lookup VALUES ({placeholders})", vals)
    return True


def _register_resource_lookup(con: duckdb.DuckDBPyConnection, enricher) -> bool:
    """Register the enricher's resource-level inventory lookup as a DuckDB
    in-memory table ``inv_resource_lookup`` on the given connection.
    Keyed by resource_id; columns named with inv_* prefix matching INV_FIELD_MAP.
    Returns True when the table was created, False when enricher is inactive."""
    if enricher is None or not getattr(enricher, "active", False):
        return False
    lookup: dict = getattr(enricher, "_lookup", {})
    if not lookup:
        return False
    from tools.inventory_enricher import INV_FIELD_MAP
    raw_to_inv: dict[str, str] = {v: k for k, v in INV_FIELD_MAP.items()}
    inv_fields: list[str] = []
    for entry in lookup.values():
        for raw_field in entry:
            inv_col = raw_to_inv.get(raw_field)
            if inv_col and inv_col not in inv_fields:
                inv_fields.append(inv_col)
    if not inv_fields:
        return False
    col_defs = ", ".join(f'"{f}" VARCHAR' for f in inv_fields)
    con.execute('DROP TABLE IF EXISTS inv_resource_lookup')
    con.execute(f'CREATE TABLE inv_resource_lookup (resource_id VARCHAR, {col_defs})')
    placeholders = ", ".join("?" * (1 + len(inv_fields)))
    for (acct_id, res_id), entry in lookup.items():
        vals = [str(res_id)] + [
            str(entry.get(INV_FIELD_MAP[f], "") or "") for f in inv_fields
        ]
        con.execute(f"INSERT INTO inv_resource_lookup VALUES ({placeholders})", vals)
    return True

def _has_resource_lookup(con: duckdb.DuckDBPyConnection) -> bool:
    """True when inv_resource_lookup was registered on this connection."""
    try:
        con.execute("SELECT 1 FROM inv_resource_lookup LIMIT 1")
        return True
    except Exception:
        return False


def _load_df(
    csv_text: str | None = None, enricher=None, file_path: str | None = None,
    filters: dict | None = None,
) -> tuple[pd.DataFrame, duckdb.DuckDBPyConnection]:
    """Load CUR data into a DataFrame + DuckDB connection.

    The source is either ``csv_text`` (the legacy in-memory pipeline) or
    ``file_path`` (the file-path pipeline used for large CUR files).

    **File-path mode (Step 1):** DuckDB reads the CUR straight from disk through
    a native ``cur_data`` view (``read_csv_auto`` / ``read_parquet``) — nothing
    is materialised in Python, so aggregations stream over the file instead of
    parsing a multi-GB DataFrame. Filters are pushed down as a SQL ``WHERE`` via
    :func:`_build_filter_sql`. The returned ``df`` is **not** a DataFrame: it is
    a lightweight columns-carrier (``types.SimpleNamespace`` exposing only
    ``.columns``) so query functions can keep detecting columns via
    ``df.columns`` while every aggregation runs against the view. Row counts come
    from the view via :func:`_row_count`; the two enrichment summaries that need
    real row data are handled separately. (The pandas ``_df_cache`` is
    intentionally bypassed in this mode now; inventory enrichment for file-path
    mode lands in Step 3 — it is silently skipped here.)

    **csv_text mode (unchanged):** parsed with ``pd.read_csv``, optionally
    enriched and filtered in pandas, then registered as ``cur_data``.

    ``enricher`` is optional. In csv_text mode, when supplied, the inventory
    enricher adds ``inv_*`` virtual columns *before* the frame is registered;
    an inactive enricher (no inventory loaded) is a safe no-op.

    ``filters`` (optional) restricts ``cur_data`` so every aggregation operates
    on the filtered subset.
    """
    if file_path is not None:
        # Prefer persistent DuckDB file if available — instant queries, no gz decompression.
        _duckdb_path = str(file_path).replace(".csv.gz", ".duckdb").replace(".csv", ".duckdb")
        if not str(file_path).endswith(".duckdb") and os.path.exists(_duckdb_path):
            file_path = _duckdb_path
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA memory_limit='500MB'")
    if file_path is not None:
        # ── File-path pipeline: DuckDB native view straight off disk ──
        path_sql = str(file_path).replace("'", "''")
        if str(file_path).lower().endswith(".duckdb"):
            # Persistent DuckDB file — attach and use cur_data table directly.
            con.execute(f"ATTACH '{path_sql}' AS src (READ_ONLY)")
            con.execute("CREATE VIEW cur_data AS SELECT * FROM src.cur_data")
        elif str(file_path).lower().endswith(".parquet"):
            reader = f"read_parquet('{path_sql}')"
            con.execute(f"CREATE VIEW cur_data AS SELECT * FROM {reader}")
        else:
            reader = f"read_csv_auto('{path_sql}', ignore_errors=true)"
            con.execute(f"CREATE VIEW cur_data AS SELECT * FROM {reader}")
        if filters:
            filter_sql = _build_filter_sql(filters, con)
            if filter_sql:
                con.execute(
                    f"CREATE OR REPLACE VIEW cur_data AS "
                    f"SELECT * FROM {reader} WHERE {filter_sql}"
                )
        # Register the inventory account lookup as a tiny in-memory table so
        # query functions can LEFT JOIN inv_account_lookup without materialising
        # any CUR rows. The table holds at most one row per inventory account
        # (~24 rows, ~1KB) — zero impact on the file-path memory profile.
        _register_account_lookup(con, enricher)
        _register_resource_lookup(con, enricher)
        # Return a columns-carrier (NOT a DataFrame): query functions detect
        # columns via ``df.columns`` while all aggregation hits the view, so no
        # CUR data is materialised in Python. Append inv_* columns to the carrier
        # so downstream detectors see them alongside native CUR columns.
        cols = [row[0] for row in con.execute("DESCRIBE cur_data").fetchall()]
        inv_cols = [row[0] for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'inv_account_lookup' AND column_name <> 'account_id'"
        ).fetchall()] if getattr(enricher, "active", False) else []
        df_meta = SimpleNamespace(columns=cols + inv_cols)
        return df_meta, con
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
        cost_col = _detect_cost_col(list(df.columns))
        if not cost_col:
            return {"error": "No cost column found in CUR data."}
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        return {
            "total_cost": round(total, 4),
            "row_count": _row_count(df, con),
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
            f"SELECT COALESCE(NULLIF(TRIM(CAST(\"{svc_col}\" AS VARCHAR)), ''), 'Unallocated') AS svc, "
            f'SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY svc ORDER BY cost DESC LIMIT {limit}'
        ).fetchall()
        return [{"service": r[0], "cost": round(float(r[1] or 0), 4)} for r in rows]
    finally:
        con.close()


def _is_marketplace_code(code: str) -> bool:
    """True if product code looks like an opaque AWS Marketplace identifier."""
    if not code:
        return False
    code = code.strip()
    # Known AWS service prefixes
    if code.startswith(('Amazon', 'AWS', 'aws', 'Elastic', 'Compute')):
        return False
    # Opaque marketplace codes are lowercase alphanumeric, 20+ chars
    if len(code) >= 20 and code.replace('-', '').replace('_', '').isalnum() and code == code.lower():
        return True
    return False


def get_cost_by_line_item_category(
    csv_text: str | None = None,
    file_path: str | None = None,
    filters: dict | None = None,
) -> dict:
    """Break down costs by line item type category.
    Returns structured dict with: aws_services, marketplace, reserved_instances,
    savings_plans, taxes, credits_discounts, total_gross, total_net.
    """
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        svc_col = _detect_service_col(cols)
        if not cost_col:
            return {}

        # Detect key columns
        type_col = next((c for c in cols if c.lower() in ('line_item_line_item_type', 'lineitem/lineitemtype')), None)
        desc_col = next((c for c in cols if c.lower() in ('line_item_line_item_description', 'lineitem/lineitemdescription')), None)
        acct_col = next((c for c in cols if c.lower() in ('line_item_usage_account_id', 'lineitem/usageaccountid')), None)
        acct_name_col = next((c for c in cols if c.lower() in ('line_item_usage_account_name', 'lineitem/usageaccountname')), None)

        if not type_col or not svc_col:
            return {}

        # Fetch all rows grouped by service + type + description
        rows = con.execute(
            f'SELECT '
            f'  COALESCE(NULLIF(TRIM(CAST("{svc_col}" AS VARCHAR)), \'\'), \'Unallocated\') AS svc, '
            f'  COALESCE(NULLIF(TRIM(CAST("{type_col}" AS VARCHAR)), \'\'), \'Other\') AS item_type, '
            f'  COALESCE(NULLIF(TRIM(CAST("{desc_col}" AS VARCHAR)), \'\'), \'\') AS description, '
            f'  SUM("{cost_col}") AS cost '
            f'FROM cur_data '
            f'GROUP BY svc, item_type, description '
            f'ORDER BY cost DESC'
        ).fetchall()

        aws_services: dict = {}
        marketplace: list = []
        reserved_instances: list = []
        savings_plans: list = []
        taxes: float = 0.0
        credits_discounts: float = 0.0
        total_gross: float = 0.0
        total_net: float = 0.0

        DISCOUNT_TYPES = {'EdpDiscount', 'BundledDiscount', 'SavingsPlanNegation', 'Refund', 'Credit'}
        MP_TYPES = {'Fee', 'Usage'}
        RI_TYPES = {'RIFee'}
        SP_TYPES = {'SavingsPlanRecurringFee', 'SavingsPlanCoveredUsage'}

        for svc, item_type, description, cost in rows:
            cost = float(cost or 0)
            total_net += cost

            if item_type in DISCOUNT_TYPES:
                credits_discounts += cost
                continue

            total_gross += cost

            if item_type == 'Tax':
                taxes += cost
                continue

            if item_type in RI_TYPES:
                reserved_instances.append({
                    "service": svc,
                    "description": description,
                    "cost": round(cost, 4),
                })
                continue

            if item_type in SP_TYPES:
                savings_plans.append({
                    "service": svc,
                    "description": description,
                    "item_type": item_type,
                    "cost": round(cost, 4),
                })
                continue

            # Marketplace detection: opaque code or marketplace keyword in description
            is_mp = _is_marketplace_code(svc) or 'marketplace' in description.lower()
            if is_mp and item_type in MP_TYPES:
                # Extract readable name from description
                readable = description.split('|')[0].strip() if description else svc
                marketplace.append({
                    "product_code": svc,
                    "description": readable,
                    "item_type": item_type,
                    "cost": round(cost, 4),
                })
                continue

            # Regular AWS service
            if svc not in aws_services:
                aws_services[svc] = 0.0
            aws_services[svc] += cost

        # Sort and format aws_services
        aws_services_list = [
            {"service": k, "cost": round(v, 4)}
            for k, v in sorted(aws_services.items(), key=lambda x: -x[1])
        ]

        # Aggregate marketplace by readable description
        mp_agg: dict = {}
        for mp in marketplace:
            key = mp["description"]
            mp_agg[key] = mp_agg.get(key, 0.0) + mp["cost"]
        marketplace_list = [
            {"description": k, "cost": round(v, 4)}
            for k, v in sorted(mp_agg.items(), key=lambda x: -x[1])
        ]

        # Aggregate RI by service
        ri_agg: dict = {}
        for ri in reserved_instances:
            ri_agg[ri["service"]] = ri_agg.get(ri["service"], 0.0) + ri["cost"]
        ri_list = [
            {"service": k, "cost": round(v, 4)}
            for k, v in sorted(ri_agg.items(), key=lambda x: -x[1])
        ]

        # Aggregate SP
        sp_agg: dict = {}
        for sp in savings_plans:
            sp_agg[sp["service"]] = sp_agg.get(sp["service"], 0.0) + sp["cost"]
        sp_list = [
            {"service": k, "cost": round(v, 4)}
            for k, v in sorted(sp_agg.items(), key=lambda x: -x[1])
        ]

        return {
            "aws_services": aws_services_list,
            "marketplace": marketplace_list,
            "reserved_instances": ri_list,
            "savings_plans": sp_list,
            "taxes": round(taxes, 4),
            "credits_discounts": round(credits_discounts, 4),
            "total_gross": round(total_gross, 4),
            "total_net": round(total_net, 4),
        }
    finally:
        con.close()


def _normalise_lifecycle(raw: str) -> str:
    """Normalise inconsistent user_life_cycle tag values to standard categories."""
    if not raw:
        return "Untagged"
    r = raw.strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    if r in ("postgolive", "postgoelive", "postglive", "postgo", "postgoliive",
             "postgoliive", "postgolove", "postgolive", "aftergolive", "prod"):
        return "Post-GoLive"
    if r in ("pregolive", "pregoelive", "preglive", "prgo", "pregoliive", "pre"):
        return "Pre-GoLive"
    if r in ("golive", "live"):
        return "GoLive"
    if r in ("test", "qatest", "qa", "aosqa", "aostestdev", "aosdev", "aosstg",
             "aossup", "aosprod", "shortterm", "automation"):
        return "Test"
    if r in ("alltime", "allattime", "atalltimes", "atalltime", "alltimes",
             "allthetime", "standard", "shared", "internal", "lifecycle",
             "onceinmonth", "prestg"):
        return "Shared/Always-On"
    if r in ("na", "unknown", "operative", "pregoelive", "pregolive"):
        return "Other"
    return "Other"


def get_cost_by_lifecycle(
    csv_text: str | None = None,
    file_path: str | None = None,
    filters: dict | None = None,
) -> list[dict]:
    """Cost breakdown by normalised user_life_cycle tag from resource_tags JSON.
    Returns list of {lifecycle, cost, pct_of_total, raw_values} sorted by cost desc.
    Untagged resources grouped separately."""
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col:
            return []

        # Check if resource_tags column exists
        tags_col = next((c for c in cols if c.lower() in ('resource_tags', 'resourcetags')), None)

        if tags_col:
            rows = con.execute(f"""
                SELECT
                    json_extract_string("{tags_col}", '$.user_life_cycle') as raw_lc,
                    SUM("{cost_col}") as cost
                FROM cur_data
                GROUP BY raw_lc
                ORDER BY cost DESC
            """).fetchall()
        else:
            return []

        # Aggregate by normalised lifecycle
        agg: dict[str, float] = {}
        raw_map: dict[str, list[str]] = {}
        for raw_lc, cost in rows:
            cost = float(cost or 0)
            normalised = _normalise_lifecycle(raw_lc or "")
            agg[normalised] = agg.get(normalised, 0.0) + cost
            if raw_lc:
                raw_map.setdefault(normalised, [])
                if raw_lc not in raw_map[normalised]:
                    raw_map[normalised].append(raw_lc)

        total = sum(agg.values())
        result = [
            {
                "lifecycle": k,
                "cost": round(v, 4),
                "pct_of_total": round(v / total * 100, 2) if total else 0,
                "raw_values": raw_map.get(k, []),
            }
            for k, v in sorted(agg.items(), key=lambda x: -x[1])
        ]
        return result
    finally:
        con.close()


def _normalise_environment(raw: str) -> str:
    """Normalise inconsistent environment tag values to standard categories."""
    if not raw:
        return "Untagged"
    r = raw.strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    if r in ("production", "prod", "prd", "aosprod", "o1prod", "nvaprod",
             "nvaprod2", "aosqaprod", "live") or r.startswith("aosnv") or r.startswith("nvaprod"):
        return "Production"
    if r in ("nonproduction", "nonprod", "nonp", "nonprd"):
        return "Non-Production"
    if r in ("staging", "stg", "stage", "o1stg", "aostg", "aosstg", "prestg",
             "prestaging", "nvastg"):
        return "Staging"
    if r in ("uat", "useracceptance"):
        return "UAT"
    if r in ("qa", "qatest", "aosqa", "aosqastg", "qastg"):
        return "QA"
    if r in ("development", "dev", "aosdev", "develop"):
        return "Development"
    if r in ("dr", "disasterrecovery"):
        return "DR"
    if r in ("performance", "perf", "load"):
        return "Performance"
    if r in ("demo", "training", "sandbox", "conf", "test"):
        return "Demo/Training"
    if r in ("sharedinfrastructure", "shared", "sharedinf", "internal",
             "sharedinfra"):
        return "Shared"
    return "Other"


def get_cost_by_hosting_environment(
    csv_text: str | None = None,
    file_path: str | None = None,
    filters: dict | None = None,
) -> list[dict]:
    """Cost breakdown by normalised environment — uses COALESCE priority:
    user_environment → user_hosting_environment → Untagged.
    Each row contributes to exactly one category (no double counting).
    Untagged included to reconcile with total spend."""
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col:
            return []
        tags_col = next((c for c in cols if c.lower() in ('resource_tags', 'resourcetags')), None)
        if not tags_col:
            return []
        rows = con.execute(f"""
            SELECT
                COALESCE(
                    NULLIF(TRIM(json_extract_string("{tags_col}", '$.user_environment')), ''),
                    NULLIF(TRIM(json_extract_string("{tags_col}", '$.user_hosting_environment')), '')
                ) as raw_env,
                SUM("{cost_col}") as cost
            FROM cur_data
            GROUP BY raw_env
            ORDER BY cost DESC
        """).fetchall()

        agg: dict[str, float] = {}
        for raw_env, cost in rows:
            cost = float(cost or 0)
            normalised = _normalise_environment(raw_env or "")
            agg[normalised] = agg.get(normalised, 0.0) + cost

        total = sum(agg.values())
        result = [
            {
                "environment": k,
                "cost": round(v, 4),
                "pct_of_total": round(v / total * 100, 2) if total else 0,
            }
            for k, v in sorted(agg.items(), key=lambda x: -x[1])
        ]
        return result
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
            f'FROM cur_data '
            f"WHERE \"{date_col}\" IS NOT NULL AND TRIM(CAST(\"{date_col}\" AS VARCHAR)) <> '' "
            f'GROUP BY day ORDER BY day'
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
            if round(c, 4) != 0.0
        ]
    finally:
        con.close()


def _has_inv_lookup(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True when inv_account_lookup was registered on this connection."""
    try:
        con.execute("SELECT 1 FROM inv_account_lookup LIMIT 1")
        return True
    except Exception:
        return False


def get_cost_by_account(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None, enricher=None) -> list[dict]:
    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        acct_col = _detect_account_col(cols)
        if not cost_col or not acct_col:
            return []
        # Check whether the inv_account_lookup table was registered (file-path
        # pipeline with an active enricher). If so, LEFT JOIN to pull inv_customer
        # as the account name and inv_environment as context.
        has_inv = _has_inv_lookup(con)
        name_col = "line_item_usage_account_name" if "line_item_usage_account_name" in cols else None
        if has_inv:
            rows = con.execute(
                f'SELECT c."{acct_col}", '
                f'COALESCE(i.inv_customer, \'\') AS account_name, '
                f'COALESCE(i.inv_environment, \'\') AS environment, '
                f'SUM(c."{cost_col}") AS cost, COUNT(*) AS rc '
                f'FROM cur_data c '
                f'LEFT JOIN inv_account_lookup i ON CAST(c."{acct_col}" AS VARCHAR) = i.account_id '
                f'GROUP BY c."{acct_col}", i.inv_customer, i.inv_environment ORDER BY cost DESC'
            ).fetchall()
            return [
                {
                    "account_id": str(r[0]) if r[0] is not None else "",
                    "account_name": str(r[1]) if r[1] else "",
                    "environment": str(r[2]) if r[2] else "",
                    "cost": round(float(r[3] or 0), 4),
                    "row_count": int(r[4] or 0),
                }
                for r in rows
            ]
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
        logger.exception("get_cost_by_account failed")
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


def get_cost_by_environment(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None, enricher=None) -> list[dict]:
    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        env_col = _resolve_tag_col(df, "tag_Environment")
        has_inv = _has_inv_lookup(con)
        acct_col = _detect_account_col(cols)

        if has_inv and acct_col and not env_col:
            # No native environment tag — derive from inventory JOIN.
            rows = con.execute(
                f'SELECT COALESCE(NULLIF(i.inv_environment, \'\'), \'Untagged\') AS env, '
                f'SUM(c."{cost_col}") AS cost '
                f'FROM cur_data c '
                f'LEFT JOIN inv_account_lookup i ON CAST(c."{acct_col}" AS VARCHAR) = i.account_id '
                f'GROUP BY env ORDER BY cost DESC'
            ).fetchall()
            return [
                {
                    "environment": str(r[0]),
                    "cost": round(float(r[1] or 0), 4),
                    "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
                    "env_owner_team": "",
                    "env_owner_email": "",
                }
                for r in rows
            ]

        if not env_col:
            return []

        has_team = "env_owner_team" in cols
        has_email = "env_owner_email" in cols
        select_extra = ""
        if has_team:
            select_extra += ', MAX("env_owner_team")'
        if has_email:
            select_extra += ', MAX("env_owner_email")'
        rows = con.execute(
            f'SELECT "{env_col}", SUM("{cost_col}") AS cost{select_extra} '
            f'FROM cur_data GROUP BY "{env_col}" ORDER BY cost DESC'
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
    except Exception as _e:
        logger.exception("get_cost_by_environment EXCEPTION: %s", _e)
        return []
    finally:
        con.close()


def get_cost_by_env_month(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None, enricher=None) -> list[dict]:
    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        date_col = _detect_date_col(cols)
        if not cost_col or not date_col:
            return []
        env_col = _resolve_tag_col(df, "tag_Environment")
        has_inv = _has_inv_lookup(con)
        acct_col = _detect_account_col(cols)
        if has_inv and acct_col and not env_col:
            rows = con.execute(
                f"SELECT COALESCE(NULLIF(i.inv_environment, ''), 'Untagged') AS env, "
                f"strftime(CAST(c.\"{date_col}\" AS DATE), '%Y-%m') AS month, "
                f"SUM(c.\"{cost_col}\") AS cost "
                f"FROM cur_data c "
                f"LEFT JOIN inv_account_lookup i ON CAST(c.\"{acct_col}\" AS VARCHAR) = i.account_id "
                f"WHERE c.\"{date_col}\" IS NOT NULL AND TRIM(CAST(c.\"{date_col}\" AS VARCHAR)) <> '' "
                f"GROUP BY env, month ORDER BY month, env"
            ).fetchall()
            return [{"environment": str(r[0]), "month": str(r[1]), "cost": round(float(r[2] or 0), 4)} for r in rows]
        if not env_col:
            return []
        rows = con.execute(
            f"SELECT \"{env_col}\", strftime(CAST(\"{date_col}\" AS DATE), '%Y-%m') AS month, "
            f"SUM(\"{cost_col}\") AS cost "
            f"FROM cur_data "
            f"WHERE \"{date_col}\" IS NOT NULL AND TRIM(CAST(\"{date_col}\" AS VARCHAR)) <> '' "
            f"GROUP BY \"{env_col}\", month ORDER BY month, \"{env_col}\""
        ).fetchall()
        return [{"environment": str(r[0]), "month": str(r[1]), "cost": round(float(r[2] or 0), 4)} for r in rows]
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_env_category(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None, enricher=None) -> list[dict]:
    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        svc_col = _detect_service_col(cols)
        if not cost_col:
            return []
        env_col = _resolve_tag_col(df, "tag_Environment")
        has_inv = _has_inv_lookup(con)
        acct_col = _detect_account_col(cols)
        if has_inv and acct_col and not env_col:
            rows = con.execute(
                f"SELECT COALESCE(NULLIF(i.inv_environment, ''), 'Untagged') AS env, "
                f"COALESCE(NULLIF(TRIM(CAST(c.\"{svc_col}\" AS VARCHAR)), ''), 'Unallocated') AS svc, "
                f"COALESCE(c.line_item_line_item_type, '') AS item_type, "
                f"SUM(c.\"{cost_col}\") AS cost "
                f"FROM cur_data c "
                f"LEFT JOIN inv_account_lookup i ON CAST(c.\"{acct_col}\" AS VARCHAR) = i.account_id "
                f"GROUP BY env, svc, item_type"
            ).fetchall() if svc_col else []
            result: dict[str, dict[str, float]] = {}
            for r in rows:
                env = str(r[0])
                svc_name = str(r[1] or "")
                item_type = str(r[2] or "")
                cost = float(r[3] or 0)
                if item_type in _CREDIT_LINE_ITEM_TYPES:
                    cat = "Credits / Refunds"
                elif svc_name == "Unallocated":
                    cat = "Unallocated"
                else:
                    cat = _categorise_service(svc_name)
                result.setdefault(env, {})
                result[env][cat] = result[env].get(cat, 0.0) + cost
            return [{"environment": env, "category": cat, "cost": round(cost, 4)} for env, cats in result.items() for cat, cost in sorted(cats.items(), key=lambda kv: kv[1], reverse=True)]
        if not env_col or not svc_col:
            return []
        if "service_category" in cols:
            rows = con.execute(
                f"SELECT \"{env_col}\", COALESCE(NULLIF(service_category,''),'Unallocated') AS cat, "
                f"SUM(\"{cost_col}\") AS cost "
                f"FROM cur_data GROUP BY \"{env_col}\", cat ORDER BY \"{env_col}\", cost DESC"
            ).fetchall()
            return [{"environment": str(r[0]), "category": str(r[1]), "cost": round(float(r[2] or 0), 4)} for r in rows]
        rows = con.execute(
            f"SELECT \"{env_col}\", COALESCE(NULLIF(TRIM(CAST(\"{svc_col}\" AS VARCHAR)),''),'Unallocated') AS svc, "
            f"COALESCE(line_item_line_item_type, '') AS item_type, "
            f"SUM(\"{cost_col}\") AS cost FROM cur_data GROUP BY \"{env_col}\", svc, item_type"
        ).fetchall()
        result = {}
        for r in rows:
            env = str(r[0])
            svc_name = str(r[1] or "")
            item_type = str(r[2] or "")
            cost = float(r[3] or 0)
            if item_type in _CREDIT_LINE_ITEM_TYPES:
                cat = "Credits / Refunds"
            elif svc_name == "Unallocated":
                cat = "Unallocated"
            else:
                cat = _categorise_service(svc_name)
            result.setdefault(env, {})
            result[env][cat] = result[env].get(cat, 0.0) + cost
        return [{"environment": env, "category": cat, "cost": round(cost, 4)} for env, cats in result.items() for cat, cost in sorted(cats.items(), key=lambda kv: kv[1], reverse=True)]
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


def get_cost_by_tag(csv_text: str | None = None, tag_col: str = "", file_path: str | None = None, filters: dict | None = None, enricher=None, native_only: bool = False) -> list[dict]:
    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        actual_tag = _resolve_tag_col(df, tag_col)
        logger.info("get_cost_by_tag: requested %r -> resolved %r", tag_col, actual_tag)
        has_inv = _has_inv_lookup(con)
        acct_col = _detect_account_col(cols)

        # Mapping from requested tag names to inventory fallback columns.
        _TAG_TO_INV = {
            "tag_Customer":    "inv_customer",
            "tag_Product":     "inv_application",
            "tag_CostCentre":  "inv_budget_code",
            "tag_Team":        "inv_managed_by",  # inventory ManagedBy as team fallback
        }
        inv_fallback = _TAG_TO_INV.get(tag_col)

        if not cost_col:
            return []

        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)

        if has_inv and acct_col and not actual_tag and inv_fallback and not native_only:
            # No native tag column — derive from inventory JOIN.
            rows = con.execute(
                f'SELECT COALESCE(NULLIF(i.{inv_fallback}, \'\'), \'Untagged\') AS tag_val, '
                f'SUM(c."{cost_col}") AS cost '
                f'FROM cur_data c '
                f'LEFT JOIN inv_account_lookup i ON CAST(c."{acct_col}" AS VARCHAR) = i.account_id '
                f'GROUP BY tag_val ORDER BY cost DESC'
            ).fetchall()
            return [
                {
                    "tag_value": str(r[0]),
                    "cost": round(float(r[1] or 0), 4),
                    "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
                }
                for r in rows
            ]

        if not actual_tag:
            # For CUR 2.0 with native_only, try reading from JSON tags column
            if native_only and "tags" in cols and cost_col:
                try:
                    rows = con.execute(
                        f'SELECT regexp_extract(tags, \'"resourceTags/user:Team":"([^"]+)"\', 1) AS team_val, '
                        f'SUM("{cost_col}") AS cost '
                        f'FROM cur_data '
                        f'WHERE tags LIKE \'%user:Team%\' '
                        f'GROUP BY team_val ORDER BY cost DESC'
                    ).fetchall()
                    return [
                        {
                            "tag_value": str(r[0]) if r[0] else "Untagged",
                            "cost": round(float(r[1] or 0), 4),
                            "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
                        }
                        for r in rows if r[0]
                    ]
                except Exception:
                    pass
            return []

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
        logger.exception("get_cost_by_tag failed")
        return []
    finally:
        con.close()


def get_untagged_resources(csv_text: str | None = None, file_path: str | None = None, filters: dict | None = None, enricher=None) -> dict:
    df, con = _load_df(csv_text, file_path=file_path, filters=filters)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        total_rows = _row_count(df, con)
        # Detect tag columns across tag_ and resourceTags/user: prefixes.
        tag_cols = detect_tag_columns(df)
        logger.info("get_untagged_resources: detected tag columns %s", tag_cols)

        # CUR 2.0 Athena export: tags stored as JSON blob in `tags` column.
        # No per-tag columns exist, so detect coverage from the JSON column directly.
        has_tags_col = "tags" in cols
        if not tag_cols and has_tags_col:
            # Count rows with at least one user: tag in the JSON blob
            aws_tagged = int(con.execute(
                "SELECT COUNT(*) FROM cur_data WHERE tags LIKE '%user:%'"
            ).fetchone()[0] or 0)
            aws_tag_pct = round(aws_tagged / total_rows * 100, 1) if total_rows else 0.0
            untagged_in_aws = total_rows - aws_tagged
            untagged_pct = round(untagged_in_aws / total_rows * 100, 1) if total_rows else 0.0

            # Cost for untagged rows
            untagged_cost = 0.0
            if cost_col:
                untagged_cost = float(con.execute(
                    f'SELECT SUM("{cost_col}") FROM cur_data WHERE tags NOT LIKE \'%user:%\' OR tags IS NULL'
                ).fetchone()[0] or 0)

            # Inventory covers the gap when enricher is active
            inv_active = enricher is not None and getattr(enricher, "active", False)
            inv_pct = untagged_pct if inv_active else 0.0
            overall = round(aws_tag_pct + inv_pct, 1)

            coverage = [
                {
                    "tag": "AWS Resource Tags",
                    "coverage_pct": aws_tag_pct,
                    "untagged_count": untagged_in_aws,
                    "untagged_cost": round(untagged_cost, 4),
                },
            ]
            if inv_active and untagged_in_aws > 0:
                coverage.append({
                    "tag": "Inventory Enrichment",
                    "coverage_pct": inv_pct,
                    "untagged_count": 0,
                    "untagged_cost": 0.0,
                })
            return {
                "total_rows": total_rows,
                "tag_coverage": coverage,
                "overall_coverage_pct": min(overall, 100.0),
                "has_tag_columns": False,
                "aws_tag_pct": aws_tag_pct,
                "inventory_pct": inv_pct,
            }

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
            "has_tag_columns": True,
        }
    except Exception:
        logger.exception("get_untagged_resources failed")
        return {"total_rows": 0, "tag_coverage": [], "overall_coverage_pct": 0.0, "has_tag_columns": False}
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
            f'SELECT '
            f'CASE WHEN "pricing_term" IS NULL OR TRIM("pricing_term") = \'\' '
            f'THEN \'Credits / Discounts\' ELSE "pricing_term" END AS term, '
            f'SUM("{cost_col}") AS cost, COUNT(*) AS rc '
            f'FROM cur_data GROUP BY term ORDER BY cost DESC'
        ).fetchall()
        return [
            {
                "pricing_term": str(r[0]),
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
            f"COALESCE(NULLIF(TRIM(CAST(\"{svc_col}\" AS VARCHAR)), ''), 'Unallocated') AS service, SUM(\"{cost_col}\") AS cost "
            f'FROM cur_data '
            f"WHERE \"{date_col}\" IS NOT NULL AND TRIM(CAST(\"{date_col}\" AS VARCHAR)) <> '' "
            f'GROUP BY month, service ORDER BY month, cost DESC'
        ).fetchall()
        return [
            {
                "month": str(r[0]),
                "service": str(r[1]),
                "cost": round(float(r[2] or 0), 4),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_top_resources(csv_text: str | None = None, limit: int = 10, file_path: str | None = None, filters: dict | None = None, enricher=None) -> list[dict]:
    df, con = _load_df(csv_text, enricher=enricher, file_path=file_path, filters=filters)
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
        svc_sel = f'MAX(c."{svc_col}")' if svc_col else "''"
        region_sel = f'MAX(c."{region_col}")' if region_col else "''"
        has_res_inv = _has_resource_lookup(con)
        acct_col = _detect_account_col(cols)
        if has_res_inv and not env_col:
            env_sel = "MAX(COALESCE(NULLIF(r.inv_environment,''), a.inv_environment))"
        elif env_col:
            env_sel = f'MAX(c."{env_col}")'
        else:
            env_sel = "''"
        if has_res_inv and not team_col:
            team_sel = "MAX(COALESCE(NULLIF(r.inv_managed_by,''), a.inv_managed_by))"
        elif team_col:
            team_sel = f'MAX(c."{team_col}")'
        else:
            team_sel = "''"
        if has_res_inv and acct_col:
            join_clause = (
                f'LEFT JOIN inv_resource_lookup r ON '
                f'SPLIT_PART(CAST(c."{res_col}" AS VARCHAR), \':\', -1) = r.resource_id '
                f'LEFT JOIN inv_account_lookup a ON CAST(c."{acct_col}" AS VARCHAR) = a.account_id '
            )
        else:
            join_clause = ""
        rows = con.execute(
            f'SELECT c."{res_col}", {svc_sel}, {region_sel}, {env_sel}, {team_sel}, '
            f'SUM(c."{cost_col}") AS cost '
            f'FROM cur_data c {join_clause}'
            f"WHERE c.\"{res_col}\" IS NOT NULL AND CAST(c.\"{res_col}\" AS VARCHAR) <> '' "
            f'GROUP BY c."{res_col}" ORDER BY cost DESC LIMIT {limit}'
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
        elif "tags" in cols:
            # CUR 2.0 — tags stored as JSON string; use LIKE to detect ownership tags
            try:
                untagged_cost = float(con.execute(
                    f'SELECT SUM("{cost_col}") FROM cur_data '
                    f"WHERE tags IS NULL OR tags = '{{}}' OR ("
                    f"  tags NOT LIKE '%user:Owner%' "
                    f"  AND tags NOT LIKE '%user:Team%' "
                    f"  AND tags NOT LIKE '%user:ManagedBy%'"
                    f")"
                ).fetchone()[0] or 0)
            except Exception:
                pass
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
        # File-path mode returns a columns-carrier, not a DataFrame, and
        # enrichment is not applied there yet (Step 3). The cost-weighted and
        # unmatched-resource computations below need the real enriched rows, so
        # report enrichment as not-yet-available rather than operating on a
        # carrier that has no data.
        if not isinstance(df, pd.DataFrame):
            return {
                "active": True,
                "joinable": False,
                "reason": "enrichment summary not available in file-path mode yet",
            }
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
        "'cost_by_region' — spend breakdown by AWS region; "
        "'cost_by_environment' — spend breakdown by environment (via inventory enrichment); "
        "'cost_by_account' — spend breakdown by AWS account; "
        "'cost_by_tag' — spend breakdown by Product/Team/Customer/CostCentre tags."
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
                "enum": ["total_cost", "cost_by_service", "daily_trend", "cost_by_region", "cost_by_environment", "cost_by_account", "cost_by_tag"],
                "description": "Which analysis to run.",
            },
        },
        "required": ["session_id", "query_type"],
    }

    def __init__(self, cache: dict[str, str], report_map: dict[str, int] | None = None) -> None:
        self._cache = cache  # session_id → raw CSV text
        self._report_map = report_map if report_map is not None else {}

    async def execute(self, session_id: str, query_type: QueryType) -> str:  # type: ignore[override]
        csv_text = self._cache.get(session_id)
        file_path = None
        if not csv_text:
            report_id = self._report_map.get(session_id)
            if report_id:
                from report_store import get_report_path
                file_path = get_report_path(report_id)
        if not csv_text and not file_path:
            return json.dumps({"error": "No CUR data loaded for this session."})

        if query_type == "total_cost":
            return json.dumps(get_total_cost(csv_text, file_path=file_path))
        if query_type == "cost_by_service":
            return json.dumps(get_cost_by_service(csv_text, file_path=file_path))
        if query_type == "daily_trend":
            return json.dumps(get_daily_trend(csv_text, file_path=file_path))
        if query_type == "cost_by_region":
            return json.dumps(get_cost_by_region(csv_text, file_path=file_path))
        if query_type == "cost_by_environment":
            enricher = await build_enricher(get_registry())
            return json.dumps(get_cost_by_environment(csv_text, file_path=file_path, filters=None, enricher=enricher), default=str)
        if query_type == "cost_by_account":
            enricher = await build_enricher(get_registry())
            return json.dumps(get_cost_by_account(csv_text, file_path=file_path, filters=None, enricher=enricher), default=str)
        if query_type == "cost_by_tag":
            enricher = await build_enricher(get_registry())
            return json.dumps({
                "tag_product": get_cost_by_tag(csv_text, "tag_Product", file_path=file_path, filters=None, enricher=enricher),
                "tag_team": get_cost_by_tag(csv_text, "tag_Team", file_path=file_path, filters=None, enricher=enricher),
                "tag_customer": get_cost_by_tag(csv_text, "tag_Customer", file_path=file_path, filters=None, enricher=enricher),
                "tag_costcentre": get_cost_by_tag(csv_text, "tag_CostCentre", file_path=file_path, filters=None, enricher=enricher),
            }, default=str)
        return json.dumps({"error": f"Unknown query_type '{query_type}'."})
