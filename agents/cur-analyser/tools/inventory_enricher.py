"""Inventory enrichment for CUR data.

The enricher joins CUR cost rows to a resource inventory (loaded via the
data-source registry) on ``(account_id, resource_id)`` and surfaces inventory
attributes as ``inv_*`` virtual columns. The ``inv_`` prefix guarantees these
never collide with native CUR ``tag_*`` columns.

Crucially, when **no inventory is loaded** the enricher is inert: ``active`` is
``False``, ``enrich_dataframe`` returns the DataFrame untouched (no new
columns), and ``enrich_query_result`` returns rows unchanged. This is what keeps
the existing CUR flow byte-for-byte identical when the feature is off.

Matching is tolerant of CUR's varied ``resource_id`` formats (bare ids, ARNs):
candidate ids are derived from ARN tails and matched within the same account
first, then used to classify *why* a row failed to match.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# inv_* output column  ->  inventory enrichment field captured by the provider.
INV_FIELD_MAP: dict[str, str] = {
    "inv_customer": "Customer",
    "inv_application": "Application",
    "inv_environment": "Environment",
    "inv_budget_code": "Budget_Code",
    "inv_grade": "Grade",
    "inv_layer": "Layer",
    "inv_managed_by": "ManagedBy",
    "inv_project": "Project",
}
INV_COLUMNS: list[str] = list(INV_FIELD_MAP.keys())

# Match-failure reason codes.
REASON_MATCHED = "matched"
REASON_NOT_IN_INVENTORY = "Not in inventory"
REASON_ACCOUNT_MISMATCH = "Account mismatch"
REASON_FORMAT_MISMATCH = "Resource ID format mismatch"


def _arn_candidates(resource_id: str) -> list[str]:
    """Derive candidate resource ids from a CUR resource_id / ARN.

    e.g. ``arn:aws:ec2:us-east-1:111:instance/i-0abc`` -> ``i-0abc``;
    also yields the last ``:``-delimited segment.
    """
    rid = (resource_id or "").strip()
    if not rid:
        return []
    cands = [rid]
    if "/" in rid:
        tail = rid.rsplit("/", 1)[-1]
        if tail:
            cands.append(tail)
    if ":" in rid:
        tail = rid.rsplit(":", 1)[-1]
        # strip any path component on the colon-tail too
        if "/" in tail:
            tail = tail.rsplit("/", 1)[-1]
        if tail:
            cands.append(tail)
    # de-dup, preserve order
    seen: set[str] = set()
    out: list[str] = []
    for c in cands:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


class InventoryEnricher:
    """Enriches CUR rows/frames with inventory attributes.

    Construct with the enrichment lookup from an inventory provider
    (``{(account_id, resource_id): {fields}}``), or ``None`` for the inert
    pass-through case.
    """

    def __init__(self, lookup: Optional[dict[tuple[str, str], dict[str, Any]]] = None) -> None:
        self._lookup: dict[tuple[str, str], dict[str, Any]] = lookup or {}
        # resource_key -> set of accounts that contain it (for mismatch detection)
        self._by_resource: dict[str, set[str]] = {}
        self._accounts: set[str] = set()
        for (acct, res), _entry in self._lookup.items():
            self._by_resource.setdefault(res, set()).add(acct)
            self._accounts.add(acct)
        self._reset_stats()

    @property
    def active(self) -> bool:
        """True only when an inventory lookup is present. False => pass-through."""
        return bool(self._lookup)

    def _reset_stats(self) -> None:
        self._matched = 0
        self._unmatched = 0
        # service -> {"in_cur", "matched", "unmatched"}
        self._per_service: dict[str, dict[str, int]] = {}
        # Per-row diagnostics from the most recent enrich_dataframe pass,
        # aligned to DataFrame row order (used by get_enrichment_summary).
        self._last_matched: list[bool] = []
        self._last_reason: list[str] = []

    # ── matching ─────────────────────────────────────────────────────────────

    def match(self, account: str, resource_id: str) -> tuple[Optional[dict[str, Any]], str]:
        """Return ``(entry_or_None, reason)`` for a single CUR row."""
        if not self.active:
            return (None, REASON_NOT_IN_INVENTORY)
        account = str(account or "").strip()
        candidates = _arn_candidates(str(resource_id or ""))
        if not candidates:
            return (None, REASON_NOT_IN_INVENTORY)

        # 1) exact match within the same account
        for c in candidates:
            entry = self._lookup.get((account, c))
            if entry is not None:
                return (entry, REASON_MATCHED)

        # 2) resource exists, but under a different account
        for c in candidates:
            accts = self._by_resource.get(c)
            if accts:
                return (None, REASON_ACCOUNT_MISMATCH)

        # 3) account is known to inventory but no id form lined up, and the
        #    resource_id looks structured (ARN-ish) => format mismatch
        rid = str(resource_id or "")
        if account in self._accounts and ("/" in rid or ":" in rid):
            return (None, REASON_FORMAT_MISMATCH)

        return (None, REASON_NOT_IN_INVENTORY)

    # ── DataFrame enrichment ───────────────────────────────────────────────────

    def enrich_dataframe(self, df, *, account_col: Optional[str] = None,
                         resource_col: Optional[str] = None,
                         service_col: Optional[str] = None):
        """Add ``inv_*`` columns to ``df`` in place and return it.

        When inactive, or when the required join columns are absent, ``df`` is
        returned completely unchanged (no new columns) — preserving the existing
        flow exactly.
        """
        if not self.active:
            return df
        try:
            cols = list(df.columns)
            account_col = account_col or self._detect_account_col(cols)
            resource_col = resource_col or self._detect_resource_col(cols)
            service_col = service_col or self._detect_service_col(cols)
            if not account_col or not resource_col:
                try:
                    from tools.duckdb_engine import (
                        ACCOUNT_COL_CANDIDATES,
                        RESOURCE_COL_CANDIDATES,
                    )
                    acct_cands, res_cands = ACCOUNT_COL_CANDIDATES, RESOURCE_COL_CANDIDATES
                except Exception:
                    acct_cands, res_cands = [], []
                logger.warning(
                    "InventoryEnricher: cannot enrich — account_id column %s, "
                    "resource_id column %s. Tried account candidates %s and "
                    "resource candidates %s against CUR columns %s.",
                    f"found ('{account_col}')" if account_col else "NOT FOUND",
                    f"found ('{resource_col}')" if resource_col else "NOT FOUND",
                    acct_cands, res_cands, cols,
                )
                return df

            self._reset_stats()
            # Build inv_* column values row-by-row.
            inv_values: dict[str, list] = {c: [] for c in INV_COLUMNS}
            accounts = df[account_col].astype(str).tolist()
            resources = df[resource_col].astype(str).tolist()
            services = (
                df[service_col].astype(str).tolist() if service_col else [""] * len(df)
            )
            for acct, res, svc in zip(accounts, resources, services):
                entry, reason = self.match(acct, res)
                svc_stats = self._per_service.setdefault(
                    svc, {"in_cur": 0, "matched": 0, "unmatched": 0}
                )
                svc_stats["in_cur"] += 1
                self._last_matched.append(entry is not None)
                self._last_reason.append(reason)
                if entry is not None:
                    self._matched += 1
                    svc_stats["matched"] += 1
                    for col, field in INV_FIELD_MAP.items():
                        inv_values[col].append(entry.get(field, ""))
                else:
                    self._unmatched += 1
                    svc_stats["unmatched"] += 1
                    for col in INV_COLUMNS:
                        inv_values[col].append("")

            for col in INV_COLUMNS:
                df[col] = inv_values[col]
            logger.info(
                "InventoryEnricher: enriched %d rows (matched=%d, unmatched=%d)",
                len(df), self._matched, self._unmatched,
            )
            return df
        except Exception:
            logger.exception("InventoryEnricher.enrich_dataframe failed — returning df unchanged")
            return df

    # ── Query-result enrichment ────────────────────────────────────────────────

    def enrich_query_result(self, rows: list[dict], account_col: str,
                            resource_col: str) -> list[dict]:
        """Enrich a list of query-result dicts in place, adding ``inv_*`` keys."""
        if not self.active:
            return rows
        self._reset_stats()
        for row in rows:
            acct = str(row.get(account_col, ""))
            res = str(row.get(resource_col, ""))
            entry, _reason = self.match(acct, res)
            if entry is not None:
                self._matched += 1
                for col, field in INV_FIELD_MAP.items():
                    row[col] = entry.get(field, "")
            else:
                self._unmatched += 1
                for col in INV_COLUMNS:
                    row[col] = ""
        return rows

    # ── Stats ──────────────────────────────────────────────────────────────────

    def get_match_stats(self) -> dict[str, Any]:
        """Match-rate summary from the most recent enrichment pass."""
        total = self._matched + self._unmatched
        rate = round(self._matched / total * 100, 1) if total else 0.0
        per_service = []
        for svc, s in sorted(
            self._per_service.items(), key=lambda kv: kv[1]["in_cur"], reverse=True
        ):
            in_cur = s["in_cur"]
            per_service.append({
                "service": svc or "(unknown)",
                "in_cur": in_cur,
                "matched": s["matched"],
                "unmatched": s["unmatched"],
                "match_rate_pct": round(s["matched"] / in_cur * 100, 1) if in_cur else 0.0,
            })
        return {
            "matched_count": self._matched,
            "unmatched_count": self._unmatched,
            "match_rate_pct": rate,
            "per_service": per_service,
        }

    # ── Column detection (delegates to the engine helpers) ──────────────────────

    @staticmethod
    def _detect_account_col(cols: list[str]) -> Optional[str]:
        try:
            from tools.duckdb_engine import _detect_account_col
            return _detect_account_col(cols)
        except Exception:
            return None

    @staticmethod
    def _detect_resource_col(cols: list[str]) -> Optional[str]:
        try:
            from tools.duckdb_engine import _detect_resource_col
            return _detect_resource_col(cols)
        except Exception:
            return None

    @staticmethod
    def _detect_service_col(cols: list[str]) -> Optional[str]:
        try:
            from tools.duckdb_engine import _detect_service_col
            return _detect_service_col(cols)
        except Exception:
            return None


async def build_enricher(registry) -> InventoryEnricher:
    """Build an enricher from the registry's current inventory provider.

    Returns an inert (pass-through) enricher when no inventory is loaded or the
    feature flag is disabled, so callers never need to special-case the off
    state.
    """
    from config import settings

    if not settings.enable_inventory_enrichment:
        return InventoryEnricher(None)
    if registry is None:
        return InventoryEnricher(None)
    try:
        provider = await registry.get_inventory_provider()
        if provider is None:
            return InventoryEnricher(None)
        lookup = await provider.fetch()
        return InventoryEnricher(lookup)
    except Exception:
        logger.exception("build_enricher failed — returning inert enricher")
        return InventoryEnricher(None)
