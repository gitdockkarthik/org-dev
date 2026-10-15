#!/usr/bin/env python3
"""Batch test of the cluster alert rules, end to end on both channels (or
one, with --channels): for each rule type, a TEMPORARY copy named
"BATCHTEST - <type> - <cluster name>" with a TEST Teams webhook is created
with both channels off, then switched to the channels under test in one
change, forced to fire (tiers set so the live value is clearly beyond
critical), checked, relaxed so it resolves, checked again and deleted. The
real rules are never edited. See README_batch_test_rules.md beside this file.

Runs inside the agent container, like the default-rules script: init_storage,
then the routes_alerts functions for every rule change. The three settings
go through the LIVE agent's GET/POST /settings (--agent-url) because the
evaluator reads its own in-memory copy of them; a write from this process
straight to the database would not reach it, and a restore would not either.

  dry run (the default; reads only, prints the plan):
    docker exec -it <container> python scripts/batch_test_rules.py --to you@sintecmedia.com
  real run (asks for confirmation at every card and email):
    docker exec -it <container> python scripts/batch_test_rules.py --to you@sintecmedia.com --run
  leftovers only (dry run, then for real):
    docker exec -it <container> python scripts/batch_test_rules.py --cleanup [--restore-settings] [--run]

Safety (enforced here, covered by tests/test_batch_test_rules.py): only rules
named "BATCHTEST - ..." are ever created, edited or deleted, never ids 3 or 5;
a backup of the whole rules table and the three settings is written first;
the run refuses to start while any other rule has email on or a webhook, or
email_recipients is set; email_enabled and email_recipients are restored and
every BATCHTEST rule removed in a finally block (also on Ctrl+C, SIGTERM and
any exception), followed by a printed verification. The test webhook is read
from a file, never printed or logged (shown as "<test webhook>"), stored only
in the temporary rules, and the file is removed at the end of a --run."""
import argparse
import asyncio
import json
import math
import os
import re
import signal
import socket
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

PREFIX = "BATCHTEST - "
PROTECTED_IDS = frozenset({3, 5})
ALLOWED_DOMAINS = ("@operative.com", "@sintecmedia.com")
WEBHOOK_MASK = "<test webhook>"
DEFAULT_WEBHOOK_FILE = "/run/batch_webhook"
DEFAULT_BACKUP_DIR = "/home/backups/manual"
DEFAULT_AGENT_URL = "http://localhost:8003"
SETTINGS_KEYS = ("email_enabled", "email_recipients", "teams_enabled")
# --channels: the copy's Teams (enabled) and Email (email_enabled) toggles.
CHANNELS = {
    "both": {"enabled": True, "email_enabled": True},
    "email": {"enabled": False, "email_enabled": True},
    "teams": {"enabled": True, "email_enabled": False},
}
TEAMS_OFF_RULE = "Teams off for this rule"  # teams_alerts._TEAMS_OFF_RULE
EVALUATOR_JOB_ID = "kafka-alert-evaluator"

POLL_SECS = 30
DEFAULT_TIMEOUT_MIN = 20
SLO_TIMEOUT_MIN = 90  # two distinct hourly readings: up to about 65-80 min
OUTBOX_WAIT_SECS = 90  # the email is sent after the card; its outbox row follows
# Staging clusters first, production last; other enabled clusters after these.
CLUSTER_ORDER = (8, 10, 4, 9)
# A lag copy fires for ONE chosen name only: a custom row with low tiers, and
# generic tiers no real lag reaches.
LAG_GENERIC_OFF = 10 ** 15

EASY_TYPES = (
    "broker.cpu_pct", "broker.heap_pct", "cluster.urp_total", "cluster.rf_below_min", "cluster.leader_skew",
    "cluster.data_gb_max", "cluster.data_spread_pct", "cluster.zk_ensemble",
    "cluster.connect_failed_connectors", "cluster.connect_failed_tasks", "cluster.connect_workers_down",
    "cluster.sr_nodes_down", "cluster.sr_soft_deleted", "cluster.msg_rate_in",
)
SLOW_TYPES = ("cluster.slo_compliance",)
RISKY_TYPES = ("cluster.consumer_lag", "cluster.connector_lag")
ALL_TYPES = EASY_TYPES + SLOW_TYPES + RISKY_TYPES
BELOW_TYPES = ("cluster.msg_rate_in", "cluster.slo_compliance")
WHOLE_TYPES = (
    "cluster.connect_failed_connectors", "cluster.connect_failed_tasks", "cluster.connect_workers_down",
    "cluster.sr_nodes_down", "cluster.sr_soft_deleted", "cluster.consumer_lag", "cluster.connector_lag",
    # Counts the API takes as decimals; whole tiers keep "clearly beyond" exact.
    "cluster.urp_total", "cluster.rf_below_min", "cluster.zk_ensemble",
)
WARN_SLOW_RISKY = (
    "WARNING: consumer lag and connector lag rules fire for REAL lag (clusters 8, 9 and 10 have\n"
    "groups far above any tier today); the copy is limited to ONE chosen name, but the card and\n"
    "email describe a real group or connector. SLO compliance needs two distinct hourly readings\n"
    "(the next hour's row is written at :05, :20 for cluster 10): allow up to about 80 minutes."
)


class SafetyError(Exception):
    """A refusal: printed as REFUSED, nothing further is changed."""


class AbortRun(Exception):
    """The operator's input ended (stdin closed)."""


# ── Masking: the test webhook never reaches any output ───────────────────────

class _Masker:
    def __init__(self) -> None:
        self.secrets: list[str] = []

    def add(self, secret: str) -> None:
        if secret and secret not in self.secrets:
            self.secrets.append(secret)

    def __call__(self, text) -> str:
        text = str(text)
        for secret in self.secrets:
            text = text.replace(secret, WEBHOOK_MASK)
        return text


MASK = _Masker()


def say(*parts) -> None:
    print(MASK(" ".join(str(p) for p in parts)), flush=True)


class _MaskFilter:
    """logging filter: renders the message, masks it, drops the args."""

    def filter(self, record) -> bool:
        try:
            record.msg = MASK(record.getMessage())
            record.args = ()
            if record.exc_info:
                record.msg += "\n" + MASK("".join(traceback.format_exception(*record.exc_info)))
                record.exc_info = None
                record.exc_text = None
        except Exception:  # noqa: BLE001 -- a log line must never break the run
            record.msg, record.args = "(log line dropped: could not be masked)", ()
        return True


def _setup_logging() -> None:
    import logging
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    handler.addFilter(_MaskFilter())
    root = logging.getLogger()
    for old in list(root.handlers):
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(logging.WARNING)


def _masked_excepthook(exc_type, exc, tb) -> None:
    sys.stderr.write(MASK("".join(traceback.format_exception(exc_type, exc, tb))))


# ── Inputs ───────────────────────────────────────────────────────────────────

def load_webhook(path: str) -> str:
    """The test webhook from its file; refuses when missing or malformed.
    Never echoes the content."""
    try:
        with open(path, encoding="utf-8") as fh:
            hook = fh.read().strip()
    except FileNotFoundError:
        raise SafetyError(f"test webhook file {path} is missing (set BATCH_TEST_WEBHOOK_FILE or create it; see the README)")
    except OSError as exc:
        raise SafetyError(f"test webhook file {path} cannot be read ({type(exc).__name__})")
    if not hook:
        raise SafetyError(f"test webhook file {path} is empty")
    MASK.add(hook)
    if not hook.startswith("https://") or any(c.isspace() for c in hook) or len(hook) > 2048:
        raise SafetyError(f"test webhook file {path} does not hold a single https:// URL")
    return hook


_ADDRESS_RE = re.compile(r"[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+")


def check_to(address: str | None) -> str:
    """--to: exactly one address at @operative.com or @sintecmedia.com."""
    if not address:
        raise SafetyError("--to is required: one address at @operative.com or @sintecmedia.com")
    address = address.strip()
    if not _ADDRESS_RE.fullmatch(address):
        raise SafetyError(f"--to must be exactly one email address, got {address!r}")
    if not address.lower().endswith(ALLOWED_DOMAINS):
        raise SafetyError(f"--to must be at {' or '.join(ALLOWED_DOMAINS)}, got {address!r}")
    return address


def parse_only(value: str | None) -> list[str] | None:
    if not value:
        return None
    types = [t.strip() for t in value.split(",") if t.strip()]
    unknown = [t for t in types if t not in ALL_TYPES]
    if unknown:
        raise SafetyError(f"--only: unknown type(s) {', '.join(unknown)}; known: {', '.join(ALL_TYPES)}")
    return list(dict.fromkeys(types))


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="print the plan, change nothing (the default)")
    p.add_argument("--run", action="store_true", help="required to change anything")
    p.add_argument("--only", help="comma-separated alert types (needed for SLO and lag types)")
    p.add_argument("--cluster", type=int, help="test on this cluster id only")
    p.add_argument("--cleanup", action="store_true", help="remove leftover BATCHTEST rules only")
    p.add_argument("--restore-settings", action="store_true",
                   help="with --cleanup: put email_enabled and email_recipients back from the newest backup")
    p.add_argument("--to", help="the one address emails go to during the run")
    p.add_argument("--channels", choices=sorted(CHANNELS), default="both",
                   help="the copy's channels: both (default), email only (no Teams post expected) or teams only")
    p.add_argument("--escalate", action="store_true",
                   help="fire at warning first, then escalate to critical (where the value allows two tiers)")
    p.add_argument("--timeout-minutes", type=int, help=f"per-rule wait limit (default {DEFAULT_TIMEOUT_MIN}; SLO {SLO_TIMEOUT_MIN})")
    p.add_argument("--agent-url", default=os.environ.get("BATCH_TEST_AGENT_URL", DEFAULT_AGENT_URL))
    p.add_argument("--log-file", help="a copy of the container log to search for the 'rule email sent' lines")
    args = p.parse_args(argv)
    if args.run and args.dry_run:
        p.error("--run and --dry-run together")
    if args.restore_settings and not args.cleanup:
        p.error("--restore-settings works with --cleanup only")
    return args


# ── The live agent's settings ────────────────────────────────────────────────

class AgentSettings:
    """GET/POST /settings on the live agent: the only way to change what its
    evaluator reads. Keeps only the keys this script needs."""

    def __init__(self, base_url: str, transport=None) -> None:
        import httpx
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport, timeout=20)

    async def read(self, hook: str | None = None) -> dict:
        resp = await self._client.get("/settings")
        resp.raise_for_status()
        data = resp.json()
        out = {k: data.get(k) for k in SETTINGS_KEYS}
        # Only whether the agent-level webhook IS the test webhook; never its value.
        out["_agent_webhook_is_test"] = bool(hook) and str(data.get("teams_webhook_url") or "").strip() == hook
        return out

    async def write(self, values: dict) -> None:
        values = {k: v for k, v in values.items() if k in ("email_enabled", "email_recipients")}
        resp = await self._client.post("/settings", json=values)
        resp.raise_for_status()
        if not (resp.json() or {}).get("ok"):
            raise RuntimeError("POST /settings did not answer ok")

    async def close(self) -> None:
        await self._client.aclose()


# ── Deps (injected by the tests) ─────────────────────────────────────────────

def _stdin_line(prompt: str) -> "asyncio.Future":
    """Read one line in a daemon thread, so a signal never waits on it and
    the process can exit while it is blocked."""
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def _read() -> None:
        try:
            sys.stdout.write(MASK(prompt))
            sys.stdout.flush()
            line = sys.stdin.readline()
            result = line if line else None
        except Exception:  # noqa: BLE001
            result = None
        loop.call_soon_threadsafe(lambda: fut.done() or fut.set_result(result))

    threading.Thread(target=_read, daemon=True).start()
    return fut


async def _ask(prompt: str) -> str:
    line = await _stdin_line(prompt)
    if line is None:
        raise AbortRun("operator input closed")
    return line.rstrip("\n")


@dataclass
class Deps:
    settings: AgentSettings
    ask: object = _ask                 # async (prompt) -> str
    sleep: object = asyncio.sleep      # async (seconds) -> None
    interactive_check: bool = True     # refuse --run without a terminal


# ── Database helpers ─────────────────────────────────────────────────────────

def _session():
    from database import SessionLocal
    if SessionLocal is None:
        raise SafetyError("DATABASE_URL is not set: no database")
    return SessionLocal()


async def _rows(sql: str, params: dict | None = None) -> list:
    from sqlalchemy import text
    async with _session() as session:
        return (await session.execute(text(sql), params or {})).all()


async def snapshot_rules() -> list[dict]:
    """Every kafka_alert_configs row, all columns, as JSON-ready dicts."""
    rows = await _rows("SELECT row_to_json(c)::text FROM kafka_alert_configs c ORDER BY id")
    return [json.loads(r[0]) for r in rows]


def is_batch(rule: dict) -> bool:
    return str(rule.get("name") or "").startswith(PREFIX)


async def guard_rule(rule_id: int) -> dict:
    """The rule row, or SafetyError unless it may be touched: a BATCHTEST name
    and never ids 3 or 5. Called before every edit and delete."""
    if isinstance(rule_id, bool) or not isinstance(rule_id, int):
        raise SafetyError(f"refusing rule id {rule_id!r}: not an integer id")
    if rule_id in PROTECTED_IDS:
        raise SafetyError(f"refusing rule id {rule_id}: ids 3 and 5 are never touched")
    rows = await _rows("SELECT id, name FROM kafka_alert_configs WHERE id = :id", {"id": rule_id})
    if not rows:
        raise SafetyError(f"refusing rule id {rule_id}: not found")
    name = rows[0].name or ""
    if not name.startswith(PREFIX):
        raise SafetyError(f"refusing rule id {rule_id} ({name!r}): its name does not start with {PREFIX!r}")
    return {"id": rule_id, "name": name}


def _detail(exc: Exception) -> str:
    return str(getattr(exc, "detail", None) or exc)


async def create_rule(body: dict) -> int:
    """routes_alerts.create_alert_config with its one-rule-per-type-per-cluster
    check skipped for this call only: a BATCHTEST copy must sit next to the
    disabled default rule of the same type. Every other API validation runs."""
    import routes_alerts as RA
    from fastapi import HTTPException
    if not str(body.get("name") or "").startswith(PREFIX):
        raise SafetyError(f"refusing to create {body.get('name')!r}: the name must start with {PREFIX!r}")
    original = RA._check_no_duplicate

    async def _skip_duplicate_check(*_args, **_kwargs) -> None:
        return None

    RA._check_no_duplicate = _skip_duplicate_check
    try:
        result = await RA.create_alert_config(RA.AlertConfigPayload(**body))
    except HTTPException as exc:
        raise RuntimeError(f"create refused by the API ({exc.status_code}): {_detail(exc)}")
    finally:
        RA._check_no_duplicate = original
    return int(result["config"]["id"])


async def update_rule(rule_id: int, body: dict) -> None:
    import routes_alerts as RA
    from fastapi import HTTPException
    await guard_rule(rule_id)
    if "name" in body and not str(body["name"] or "").startswith(PREFIX):
        raise SafetyError(f"refusing to rename rule {rule_id} without the {PREFIX!r} prefix")
    try:
        await RA.update_alert_config(rule_id, RA.AlertConfigUpdatePayload(**body))
    except HTTPException as exc:
        raise RuntimeError(f"update of rule {rule_id} refused by the API ({exc.status_code}): {_detail(exc)}")


async def remove_rule(rule_id: int) -> None:
    """Disable, then delete (guarded twice)."""
    import routes_alerts as RA
    await update_rule(rule_id, {"enabled": False, "email_enabled": False})
    await guard_rule(rule_id)
    await RA.delete_alert_config(rule_id)


async def batch_rule_ids() -> list[int]:
    rows = await _rows("SELECT id FROM kafka_alert_configs WHERE name LIKE :p ORDER BY id", {"p": "BATCHTEST - %"})
    return [r.id for r in rows]


async def cluster_names(enabled_only: bool = True) -> dict[int, str]:
    from config import settings
    from storage import get_backend
    clusters = await get_backend().get_clusters(settings.agent_slug)
    return {int(c["id"]): c.get("name", str(c["id"])) for c in clusters
            if c.get("id") is not None and (c.get("enabled") or not enabled_only)}


async def evaluator_schedule() -> dict:
    rows = await _rows("SELECT cron_expression, enabled FROM kafka_job_schedules WHERE job_id = :j ORDER BY id DESC LIMIT 1",
                       {"j": EVALUATOR_JOB_ID})
    if not rows:
        return {"cron": None, "enabled": False, "minutes": None}
    cron = rows[0].cron_expression
    m = re.fullmatch(r"\*/(\d+) \* \* \* \*", (cron or "").strip())
    return {"cron": cron, "enabled": bool(rows[0].enabled), "minutes": int(m.group(1)) if m else None}


async def last_evaluator_run() -> str:
    # kafka_job_runs has no index beyond its primary key: look at recent ids only.
    rows = await _rows(
        "SELECT started_at, status FROM kafka_job_runs WHERE job_id = :j "
        "AND id > (SELECT COALESCE(MAX(id), 0) - 20000 FROM kafka_job_runs) ORDER BY id DESC LIMIT 1",
        {"j": EVALUATOR_JOB_ID},
    )
    if not rows:
        return "no recent run found"
    return f"{_t(rows[0].started_at)} ({rows[0].status})"


async def pending_rule_notices() -> int:
    rows = await _rows("SELECT COUNT(*) AS n FROM kafka_notification_outbox WHERE routing = 'rule' AND finished_at IS NULL")
    return int(rows[0].n)


def _t(ts) -> str:
    if ts is None:
        return "-"
    if isinstance(ts, str):
        return ts
    return ts.astimezone(timezone.utc).strftime("%H:%M:%S UTC")


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Safety checks ────────────────────────────────────────────────────────────

def rule_problems(rules: list[dict]) -> list[str]:
    problems = []
    for r in rules:
        if is_batch(r):
            continue
        if r.get("email_enabled"):
            problems.append(f"rule {r['id']} ({r.get('name')}) has email_enabled on")
        if r.get("webhook_url"):
            problems.append(f"rule {r['id']} ({r.get('name')}) has a webhook_url set")
    return problems


def settings_problems(saved: dict, channels: str = "both") -> list[str]:
    problems = []
    if str(saved.get("email_recipients") or "").strip():
        problems.append("setting email_recipients is not empty (rule emails would reach those addresses too)")
    # The evaluators run when either global switch is on, and the run turns
    # email_enabled on itself; only a run that expects cards needs Teams on.
    if not saved.get("teams_enabled") and CHANNELS[channels]["enabled"]:
        problems.append("setting teams_enabled is off: no Teams card would be posted (use --channels email)")
    if saved.get("_agent_webhook_is_test"):
        problems.append("the test webhook is the agent-level teams_webhook_url (the real channel): use a test channel")
    return problems


# ── Live values: the evaluator's own queries and guards ──────────────────────

@dataclass
class Reading:
    value: float | int | None = None
    detail: str = ""
    subject: str | None = None   # lag types: the chosen group or connector
    ignored: str | None = None   # the evaluator would ignore the cluster: why
    min_rf: int | None = None


async def read_value(alert_type: str, cid: int) -> Reading:
    """The value the evaluator would compute for one rule of this type on this
    cluster, with its guards, using teams_alerts' own SQL and probe helpers."""
    import httpx
    import teams_alerts as TA
    from sqlalchemy import select
    from models import KafkaBrokerMetrics

    now = _now()
    async with _session() as session:
        async def q(stmt, params):
            return (await session.execute(stmt, params)).all()

        if alert_type in TA.BROKER_THRESHOLD_METRICS:
            column, _label = TA.BROKER_THRESHOLD_METRICS[alert_type]
            rows = (await session.execute(select(
                KafkaBrokerMetrics.broker_id, KafkaBrokerMetrics.cpu_pct, KafkaBrokerMetrics.heap_pct,
                KafkaBrokerMetrics.time,
            ).where(KafkaBrokerMetrics.cluster_id == cid))).all()
            vals = [
                (float(getattr(r, column)), str(r.broker_id)) for r in rows
                if r.time is not None and now - r.time <= TA._BROKER_METRICS_MAX_AGE
                and not ((r.cpu_pct or 0.0) == 0.0 and (r.heap_pct or 0.0) == 0.0)
                and getattr(r, column) is not None
            ]
            if not vals:
                return Reading(ignored="no fresh broker metrics rows (6 min)")
            value, broker = max(vals)
            return Reading(round(value, 1), f"highest of {len(vals)} brokers (broker {broker}); one card per broker at or above the tier")

        if alert_type in ("cluster.urp_total", "cluster.rf_below_min"):
            fresh = await q(TA._CLUSTER_STRUCTURE_FRESH_SQL, {
                "job_ids": [f"{TA._CLUSTER_STRUCTURE_JOB_PREFIX}{cid}"], "since": now - TA._CLUSTER_STRUCTURE_MAX_AGE,
            })
            if not fresh:
                return Reading(ignored="no successful topic-structure run in 15 min")
            if alert_type == "cluster.urp_total":
                rows = await q(TA._URP_TOTAL_SQL, {"cids": [cid]})
                return Reading(int(rows[0].value) if rows else 0, "under-replicated partitions")
            count = count3 = int((await q(TA._RF_BELOW_MIN_SQL, {"cid": cid, "min_rf": 3}))[0][0])
            if count >= 2:  # enough for a tier clearly under the count
                return Reading(count, "topics below RF 3", min_rf=3)
            from sqlalchemy import text
            max_rf = (await q(text(
                "SELECT MAX(replication_factor) FROM kafka_topic_metrics WHERE cluster_id = :cid AND partition_count > 0 "
                r"AND topic NOT LIKE '\_\_%' AND topic <> '_schemas'"), {"cid": cid}))[0][0]
            if max_rf is None:
                return Reading(0, "no topics", min_rf=3)
            min_rf = max(2, int(max_rf) + 1)
            count = int((await q(TA._RF_BELOW_MIN_SQL, {"cid": cid, "min_rf": min_rf}))[0][0])
            return Reading(count, f"topics below RF {min_rf} (min_rf raised: {'none' if not count3 else 'one'} below 3)", min_rf=min_rf)

        if alert_type == TA._LEADER_SKEW_ALERT_TYPE:
            rows = await q(TA._LEADER_SKEW_SQL, {"cids": [cid]})
            if not rows or rows[0].last_updated is None or now - rows[0].last_updated > TA._LEADER_SKEW_MAX_AGE:
                return Reading(ignored="no broker distribution update in 15 min")
            r = rows[0]
            if r.brokers < 2 or float(r.avg_leaders or 0) <= 0:
                return Reading(ignored="fewer than 2 brokers or no leaders")
            return Reading(round(float(r.max_leaders) / float(r.avg_leaders), 2), "busiest broker / average leaders")

        if alert_type == TA._MSG_RATE_IN_ALERT_TYPE:
            latest = [r for r in await q(TA._MSG_RATE_LATEST_SQL, {"cids": [cid], "since": now - TA._MSG_RATE_MAX_AGE})
                      if r.collected_at is not None]
            if not latest:
                return Reading(ignored="no inflow run in 15 min")
            run_at = latest[0].collected_at
            for r in await q(TA._MSG_RATE_RUN_SQL, {"cids": [cid], "runs": [run_at]}):
                interval = float(r.interval_seconds) if r.interval_seconds is not None else None
                if r.collected_at != run_at or interval is None or interval <= 0 or interval > TA._MSG_RATE_MAX_INTERVAL_SECS:
                    continue
                return Reading(round(float(r.msgs or 0) / interval, 2), f"msgs/sec, run {_t(run_at)}")
            return Reading(ignored="latest inflow run has no valid interval")

        if alert_type == TA._SLO_COMPLIANCE_ALERT_TYPE:
            rows = await q(TA._SLO_LATEST_SQL, {"cids": [cid]})
            if not rows or rows[0].hour_bucket is None:
                return Reading(ignored="no kafka_slo_compliance rows")
            r = rows[0]
            if now - r.hour_bucket > TA._SLO_MAX_AGE:
                return Reading(ignored="latest hour_bucket older than 3 h")
            if r.overall_compliance_pct is None:
                return Reading(ignored="overall_compliance_pct is NULL")
            return Reading(round(float(r.overall_compliance_pct), 1), f"% for hour_bucket {_t(r.hour_bucket)}")

        if alert_type in TA._DATA_GB_ALERT_TYPES:
            rows = sorted(await q(TA._DATA_GB_SQL, {"cids": [cid]}), key=lambda r: str(r.broker_id))
            if len(rows) < 2:
                return Reading(ignored="fewer than 2 broker metrics rows")
            if any(r.data_gb_true is None for r in rows):
                return Reading(ignored="a broker has no data_gb_true")
            if any(r.time is None or now - r.time > TA._DATA_GB_MAX_AGE for r in rows):
                return Reading(ignored="a broker has no metrics in the last 6 min")
            hi = max(rows, key=lambda r: r.data_gb_true)
            lo = min(rows, key=lambda r: r.data_gb_true)
            if alert_type == TA._DATA_GB_MAX_ALERT_TYPE:
                return Reading(round(float(hi.data_gb_true), 1), f"GB on broker {hi.broker_id}")
            if float(hi.data_gb_true) <= 0:
                return Reading(ignored="largest broker holds 0 GB")
            spread = round((float(hi.data_gb_true) - float(lo.data_gb_true)) / float(hi.data_gb_true) * 100, 1)
            return Reading(spread, f"% spread, broker {lo.broker_id} vs {hi.broker_id}")

        if alert_type == TA._ZK_ENSEMBLE_ALERT_TYPE:
            rows = await q(TA._ZK_URL_SQL, {"cids": [cid]})
            nodes = TA._zk_nodes(rows[0].zookeeper_url if rows else None)
            if not rows:
                return Reading(ignored="cluster not found")
            if len(nodes) < 2:
                return Reading(ignored="zookeeper_url empty or lists one node")
            answers = dict(zip(nodes, await asyncio.gather(*(TA._zk_probe(n) for n in nodes))))
            value, _desc = TA._zk_ensemble_value(nodes, answers)
            return Reading(value, f"problems across {len(nodes)} ZooKeeper nodes (probed live)")

        if alert_type in TA._CONNECT_SNAPSHOT_ALERT_TYPES or alert_type == TA._CONNECT_WORKERS_ALERT_TYPE:
            rows = await q(TA._CONNECT_URL_SQL, {"cids": [cid]})
            workers = TA._connect_workers(rows[0].kafka_connect_url if rows else None)
            if not workers:
                return Reading(ignored="kafka_connect_url is empty")
            if alert_type == TA._CONNECT_WORKERS_ALERT_TYPE:
                limit = asyncio.Semaphore(TA._CONNECT_PROBE_CONCURRENCY)
                async with httpx.AsyncClient(timeout=TA._CONNECT_PROBE_TIMEOUT_SECS) as client:
                    up = dict(zip(workers, await asyncio.gather(*(TA._connect_probe(client, w, limit) for w in workers))))
                value, _desc = TA._connect_workers_value(workers, up)
                return Reading(value, f"of {len(workers)} Connect workers down (probed live)")
            snap = await q(TA._CONNECT_SNAPSHOT_SQL, {"cids": [cid], "since": now - TA._CONNECT_SNAPSHOT_WINDOW})
            if not snap:
                return Reading(ignored="no connector snapshot rows in 15 min")
            if now - snap[0].collected_at > TA._CONNECT_SNAPSHOT_MAX_AGE:
                return Reading(ignored="latest connector snapshot older than 6 min")
            if snap[0].previous_rows is not None and len(snap) * 2 < snap[0].previous_rows:
                return Reading(ignored="latest connector snapshot looks partial")
            if alert_type == TA._CONNECT_FAILED_CONNECTORS_ALERT_TYPE:
                return Reading(sum(1 for r in snap if r.state == "FAILED"), f"FAILED of {len(snap)} connectors")
            return Reading(sum(r.failed_tasks or 0 for r in snap if (r.failed_tasks or 0) > 0), "failed tasks")

        if alert_type in TA._SR_ALERT_TYPES:
            rows = await q(TA._SR_URL_SQL, {"cids": [cid]})
            nodes = TA._sr_nodes(rows[0].schema_registry_url if rows else None)
            if not nodes:
                return Reading(ignored="schema_registry_url is empty")
            limit = asyncio.Semaphore(TA._SR_PROBE_CONCURRENCY)
            async with httpx.AsyncClient(timeout=TA._SR_PROBE_TIMEOUT_SECS) as client:
                up = dict(zip(nodes, await asyncio.gather(*(TA._sr_probe(client, n, limit) for n in nodes))))
            if alert_type == TA._SR_NODES_ALERT_TYPE:
                value, _desc = TA._sr_nodes_value(nodes, up)
                return Reading(value, f"of {len(nodes)} Schema Registry nodes down (probed live)")
            first_up = next((n for n in nodes if up[n]), None)
            if first_up is None:
                return Reading(ignored="no Schema Registry node answered")
            async with httpx.AsyncClient(timeout=TA._SR_SUBJECTS_TIMEOUT_SECS) as client:
                value, detail = await TA._sr_soft_deleted_value(client, first_up)
            if value is None:
                return Reading(ignored=detail)
            return Reading(value, "soft-deleted subjects (read live)")

        if alert_type in TA.CLUSTER_LAG_TYPES:
            since = now - TA._LAG_MAX_AGE
            groups = {r.group_id: int(r.total_lag or 0) for r in await q(TA._LAG_FRESH_SQL, {"cids": [cid], "since": since})}
            if alert_type == TA._CONSUMER_LAG_ALERT_TYPE:
                names = groups
                noun = "consumer group"
            else:
                snap = await q(TA._CONNECT_SNAPSHOT_SQL, {"cids": [cid], "since": since})
                if not snap:
                    return Reading(ignored="no connector snapshot in 20 min")
                if snap[0].previous_rows is not None and len(snap) * 2 < snap[0].previous_rows:
                    return Reading(ignored="latest connector snapshot looks partial")
                names = {r.connector_name: groups[f"connect-{r.connector_name}"] for r in snap
                         if f"connect-{r.connector_name}" in groups}
                noun = "connector"
            if not names:
                return Reading(ignored=f"no fresh {noun} rows in 20 min")
            name, lag = max(names.items(), key=lambda kv: (kv[1], kv[0]))
            above = sum(1 for v in names.values() if v > 0)
            return Reading(lag, f"lag of {noun} {name!r} (largest of {len(names)}; {above} above 0)", subject=name)

    raise SafetyError(f"no reader for {alert_type}")


# ── Tiers ────────────────────────────────────────────────────────────────────

def _num(x: float) -> float | int:
    x = round(float(x), 4)
    return int(x) if x == int(x) else x


@dataclass
class Tiers:
    fire: dict
    resolve: dict
    escalate: dict | None = None   # applied after a warning fire (--escalate)
    fire_severity: str = "critical"


def plan_tiers(alert_type: str, value, escalate: bool = False) -> tuple[Tiers | None, str | None]:
    """(tiers, None), or (None, why it cannot be forced). Fire tiers put the
    value clearly beyond critical (for below types: clearly under it); resolve
    tiers put it clearly back inside. All pass the API's validation."""
    if value is None:
        return None, "no value"
    v = float(value)
    whole = alert_type in WHOLE_TYPES
    if alert_type == "cluster.slo_compliance":
        # strictly below; tiers 0..100, descending
        if v >= 100:
            return None, f"value {value}%: cannot be under a tier (tiers are at most 100, strict)"
        t = Tiers(fire={"critical": 100}, resolve={"critical": 0})
        if escalate:
            t.fire, t.fire_severity = {"warning": 100, "critical": 0}, "warning"
            t.escalate = {"warning": 100, "critical": _num(min(99.99, (v + 100) / 2))}
        return t, None
    if alert_type == "cluster.msg_rate_in":
        # at or below; tiers >= 0, descending
        if v <= 0:
            return None, f"value {value}: 0 msgs/sec cannot resolve (0 is at or below every tier)"
        high, low = _num(v * 10 + 10), _num(v / 10)
        t = Tiers(fire={"critical": high}, resolve={"critical": low})
        if escalate:
            t.fire, t.fire_severity = {"warning": high, "critical": low}, "warning"
            t.escalate = {"warning": _num(v * 20 + 20), "critical": high}
        return t, None
    # above-direction types: the value is at or above a tier
    if whole:
        if v < 1:
            return None, f"value {value}: cannot be at or above a tier (tiers must be > 0)"
        c = max(1, int(v) // 2)
        r = int(v) * 10 + 10
        t = Tiers(fire={"critical": c}, resolve={"critical": r})
        if escalate and c >= 2:
            t.fire, t.fire_severity = {"warning": c, "critical": r}, "warning"
            t.escalate = {"warning": max(1, c // 2), "critical": c}
        return t, None
    if v <= 0:
        return None, f"value {value}: cannot be forced above a tier (tiers must be > 0)"
    c, r = _num(v / 2), _num(v * 10 + 10)
    if c <= 0:
        return None, f"value {value}: too small to set a tier under it"
    t = Tiers(fire={"critical": c}, resolve={"critical": r})
    w = _num(v / 4)
    if escalate and 0 < w < c:
        t.fire, t.fire_severity = {"warning": c, "critical": r}, "warning"
        t.escalate = {"warning": w, "critical": c}
    return t, None


def tier_body(alert_type: str, tiers: dict, reading: Reading) -> dict:
    """The rule fields carrying these tiers: lag types put them on a custom row
    for the one chosen name and keep the generic tiers out of reach."""
    if alert_type in RISKY_TYPES:
        return {"tiers": {"critical": LAG_GENERIC_OFF}, "custom": [{"name": reading.subject, "tiers": tiers}], "blacklist": []}
    return {"tiers": tiers}


# ── Plan ─────────────────────────────────────────────────────────────────────

@dataclass
class PlanItem:
    alert_type: str
    cluster_id: int | None = None
    cluster_name: str = "-"
    reading: Reading | None = None
    tiers: Tiers | None = None
    skip: str | None = None
    others: list = field(default_factory=list)


async def build_plan(types: list[str], names: dict[int, str], only_cluster: int | None, escalate: bool) -> list[PlanItem]:
    order = [only_cluster] if only_cluster is not None else (
        [c for c in CLUSTER_ORDER if c in names] + sorted(c for c in names if c not in CLUSTER_ORDER))
    plan = []
    for alert_type in types:
        item = PlanItem(alert_type)
        reasons = []
        for cid in order:
            try:
                reading = await read_value(alert_type, cid)
            except SafetyError:
                raise
            except Exception as exc:  # noqa: BLE001 -- a failed read is a skip, not a crash
                reading = Reading(ignored=f"read failed ({type(exc).__name__}: {exc})")
            if reading.ignored:
                reasons.append(f"{cid}: ignored by the evaluator ({reading.ignored})")
                continue
            tiers, why = plan_tiers(alert_type, reading.value, escalate)
            if tiers is None:
                reasons.append(f"{cid}: {why}")
                continue
            item.cluster_id, item.cluster_name, item.reading, item.tiers = cid, names[cid], reading, tiers
            break  # the first forceable cluster in CLUSTER_ORDER
        if item.tiers is None:
            item.skip = "; ".join(reasons) or "no enabled cluster"
        else:
            item.others = reasons
        plan.append(item)
    return plan


def expected_wait(alert_type: str, minutes: int | None) -> str:
    m = minutes or 5
    if alert_type == "cluster.slo_compliance":
        return "fire at the first evaluator run after the next hourly row (up to ~80 min)"
    if alert_type == "cluster.msg_rate_in":
        return f"fire on the 2nd new inflow run (~{m}-{2 * m + 5} min)"
    return f"fire on the 2nd evaluator run (~{m}-{2 * m + 1} min), resolve on the next (<= {m + 1} min)"


def print_plan(plan: list[PlanItem], minutes: int | None, escalate: bool) -> None:
    say("")
    say("PLAN (one rule at a time, in this order)")
    for i, item in enumerate(plan, 1):
        if item.skip:
            say(f"  {i:>2}. {item.alert_type:<36} SKIP: {item.skip}")
            continue
        r, t = item.reading, item.tiers
        say(f"  {i:>2}. {item.alert_type:<36} cluster {item.cluster_id} {item.cluster_name}")
        say(f"      value now: {r.value} {r.detail}" + (f"; min_rf {r.min_rf}" if r.min_rf else ""))
        say(f"      fire tiers: {json.dumps(t.fire)} -> expect {t.fire_severity}"
            + (f"; escalate tiers: {json.dumps(t.escalate)} -> critical" if t.escalate else
               ("; escalation n/a for this value" if escalate else "")))
        say(f"      resolve tiers: {json.dumps(t.resolve)}; {expected_wait(item.alert_type, minutes)}")
        if item.alert_type in RISKY_TYPES:
            say(f"      lag copy: custom row for {r.subject!r} only; generic tiers {LAG_GENERIC_OFF} (no other name fires)")
        if item.others:
            say(f"      other clusters: {'; '.join(item.others)}")


# ── Evidence ─────────────────────────────────────────────────────────────────

async def triggers(rule_id: int) -> list:
    return await _rows(
        "SELECT id, subject, severity, triggered_at, resolved_at, metric_value, teams_post_success, error_detail "
        "FROM kafka_alert_triggers WHERE alert_config_id = :id ORDER BY id", {"id": rule_id})


async def outbox_row(rule_id: int, kind: str, since: datetime):
    """The newest outbox record of this rule's fire / escalate / resolve notice
    (kind rule_<kind>) since the given time, or None."""
    rows = await _rows(
        "SELECT id, email_status, teams_status, email_last_error, teams_last_error, created_at "
        "FROM kafka_notification_outbox WHERE alert_config_id = :id AND kind = :kind AND created_at >= :since "
        "ORDER BY id DESC LIMIT 1", {"id": rule_id, "kind": f"rule_{kind}", "since": since})
    return rows[0] if rows else None


def log_line(path: str | None, rule_id: int, kind: str) -> str:
    """'sent', 'FAILED', 'not found' or 'not checked' for the notifier's
    "rule email sent: rule_id=<id> kind=<kind>" line."""
    if not path:
        return "not checked"
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except OSError as exc:
        return f"not readable ({type(exc).__name__})"
    if f"rule email sent: rule_id={rule_id} kind={kind} " in text:
        return "sent"
    if f"rule email failed: rule_id={rule_id} kind={kind}" in text:
        return "FAILED"
    return "not found"


def _host_log_hint(rule_id: int, kind: str, since: datetime) -> str:
    container = os.environ.get("HOSTNAME") or socket.gethostname()
    return (f"sudo docker logs --since {since.strftime('%Y-%m-%dT%H:%M:%SZ')} {container} 2>&1 "
            f"| grep 'rule_id={rule_id} kind={kind} '")


@dataclass
class Result:
    alert_type: str
    cluster: str
    fire: str = "-"
    fire_teams: str = "-"
    fire_email: str = "-"
    fire_log: str = "-"
    fire_ok: str = "-"
    escalate: str = "-"
    resolve: str = "-"
    resolve_teams: str = "-"
    resolve_email: str = "-"
    resolve_log: str = "-"
    resolve_ok: str = "-"
    seconds: int = 0
    notes: list = field(default_factory=list)
    rule_id: int | None = None


# ── The run ──────────────────────────────────────────────────────────────────

@dataclass
class Ctx:
    args: object
    deps: Deps
    hook: str | None = None
    to: str | None = None
    saved_settings: dict | None = None
    settings_touched: bool = False
    saved_rules: list | None = None
    backup_path: str | None = None
    results: list = field(default_factory=list)
    eval_minutes: int | None = None
    cleaning: bool = False
    changes_allowed: bool = False


async def write_backup(ctx: Ctx, rules: list[dict]) -> str:
    """The whole rules table and the three settings, chmod 600. Webhook URLs
    of the BATCHTEST copies are left out; the test webhook nowhere."""
    backup_dir = os.environ.get("BATCH_TEST_BACKUP_DIR", DEFAULT_BACKUP_DIR)
    os.makedirs(backup_dir, mode=0o700, exist_ok=True)
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(backup_dir, f"rules-before-batch-{stamp}.json")
    out_rules = []
    for r in rules:
        r = dict(r)
        if is_batch(r) and r.get("webhook_url"):
            r["webhook_url"] = None
            r["webhook_url_excluded"] = True
        out_rules.append(r)
    doc = {
        "created_at": _now().isoformat(),
        "purpose": "state before scripts/batch_test_rules.py; restore with --cleanup --restore-settings --run",
        "settings": {k: ctx.saved_settings.get(k) for k in SETTINGS_KEYS} if ctx.saved_settings else None,
        "kafka_alert_configs": out_rules,
    }
    text = MASK(json.dumps(doc, indent=1, sort_keys=True, default=str))
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, text.encode("utf-8"))
    finally:
        os.close(fd)
    return path


async def remove_leftovers(ctx: Ctx, label: str) -> int:
    ids = await batch_rule_ids()
    for rule_id in ids:
        if not ctx.changes_allowed:
            say(f"  would remove {label} BATCHTEST rule {rule_id}")
            continue
        try:
            await remove_rule(rule_id)
            say(f"  removed {label} BATCHTEST rule {rule_id}")
        except SafetyError as exc:
            say(f"  NOT removed: {exc}")
    return len(ids)


async def ask_choice(ctx: Ctx, question: str) -> str:
    while True:
        answer = (await ctx.deps.ask(f"  >> {question} [y = seen / n = not seen / s = skip the rest of this rule]: ")).strip().lower()
        if answer in ("y", "n", "s"):
            return answer
        say("     please answer y, n or s")


async def wait_for(ctx: Ctx, label: str, check, timeout_secs: int):
    """Poll check() every POLL_SECS until it returns something truthy or the
    limit passes (None)."""
    start = time.monotonic()
    while True:
        found = await check()
        if found:
            return found
        elapsed = time.monotonic() - start
        if elapsed >= timeout_secs:
            return None
        say(f"     waiting for {label}: {int(elapsed // 60)}m{int(elapsed % 60):02d}s "
            f"(evaluator last ran {await last_evaluator_run()})")
        await ctx.deps.sleep(POLL_SECS)


async def wait_outbox(ctx: Ctx, rule_id: int, kind: str, since: datetime):
    start = time.monotonic()
    while True:
        row = await outbox_row(rule_id, kind, since)
        if row is not None or time.monotonic() - start >= OUTBOX_WAIT_SECS:
            return row
        await ctx.deps.sleep(10)


def _trigger_line(t) -> str:
    return (f"trigger {t.id} subject={t.subject} severity={t.severity} triggered {_t(t.triggered_at)} "
            f"resolved {_t(t.resolved_at)} value={t.metric_value} teams_post_success={t.teams_post_success}"
            + (f" error={t.error_detail}" if t.error_detail else ""))


async def report_notice(ctx: Ctx, res: Result, rule_id: int, kind: str, since: datetime, prefix: str) -> None:
    row = await wait_outbox(ctx, rule_id, kind, since)
    if row is None:
        email, teams = "no outbox row", "no outbox row"
        say(f"     outbox: no rule_{kind} row within {OUTBOX_WAIT_SECS} s")
    else:
        email, teams = row.email_status, row.teams_status
        say(f"     outbox row {row.id} at {_t(row.created_at)}: email {row.email_status}"
            + (f" ({row.email_last_error})" if row.email_last_error else "")
            + f", teams {row.teams_status}" + (f" ({row.teams_last_error})" if row.teams_last_error else ""))
    log = log_line(ctx.args.log_file, rule_id, kind)
    say(f"     'rule email sent' log line: {log}")
    if log == "not checked":
        say(f"     check it on the host: {_host_log_hint(rule_id, kind, since)}")
    setattr(res, f"{prefix}_email", email)
    setattr(res, f"{prefix}_log", log)
    if prefix == "resolve":
        res.resolve_teams = teams


async def test_rule(ctx: Ctx, item: PlanItem) -> Result:
    """One rule type end to end. The copy is deleted before the next rule on
    every normal path (done, skipped, timeout, error); an interrupt or a
    refusal propagates and finish() removes it."""
    res = Result(item.alert_type, f"{item.cluster_id} {item.cluster_name}" if item.cluster_id else "-")
    start = time.monotonic()
    if item.skip:
        res.notes.append(f"SKIP: {item.skip}")
        return res
    say("")
    say(f"=== {item.alert_type} on cluster {item.cluster_id} {item.cluster_name} ===")
    try:
        await exercise(ctx, item, res, start)
    except SafetyError:
        raise
    except Exception as exc:  # noqa: BLE001 -- one rule's failure must not stop the others
        res.notes.append(f"ERROR {type(exc).__name__}: {MASK(exc)}")
        say(f"  ERROR: {type(exc).__name__}: {exc}")
    if res.rule_id is not None:
        try:
            await remove_rule(res.rule_id)
            say(f"  rule {res.rule_id} disabled and deleted")
        except Exception as exc:  # noqa: BLE001
            res.notes.append(f"delete failed ({type(exc).__name__}); the cleanup retries")
    res.seconds = int(time.monotonic() - start)
    return res


async def exercise(ctx: Ctx, item: PlanItem, res: Result, start: float) -> None:
    timeout = 60 * (ctx.args.timeout_minutes or (SLO_TIMEOUT_MIN if item.alert_type in SLOW_TYPES else DEFAULT_TIMEOUT_MIN))
    # Safety again before every rule: someone may have changed a rule meanwhile.
    problems = rule_problems(await snapshot_rules())
    if problems:
        raise SafetyError("; ".join(problems))
    reading = await read_value(item.alert_type, item.cluster_id)
    if reading.ignored:
        res.notes.append(f"SKIP: ignored by the evaluator now ({reading.ignored})")
        return
    tiers, why = plan_tiers(item.alert_type, reading.value, ctx.args.escalate)
    if tiers is None:
        res.notes.append(f"SKIP: {why}")
        return
    if item.alert_type in RISKY_TYPES and reading.subject != (item.reading.subject if item.reading else None):
        res.notes.append(f"name changed since the plan: now {reading.subject!r}")
    say(f"  value now: {reading.value} {reading.detail}")
    body = {
        "name": f"{PREFIX}{item.alert_type} - {item.cluster_name}",
        "alert_type": item.alert_type, "cluster_id": item.cluster_id, "severity": "warning",
        "webhook_url": ctx.hook, "cooldown_minutes": 0, "enabled": False, "email_enabled": False,
        "send_resolve_card": True, **tier_body(item.alert_type, tiers.fire, reading),
    }
    if item.alert_type == "cluster.rf_below_min":
        body["min_rf"] = reading.min_rf
    rule_id = res.rule_id = await create_rule(body)
    say(f"  created rule {rule_id} {body['name']!r} (Teams and Email off; webhook {WEBHOOK_MASK}; resolve card on; "
        f"cooldown 0) with tiers {json.dumps(tiers.fire)}")
    channels = CHANNELS[ctx.args.channels]
    await update_rule(rule_id, channels)  # both toggles in one change: the rule cannot run half set up
    enabled_at = _now()
    say(f"  enabled at {_t(enabled_at)} (Teams {'on' if channels['enabled'] else 'off'}, "
        f"Email {'on' if channels['email_enabled'] else 'off'}); {expected_wait(item.alert_type, ctx.eval_minutes)}")

    async def fired():
        rows = await triggers(rule_id)
        open_rows = [t for t in rows if t.resolved_at is None and t.severity == tiers.fire_severity]
        return open_rows or None

    found = await wait_for(ctx, f"the {tiers.fire_severity} fire", fired, timeout)
    if not found:
        res.fire = "TIMEOUT"
        res.notes.append(f"no {tiers.fire_severity} trigger within {timeout // 60} min")
        return
    res.fire = f"{tiers.fire_severity} x{len(found)}"
    if channels["enabled"]:
        res.fire_teams = "ok" if all(t.teams_post_success for t in found) else "FAILED"
    elif all(t.teams_post_success is False and t.error_detail == TEAMS_OFF_RULE for t in found):
        res.fire_teams = "off"
    else:
        res.fire_teams = "WRONG"
        res.notes.append(f"Teams is off for this copy, but a trigger was not saved as {TEAMS_OFF_RULE!r}")
    say(f"  FIRED at {_t(found[0].triggered_at)} ({int(time.monotonic() - start)} s since start)")
    for t in found:
        say(f"     {_trigger_line(t)}")
    await report_notice(ctx, res, rule_id, "fire", enabled_at, "fire")
    check_email_only(ctx, res, "fire")
    answer = await ask_choice(ctx, expect_question(ctx, "Card", "email"))
    res.fire_ok = answer
    if answer == "s":
        res.notes.append("operator skipped after the fire")
        return

    if tiers.escalate:
        esc_from = _now()
        await update_rule(rule_id, tier_body(item.alert_type, tiers.escalate, reading))
        say(f"  tiers set to {json.dumps(tiers.escalate)} for the escalation")

        async def escalated():
            rows = await triggers(rule_id)
            return [t for t in rows if t.resolved_at is None and t.severity == "critical"] or None

        found = await wait_for(ctx, "the escalation to critical", escalated, timeout)
        if not found:
            res.escalate = "TIMEOUT"
            res.notes.append("no escalation within the limit")
        else:
            res.escalate = "critical"
            for t in found:
                say(f"     {_trigger_line(t)}")
            kind = "escalate"
            row = await wait_outbox(ctx, rule_id, kind, esc_from)
            say(f"     outbox: " + (f"row {row.id} email {row.email_status}, teams {row.teams_status}" if row else "no row"))
            say(f"     'rule email sent' log line: {log_line(ctx.args.log_file, rule_id, kind)}")
            answer = await ask_choice(ctx, expect_question(ctx, "ESCALATED card", "ESCALATED email"))
            res.escalate += f" ({answer})"
            if answer == "s":
                res.notes.append("operator skipped after the escalation")
                return

    resolve_from = _now()
    await update_rule(rule_id, tier_body(item.alert_type, tiers.resolve, reading))
    say(f"  tiers relaxed to {json.dumps(tiers.resolve)} at {_t(resolve_from)}")

    async def resolved():
        rows = await triggers(rule_id)
        if rows and all(t.resolved_at is not None for t in rows):
            return rows
        return None

    done = await wait_for(ctx, "the resolve", resolved, timeout)
    if not done:
        res.resolve = "TIMEOUT"
        res.notes.append(f"not resolved within {timeout // 60} min")
        return
    res.resolve = f"resolved x{len(done)}"
    say(f"  RESOLVED")
    for t in done:
        say(f"     {_trigger_line(t)}")
    await report_notice(ctx, res, rule_id, "resolve", resolve_from, "resolve")
    check_email_only(ctx, res, "resolve")
    res.resolve_ok = await ask_choice(ctx, expect_question(ctx, "RESOLVED card", "RESOLVED email"))


def expect_question(ctx: Ctx, card: str, email: str) -> str:
    """The operator's question for the channels under test."""
    if ctx.args.channels == "email":
        return f"{email[0].upper() + email[1:]} in the inbox, and NO card in the TEST channel?"
    if ctx.args.channels == "teams":
        return f"{card} in the TEST channel, and NO email in the inbox?"
    return f"{card} in the TEST channel and email in the inbox?"


def check_email_only(ctx: Ctx, res: Result, prefix: str) -> None:
    """--channels email: the email must be sent (outbox or log line) and,
    for the resolve, no card posted (the outbox Teams channel skipped)."""
    if ctx.args.channels != "email":
        return
    if "sent" not in (getattr(res, f"{prefix}_email"), getattr(res, f"{prefix}_log")):
        res.notes.append(f"{prefix} email not confirmed sent (outbox {getattr(res, f'{prefix}_email')}, "
                         f"log {getattr(res, f'{prefix}_log')})")
    if prefix == "resolve" and res.resolve_teams != "skipped":
        res.notes.append(f"resolve card: outbox Teams channel is {res.resolve_teams!r}, expected 'skipped'")


def print_results(results: list[Result]) -> None:
    if not results:
        return
    say("")
    say("RESULTS")
    head = ("type", "cluster", "fire", "teams", "email", "log", "ok?", "escal.", "resolve", "teams", "email", "log", "ok?", "secs")
    rows = [(r.alert_type, r.cluster, r.fire, r.fire_teams, r.fire_email, r.fire_log, r.fire_ok, r.escalate,
             r.resolve, r.resolve_teams, r.resolve_email, r.resolve_log, r.resolve_ok, str(r.seconds)) for r in results]
    widths = [max(len(str(x)) for x in col) for col in zip(head, *rows)]
    say("  " + "  ".join(h.ljust(w) for h, w in zip(head, widths)))
    for row, r in zip(rows, results):
        say("  " + "  ".join(str(x).ljust(w) for x, w in zip(row, widths)))
        for note in r.notes:
            say(f"      note: {note}")


async def verify(ctx: Ctx) -> bool:
    say("")
    say("FINAL VERIFICATION")
    ok = True

    def line(label: str, now, saved, good: bool) -> None:
        nonlocal ok
        ok = ok and good
        say(f"  {'OK  ' if good else 'FAIL'} {label}: {now}" + ("" if saved is None else f" (saved {saved})"))

    try:
        rules = await snapshot_rules()
        saved = [r for r in ctx.saved_rules if not is_batch(r)]
        current = [r for r in rules if not is_batch(r)]
        line("non-BATCHTEST rules", len(current), len(saved), len(current) == len(saved))
        en_now = sorted(r["id"] for r in current if r.get("enabled"))
        en_saved = sorted(r["id"] for r in saved if r.get("enabled"))
        line("enabled rules", en_now, en_saved, en_now == en_saved)
        line("non-BATCHTEST rows unchanged (every column)", current == saved, None, current == saved)
        left = [r["id"] for r in rules if is_batch(r)]
        line("BATCHTEST rules left", left or "none", None, not left)
    except Exception as exc:  # noqa: BLE001
        line("rules readable", f"no ({type(exc).__name__})", None, False)
    if ctx.saved_settings is not None:
        try:
            now = await ctx.deps.settings.read()
            for k in SETTINGS_KEYS:
                line(f"setting {k}", repr(now.get(k)), repr(ctx.saved_settings.get(k)), now.get(k) == ctx.saved_settings.get(k))
        except Exception as exc:  # noqa: BLE001
            line("settings readable from the agent", f"no ({type(exc).__name__})", None, False)
    say(f"  {'VERIFICATION PASSED' if ok else 'VERIFICATION FAILED: see FAIL lines; restore with --cleanup --restore-settings --run'}")
    return ok


async def finish(ctx: Ctx) -> bool:
    """Always runs after a --run: every BATCHTEST rule disabled and deleted,
    the settings restored, the verification printed. Signals are ignored."""
    ctx.cleaning = True
    say("")
    say("CLEANUP (signals are ignored until it ends)")
    for _attempt in range(2):
        ids = await _safe(batch_rule_ids, [])
        for rule_id in ids:
            try:
                await remove_rule(rule_id)
                say(f"  removed BATCHTEST rule {rule_id}")
            except Exception as exc:  # noqa: BLE001
                say(f"  could not remove rule {rule_id}: {type(exc).__name__}: {exc}")
        if not ids:
            break
    if ctx.settings_touched and ctx.saved_settings is not None:
        restore = {k: ctx.saved_settings.get(k) for k in ("email_enabled", "email_recipients")}
        try:
            await ctx.deps.settings.write(restore)
            say(f"  settings restored: email_enabled={restore['email_enabled']!r} email_recipients={restore['email_recipients']!r}")
        except Exception as exc:  # noqa: BLE001
            say(f"  !!! settings NOT restored ({type(exc).__name__}: {exc}); run --cleanup --restore-settings --run")
    print_results(ctx.results)
    ok = True
    if ctx.saved_rules is not None:
        ok = await verify(ctx)
    if ctx.backup_path:
        say(f"  backup: {ctx.backup_path} (inside the container; copy it out: "
            f"sudo docker cp {os.environ.get('HOSTNAME') or socket.gethostname()}:{ctx.backup_path} {DEFAULT_BACKUP_DIR}/)")
    return ok


async def _safe(fn, default):
    try:
        return await fn()
    except Exception as exc:  # noqa: BLE001
        say(f"  {fn.__name__} failed: {type(exc).__name__}: {exc}")
        return default


def remove_webhook_file(path: str) -> None:
    try:
        os.remove(path)
        say(f"  test webhook file {path} removed")
    except FileNotFoundError:
        pass
    except OSError as exc:
        say(f"  could not remove the test webhook file {path} ({type(exc).__name__}); remove it by hand")


def select_types(args) -> list[str]:
    only = parse_only(args.only)
    return only if only else list(EASY_TYPES)


async def body(ctx: Ctx) -> None:
    args = ctx.args
    mode = "RUN (changes rules and settings)" if args.run else "DRY RUN (reads only; nothing is changed)"
    say(f"BATCH TEST OF ALERT RULES -- {mode}" + (" -- CLEANUP ONLY" if args.cleanup else ""))
    say(f"  started {_now().isoformat(timespec='seconds')}")
    webhook_file = os.environ.get("BATCH_TEST_WEBHOOK_FILE", DEFAULT_WEBHOOK_FILE)
    if not args.cleanup:
        ctx.hook = load_webhook(webhook_file)
        say(f"  test webhook: {WEBHOOK_MASK} (from {webhook_file})")
        ctx.to = check_to(args.to)
        say(f"  emails go to: {ctx.to}")
        ch = CHANNELS[args.channels]
        say(f"  channels: {args.channels} (Teams {'on' if ch['enabled'] else 'off'}, Email {'on' if ch['email_enabled'] else 'off'})")
    types = select_types(args) if not args.cleanup else []
    slow_risky = [t for t in types if t in SLOW_TYPES + RISKY_TYPES]
    if args.run and ctx.deps.interactive_check and not sys.stdin.isatty():
        raise SafetyError("--run needs a terminal for the confirmations: use docker exec -it")

    from config import settings as agent_settings
    from storage import init_storage
    init_storage(agent_settings.agent_slug)

    names = await cluster_names()
    if args.cluster is not None and args.cluster not in names:
        raise SafetyError(f"--cluster {args.cluster} is not an enabled cluster (enabled: {sorted(names)})")

    ctx.saved_settings = await ctx.deps.settings.read(ctx.hook)
    s = ctx.saved_settings
    say(f"  agent {args.agent_url}: teams_enabled={s['teams_enabled']!r} email_enabled={s['email_enabled']!r} "
        f"email_recipients={s['email_recipients']!r}")
    sched = await evaluator_schedule()
    ctx.eval_minutes = sched["minutes"]
    say(f"  evaluator {EVALUATOR_JOB_ID}: cron {sched['cron']!r} (every {sched['minutes'] or '?'} min), "
        f"{'enabled' if sched['enabled'] else 'DISABLED'}; last run {await last_evaluator_run()}; poll every {POLL_SECS} s")

    rules = await snapshot_rules()
    ctx.saved_rules = rules
    non_batch = [r for r in rules if not is_batch(r)]
    say(f"  rules: {len(non_batch)} non-BATCHTEST, enabled {sorted(r['id'] for r in non_batch if r.get('enabled'))}; "
        f"leftover BATCHTEST: {[r['id'] for r in rules if is_batch(r)] or 'none'}")

    ctx.changes_allowed = bool(args.run)
    if args.run:
        ctx.backup_path = await write_backup(ctx, rules)
        say(f"  backup written: {ctx.backup_path} (mode 600)")
    else:
        say(f"  would write the backup to {os.environ.get('BATCH_TEST_BACKUP_DIR', DEFAULT_BACKUP_DIR)}/rules-before-batch-<UTC time>.json (mode 600)")
    await remove_leftovers(ctx, "leftover")

    if args.cleanup:
        if args.restore_settings:
            await restore_from_backup(ctx)
        return

    problems = rule_problems(rules) + settings_problems(s, args.channels)
    if not sched["enabled"]:
        problems.append(f"the {EVALUATOR_JOB_ID} schedule is disabled or missing: nothing would fire")
    pending = await pending_rule_notices()
    if pending:
        problems.append(f"{pending} rule notice(s) are still pending in the outbox (they would be emailed to --to)")
    if args.run and problems:
        raise SafetyError("; ".join(problems))
    if problems:
        say("  --run would REFUSE: " + "; ".join(problems))
    else:
        say("  safety checks: OK")

    if slow_risky:
        say("")
        say(WARN_SLOW_RISKY)
    plan = await build_plan(types, names, args.cluster, args.escalate)
    print_plan(plan, ctx.eval_minutes, args.escalate)
    if not args.run:
        say("")
        say("DRY RUN: nothing was changed. Add --run to execute the plan above.")
        return

    if slow_risky:
        if (await ctx.deps.ask("Type YES to continue with the SLO / lag types: ")).strip() != "YES":
            raise SafetyError("not confirmed with YES")

    ctx.settings_touched = True  # set first: a failure half-way still restores
    await ctx.deps.settings.write({"email_enabled": True, "email_recipients": ctx.to})
    say(f"  settings for the run: email_enabled=True email_recipients={ctx.to!r} (restored at the end)")
    for item in plan:
        ctx.results.append(await test_rule(ctx, item))


async def restore_from_backup(ctx: Ctx) -> None:
    backup_dir = os.environ.get("BATCH_TEST_BACKUP_DIR", DEFAULT_BACKUP_DIR)
    files = sorted(f for f in os.listdir(backup_dir) if re.fullmatch(r"rules-before-batch-\d{8}T\d{6}Z\.json", f)) \
        if os.path.isdir(backup_dir) else []
    # The newest backup taken by a real run, not by this cleanup itself.
    files = [f for f in files if os.path.join(backup_dir, f) != ctx.backup_path]
    if not files:
        raise SafetyError(f"no earlier backup in {backup_dir} to restore the settings from")
    path = os.path.join(backup_dir, files[-1])
    with open(path, encoding="utf-8") as fh:
        saved = (json.load(fh).get("settings") or {})
    restore = {k: saved.get(k) for k in ("email_enabled", "email_recipients")}
    say(f"  settings in {path}: {restore}")
    if ctx.changes_allowed:
        await ctx.deps.settings.write(restore)
        ctx.saved_settings.update(restore)  # what the verification now expects
        say("  settings restored from the backup")
    else:
        say("  would restore them (add --run)")


async def run(args, deps: Deps) -> int:
    ctx = Ctx(args, deps)
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    state = {"signal": None}

    def on_signal(name: str) -> None:
        if ctx.cleaning or state["signal"] is not None:
            say(f"\n{name} ignored: {'cleanup in progress' if ctx.cleaning else 'already stopping'}")
            return
        state["signal"] = name
        say(f"\n{name} received: stopping, then cleaning up")
        task.cancel()

    installed = []
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        try:
            loop.add_signal_handler(sig, on_signal, sig.name)
            installed.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            pass

    code = 0
    try:
        await body(ctx)
    except SafetyError as exc:
        say(f"REFUSED: {exc}")
        code = 2
    except AbortRun as exc:
        say(f"STOPPED: {exc}")
        code = 130
    except asyncio.CancelledError:
        if hasattr(task, "uncancel"):
            task.uncancel()
        say(f"STOPPED by {state['signal'] or 'cancellation'}")
        code = 130
    except KeyboardInterrupt:
        say("STOPPED by Ctrl+C")
        code = 130
    except Exception:  # noqa: BLE001
        say("ERROR: " + traceback.format_exc())
        code = 1
    finally:
        try:
            if ctx.changes_allowed and ctx.saved_rules is not None:
                if not await finish(ctx):
                    code = code or 1
            if args.run and not args.cleanup:
                remove_webhook_file(os.environ.get("BATCH_TEST_WEBHOOK_FILE", DEFAULT_WEBHOOK_FILE))
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)
            await deps.settings.close()
    return code


def main(argv=None) -> int:
    _setup_logging()
    sys.excepthook = _masked_excepthook
    args = parse_args(argv)
    deps = Deps(settings=AgentSettings(args.agent_url))
    return asyncio.run(run(args, deps))


if __name__ == "__main__":
    sys.exit(main())
