"""Notification outbox, step 1: shadow mode (design Part 2).

The inline owner and rule sends are unchanged. After each one, a separate
best-effort step records one kafka_notification_outbox row (origin 'inline',
finished on insert) describing what the inline send did per channel: sent,
failed with a short secret-free error, or skipped with the reason -- including
the not-configured cases that are silent today -- plus one
kafka_notification_attempts row per channel.

Setting notification_outbox_mode: "off" records nothing; "shadow" (default)
records; "on" behaves as "shadow" until the delivery worker exists.

Recording never raises and never changes a send, a trigger write or the
caller's timing: hooks call record_inline_soon (a background task) or, on the
watchdog path that exits right after, await record_inline under
RECORD_TIMEOUT_SECS inside the caller's existing cap. A failure to record
logs one WARNING and changes nothing.
"""
import asyncio
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Identifies this process run in attempt rows and payloads.
BOOT_ID = uuid.uuid4().hex

# Cap for an awaited record (watchdog notices, just before the restart).
RECORD_TIMEOUT_SECS = 1.5

_MAX_AGE = {
    "owner_watchdog": timedelta(hours=12),
    "owner_summary": timedelta(hours=2),
    "rule_fire": timedelta(hours=24),
    "rule_escalate": timedelta(hours=24),
    "rule_resolve": timedelta(hours=24),
    "rule_reminder": timedelta(hours=6),
    "heartbeat": timedelta(hours=12),
    "deadman": timedelta(hours=12),
}
_CHANNELS = ("email", "teams")
_ERROR_MAX = 300
_URL_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://\S+")
_CONFIG_ERRORS = (
    "not configured", "invalid smtp port", "invalid from address", "no valid recipients",
    "no allowed recipients", "authentication failed", "sender address refused",
    "all recipients refused", "could not resolve smtp host",
)

_tasks: set = set()


def mode() -> str:
    """"off" or "shadow" ("on" and anything unknown behave as "shadow")."""
    try:
        from routes_settings import _config
        value = str(_config.get("notification_outbox_mode") or "shadow").strip().lower()
    except Exception:  # noqa: BLE001
        value = "shadow"
    return "off" if value == "off" else "shadow"


def enabled() -> bool:
    return mode() != "off"


# ── Channel results ──────────────────────────────────────────────────────────

def sent(duration_ms=None, started_at=None, warning=None, detail=None) -> dict:
    return {"status": "sent", "duration_ms": duration_ms, "started_at": started_at,
            "warning": warning, "detail": detail or {}}


def failed(error, duration_ms=None, started_at=None, detail=None, config_problem=False) -> dict:
    return {"status": "failed", "error": error, "duration_ms": duration_ms, "started_at": started_at,
            "detail": detail or {}, "config_problem": config_problem}


def skipped(reason, config_problem=False) -> dict:
    """A channel not attempted. config_problem=True when it was wanted but
    is not configured (counts against the notice); False when a setting or
    rule deliberately leaves it out."""
    return {"status": "skipped", "reason": reason, "config_problem": config_problem}


def from_report(entry) -> dict:
    """Channel result from a report entry filled by send_to_teams,
    notify_owner or notify_rule_email."""
    entry = dict(entry or {})
    status = entry.get("status")
    if status == "sent":
        return sent(entry.get("duration_ms"), entry.get("started_at"), entry.get("warning"), entry.get("detail"))
    if status == "skipped":
        return skipped(entry.get("reason") or "no reason given", bool(entry.get("config_problem")))
    return failed(entry.get("error") or "failed (no error given)", entry.get("duration_ms"),
                  entry.get("started_at"), entry.get("detail"), bool(entry.get("config_problem")))


# ── Pure row building ────────────────────────────────────────────────────────

def _secrets() -> list:
    try:
        from routes_settings import _config
        return [str(_config.get(k) or "") for k in ("smtp_password", "teams_webhook_url")]
    except Exception:  # noqa: BLE001
        return []


def clean_error(text, secrets=None):
    """Short, single-line, secret-free text: CR/LF removed, any configured
    secret masked, every URL (webhook URLs carry a signature) replaced."""
    if text is None or text == "":
        return None
    text = str(text).replace("\r", " ").replace("\n", " ").strip()
    for secret in (secrets if secrets is not None else _secrets()):
        if secret and len(secret) >= 4:
            text = text.replace(secret, "***")
    text = _URL_RE.sub("<url>", text)
    return text[:_ERROR_MAX]


def outcome(result: dict) -> str:
    status = result.get("status")
    if status in ("sent", "skipped"):
        return status
    error = str(result.get("error") or "").lower()
    if "timed out" in error:
        return "timeout"
    if result.get("config_problem") or any(e in error for e in _CONFIG_ERRORS):
        return "failed_config"
    http_status = (result.get("detail") or {}).get("http_status")
    if isinstance(http_status, int) and 400 <= http_status < 500 and http_status != 429:
        return "failed_permanent"
    return "failed_transient"


def state(results: dict) -> str:
    """delivered / degraded / failed / suppressed -- see KafkaNotificationOutbox."""
    values = [results.get(c) or {} for c in _CHANNELS]
    any_sent = any(r.get("status") == "sent" for r in values)
    problem = any(
        r.get("status") == "failed"
        or (r.get("status") == "skipped" and r.get("config_problem"))
        or (r.get("status") == "sent" and r.get("warning"))
        for r in values
    )
    if any_sent:
        return "degraded" if problem else "delivered"
    return "failed" if problem else "suppressed"


def priority(kind: str, severity) -> int:
    if kind == "owner_watchdog":
        return 0
    if kind == "deadman":
        return 1
    if kind in ("rule_fire", "rule_escalate", "rule_reminder"):
        return 1 if str(severity or "").lower() == "critical" else 2
    if kind == "rule_resolve":
        return 3
    if kind == "owner_summary":
        return 4
    return 5


def build_row(kind, routing, dedupe_key, alert_config_id, cluster_id, trigger_ids, severity,
              subject_or_title, channel_results, *, source="", payload=None, now=None, secrets=None):
    """(outbox row values, attempt row values) for one inline notice. Pure."""
    now = now or datetime.now(timezone.utc)
    results = {}
    for channel in _CHANNELS:
        r = dict((channel_results or {}).get(channel) or skipped("no result recorded"))
        r["error"] = clean_error(r.get("error"), secrets)
        r["reason"] = clean_error(r.get("reason"), secrets)
        r["warning"] = clean_error(r.get("warning"), secrets)
        results[channel] = r

    row = {
        "kind": kind,
        "source": source or "inline",
        "routing": routing,
        "origin": "inline",
        "dedupe_key": dedupe_key,
        "priority": priority(kind, severity),
        "alert_config_id": alert_config_id,
        "cluster_id": cluster_id,
        "trigger_ids": list(trigger_ids) if trigger_ids else None,
        "severity": severity,
        "payload": {
            "title": clean_error(subject_or_title, secrets),
            "mode": "shadow",
            "boot_id": BOOT_ID,
            **(payload or {}),
        },
        "expires_at": now + _MAX_AGE.get(kind, timedelta(hours=24)),
        "state": state(results),
        "finished_at": now,
        "updated_at": now,
    }
    attempts = []
    sent_times = []
    for channel, r in results.items():
        attempted = r["status"] in ("sent", "failed")
        started_at = r.get("started_at") if attempted else None
        duration_ms = r.get("duration_ms") if attempted else None
        ended_at = None
        if started_at is not None and duration_ms is not None:
            ended_at = started_at + timedelta(milliseconds=duration_ms)
        if r["status"] == "sent":
            sent_times.append(ended_at or now)
        row[f"{channel}_status"] = r["status"]
        row[f"{channel}_attempts"] = 1 if attempted else 0
        row[f"{channel}_last_error"] = r["error"] if r["status"] == "failed" else r["reason"] if r["status"] == "skipped" else None
        row[f"{channel}_sent_at"] = (ended_at or now) if r["status"] == "sent" else None
        detail = dict(r.get("detail") or {})
        if r.get("warning"):
            detail["warning"] = r["warning"]
        if r.get("config_problem"):
            detail["config_problem"] = True
        attempts.append({
            "channel": channel,
            "attempt_no": 1 if attempted else 0,
            "boot_id": BOOT_ID,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_ms": duration_ms,
            "phase_reached": "done" if r["status"] == "sent" else None,
            "outcome": outcome(r),
            "error": r["error"] if r["status"] == "failed" else r["reason"] if r["status"] == "skipped" else None,
            "detail": detail or None,
        })
    row["email_warning"] = results["email"].get("warning") if results["email"]["status"] == "sent" else None
    row["delivered_at"] = max(sent_times) if sent_times else None
    return row, attempts


# ── Recording ────────────────────────────────────────────────────────────────

def _exc_text(exc: BaseException) -> str:
    orig = getattr(exc, "orig", None)  # DBAPI error without the statement parameters
    return clean_error(f"{type(exc).__name__}: {orig if orig is not None else exc}") or type(exc).__name__


async def _insert(session, row: dict, attempts: list):
    from sqlalchemy import insert
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from models import KafkaNotificationAttempt, KafkaNotificationOutbox

    stmt = (
        pg_insert(KafkaNotificationOutbox).values(**row)
        .on_conflict_do_nothing(index_elements=["dedupe_key"])
        .returning(KafkaNotificationOutbox.id)
    )
    outbox_id = (await session.execute(stmt)).scalar_one_or_none()
    if outbox_id is None:
        logger.debug("outbox: %s already recorded", row["dedupe_key"])
        return None
    if attempts:
        await session.execute(insert(KafkaNotificationAttempt), [dict(a, outbox_id=outbox_id) for a in attempts])
    return outbox_id


async def record_inline(session_or_none, kind, routing, dedupe_key, alert_config_id, cluster_id,
                        trigger_ids, severity, subject_or_title, channel_results, *, source="", payload=None):
    """Record one inline notice and its attempts. Idempotent on dedupe_key
    (a repeat records nothing and returns None). With a session, writes in
    a savepoint and leaves the commit to the caller; without, uses its own
    session. Returns the new row id or None. Never raises."""
    if not enabled():
        return None
    try:
        row, attempts = build_row(
            kind, routing, dedupe_key, alert_config_id, cluster_id, trigger_ids, severity,
            subject_or_title, channel_results, source=source, payload=payload,
        )
        if session_or_none is not None:
            async with session_or_none.begin_nested():
                return await _insert(session_or_none, row, attempts)
        from database import SessionLocal
        if SessionLocal is None:
            return None
        async with SessionLocal() as session:
            outbox_id = await _insert(session, row, attempts)
            await session.commit()
            return outbox_id
    except Exception as exc:  # noqa: BLE001 -- contract: never raise
        logger.warning("outbox: could not record %s %s: %s", kind, dedupe_key, _exc_text(exc))
        return None


def record_inline_soon(**kwargs) -> None:
    """record_inline(None, **kwargs) as a background task, so the caller
    never waits on the database. Never raises."""
    try:
        if not enabled():
            return
        task = asyncio.create_task(record_inline(None, **kwargs))
        _tasks.add(task)
        task.add_done_callback(_tasks.discard)
    except Exception as exc:  # noqa: BLE001
        logger.warning("outbox: could not schedule record of %s: %s", kwargs.get("dedupe_key"), type(exc).__name__)


async def drain(timeout: float = 10) -> None:
    """Wait for pending background records (tests, shutdown)."""
    if _tasks:
        await asyncio.wait(list(_tasks), timeout=timeout)
