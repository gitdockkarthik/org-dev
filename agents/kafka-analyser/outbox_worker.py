"""Notification outbox, step 2a: the delivery worker (design 2.3, 2.4).

Delivers kafka_notification_outbox rows that are not finished (origin
'outbox', state pending). Nothing enqueues such rows yet: in shadow mode every
row is an inline record, finished on insert, so a run finds nothing and users
see no change. enqueue() exists for the next step and for the tests; no live
notice goes through it.

One run (the "kafka-notification-delivery" job every minute, or kick() right
after an enqueue commit; both share one lock):
  a. recover: a channel left 'sending' by another boot, or whose lease has
     expired, gets its open attempt closed as 'unknown' and goes back to
     pending, flagged for one labelled resend (payload.worker.resend);
  b. claim up to BATCH due rows (FOR UPDATE SKIP LOCKED, priority order), set
     the due channels to 'sending' under a lease, insert their attempt rows
     (outcome NULL) and commit -- no transaction is open during network I/O;
  c. send each channel and record the attempt and the channel result; the
     channel update is conditional on the claim still being ours (status
     'sending', claimed_by this boot, the same in-flight attempt), so a late
     writer never overwrites a reclaimed row; its attempt row is always kept;
  d. stop claiming after CLAIM_WINDOW_SECS.
All times are database now().

Channel results: sent; known failures (SMTP failure before DATA or an SMTP
rejection of DATA, Teams connect error or HTTP error response) are retried on
backoff and can never double-send; an unknown outcome (connection lost during
DATA, no Teams response, agent restart mid-send) is resent once with
RESEND_LABEL and the same Message-ID, and a second unknown abandons the
channel. Config errors retry at the backoff cap, content errors (HTTP 4xx,
SMTP 5xx at DATA) fail at once, and a channel still undelivered at the row's
max age is abandoned with "N attempts, last error ...".

Owner routing: email first; Teams waits in 'standby' and becomes pending on
the first email failure (or a send that outlives EMAIL_CAP_SECS, or email not
configured) and goes out in the same run; email keeps retrying and the row is
degraded until it succeeds. Rule routing: each channel independent.

Recipients, SMTP settings and webhooks are read from the live settings at send
time; the payload holds only the content and a webhook reference ("rule:<id>"
or "agent_default"). Passwords, addresses and webhook URLs are never logged
or stored.

Rule rows (routing "rule") enqueued with a policy (enqueue_in, on the
evaluator's own session) follow the rule's two channel toggles, read again at
send time. Teams on and Email on: both channels independent. Teams on, Email
off: email skipped "rule email off". Teams off, Email on: email first, the
card waits in 'standby' and becomes pending in the same run once the email
has failed RULE_FALLBACK_AFTER_ATTEMPTS times, is abandoned, failed for good,
or is not configured -- only while rule_teams_fallback and teams_enabled are
on, otherwise it is skipped with the reason. Both off: no row. A resolve,
escalate or reminder row waits (at most DEPENDENCY_WAIT) for every earlier
fire or escalate row sharing a trigger id to finish that channel; a resolve
card goes out only if its fire's card did. When a rule row's Teams channel
settles, the triggers in trigger_ids still showing "queued (outbox row N)"
get teams_post_success and error_detail.

Payload of a rule row (keys shared with owner rows first):
  title       str   the email subject (or the kind), shown in the delivery log
  mode        "on"  boot_id  str (as owner rows)
  email       {"subject", "text", "html"}  rendered email; absent: no email
  teams_card  dict  the rendered Teams card; absent: no card
  teams_ref   "rule:<id>"  webhook reference, resolved at send time
  rule_name   str   the rule's name when enqueued
  policy      {"rule_id": int, "teams_toggle": bool, "email_toggle": bool}
              the rule's toggles when enqueued (the row's channel plan)
  worker      the worker's bookkeeping (due, inflight, resend, config_problem)
  ...         any extra keys the caller passes (subject, cluster_name, ...)
Dedupe keys (rule_dedupe_key): rule_fire:{trigger_id},
rule_escalate:{trigger_id}:{tier}:{eval_ts}, rule_resolve:{trigger_id},
lag:{config_id}:{kind}:{eval_ts}, lag_resolve:{config_id}:{eval_ts}.
"""
import asyncio
import copy
import functools
import json
import logging
import os
import random
import re
import smtplib
import socket
import ssl
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.policy import SMTP as SMTP_POLICY
from email.utils import formatdate, make_msgid

import httpx
from sqlalchemy import text

import outbox

logger = logging.getLogger(__name__)

JOB_ID = "kafka-notification-delivery"
JOB_CRON = "* * * * *"
JOB_TIMEOUT_SECS = 50

BATCH = 10
LEASE_SECS = 120
CLAIM_WINDOW_SECS = 40
EMAIL_CAP_SECS = 30
SMTP_SOCKET_TIMEOUT_SECS = 10
TEAMS_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
BACKOFF_BASE_SECS = 30
BACKOFF_CAP_SECS = 900
BACKOFF_JITTER = 0.2
RETRY_AFTER_MAX_SECS = 3600
RESEND_LABEL = "(re-sent after an unconfirmed delivery, may be a duplicate)"
DEADMAN_UNFINISHED_MINUTES = 15
DEADMAN_ROW_LIMIT = 50
# Watchdog notices that could not be written (database down) are sent inline
# and appended here; startup ingests them. Survives a container restart.
SPOOL_PATH = os.environ.get("OUTBOX_SPOOL_PATH", "/app/.outbox_spool.jsonl")

_CHANNELS = ("email", "teams")
_OPEN = ("pending", "sending")
_RETRY = ("failed_transient", "failed_config", "timeout")
# A channel status that no longer changes (what a dependent row waits for).
_DONE = ("sent", "failed", "skipped", "abandoned", "superseded")

# Rule rows: the Teams fallback of a rule with its Teams toggle off starts
# after this many failed email attempts (or at once, see _apply).
RULE_FALLBACK_AFTER_ATTEMPTS = 3
# A resolve, escalate or reminder row waits at most this long (from its
# creation) for its earlier fire/escalate rows; then it proceeds.
DEPENDENCY_WAIT = timedelta(minutes=60)
DEPENDENCY_POLL_SECS = 30
RULE_EMAIL_OFF = "rule email off"
TEAMS_OFF_RULE = "Teams off for this rule"  # teams_alerts._TEAMS_OFF_RULE
TEAMS_OFF_NO_FALLBACK = "Teams off for this rule (fallback off)"
TEAMS_OFF_GLOBAL = "Teams switched off globally"  # teams_alerts._TEAMS_OFF_GLOBAL
FALLBACK_NOT_NEEDED = "fallback not needed: email sent"
FALLBACK_SUPERSEDED = "fallback no longer needed: email sent"
NO_OPENING_CARD = "opening card never reached Teams"

# Never the default executor: a saturated default pool must not delay email.
_SMTP_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="outbox-smtp")

_lock = asyncio.Lock()
_run_again = False
_kick_task = None
_late_tasks: set = set()
_late_futures: set = set()
_started_at = time.time()
_last_success = None  # time.time() of the last run that completed


def _boot() -> str:
    return outbox.BOOT_ID


# ── Pure rules ───────────────────────────────────────────────────────────────

def backoff_secs(attempts: int, outcome: str, retry_after=None, rand=random.random) -> float:
    """Delay before the next attempt: 30 s doubling per attempt to a 15 min
    cap, +/-20% jitter; config errors wait the cap; a Retry-After is honoured
    (never less than it, at most an hour)."""
    if outcome == "failed_config":
        base = BACKOFF_CAP_SECS
    else:
        base = min(BACKOFF_CAP_SECS, BACKOFF_BASE_SECS * 2 ** max(0, attempts - 1))
    delay = base * (1 + BACKOFF_JITTER * (2 * rand() - 1))
    if retry_after:
        delay = max(delay, min(float(retry_after), RETRY_AFTER_MAX_SECS))
    return delay


def parse_retry_after(value):
    """Seconds from a Retry-After header (delta seconds or HTTP date), or None."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        when = parsedate_to_datetime(value)
        return max(0.0, (when - datetime.now(when.tzinfo)).total_seconds())
    except Exception:  # noqa: BLE001
        return None


def http_outcome(status: int) -> str:
    if status in (200, 202):
        return "sent"
    if status in (408, 429) or status >= 500:
        return "failed_transient"
    return "failed_permanent"


def label_subject(subject: str) -> str:
    return f"{subject} {RESEND_LABEL}"


def label_card(card: dict) -> dict:
    """A copy of card with RESEND_LABEL as its first line."""
    card = copy.deepcopy(card or {})
    block = {"type": "TextBlock", "text": RESEND_LABEL, "wrap": True, "weight": "Bolder", "color": "Warning"}
    attachments = card.get("attachments")
    if isinstance(attachments, list) and attachments and isinstance(attachments[0].get("content"), dict):
        attachments[0]["content"].setdefault("body", []).insert(0, block)
    elif isinstance(card.get("body"), list):
        card["body"].insert(0, block)
    else:
        card["text"] = f"{RESEND_LABEL} {card.get('text') or ''}".strip()
    return card


def _w(row: dict) -> dict:
    """The worker's bookkeeping in the payload: per-channel due time, the
    in-flight attempt id, channels flagged for a resend after an unknown
    outcome, channels skipped or failing for want of configuration."""
    w = row["payload"].setdefault("worker", {})
    for key, empty in (("due", {}), ("inflight", {}), ("resend", []), ("config_problem", {})):
        w.setdefault(key, empty)
    return w


def _due(row: dict, channel: str):
    value = _w(row)["due"].get(channel)
    return datetime.fromisoformat(value) if value else None


def _policy(row: dict) -> dict | None:
    """A rule row's channel plan (payload policy), or None (owner rows and
    rule rows enqueued without one)."""
    policy = row["payload"].get("policy") if row["routing"] == "rule" else None
    return policy if isinstance(policy, dict) else None


def _rule_fallback(row: dict) -> bool:
    """A rule row whose card is only a fallback: Teams off, Email on."""
    policy = _policy(row)
    return bool(policy) and not policy.get("teams_toggle") and bool(policy.get("email_toggle"))


def _email_first(row: dict) -> bool:
    return row["routing"] == "owner" or _rule_fallback(row)


def _settings() -> dict:
    from routes_settings import _config
    return dict(_config)


def queued_detail(row_id) -> str:
    """error_detail of a trigger whose Teams post waits in outbox row row_id."""
    return f"queued (outbox row {row_id})"


def row_state(row: dict) -> tuple:
    """(state, finished) from the channel statuses. A finished row is judged
    like an inline record (outbox.state); an unfinished one is degraded once
    something was sent while a channel that has already failed is still
    retrying, otherwise pending."""
    statuses = {c: row[f"{c}_status"] for c in _CHANNELS}
    w = _w(row)
    if statuses["teams"] == "standby" and statuses["email"] in _OPEN:  # owner or rule fallback
        unfinished = ["email"]
    else:
        unfinished = [c for c in _CHANNELS if statuses[c] in _OPEN]
    if unfinished:
        any_sent = any(statuses[c] == "sent" for c in _CHANNELS)
        retrying = any(row[f"{c}_attempts"] > 0 and row[f"{c}_last_error"] for c in unfinished)
        fallback_used = _email_first(row) and statuses["teams"] == "sent"  # email not confirmed yet
        return ("degraded" if any_sent and (retrying or fallback_used) else "pending"), False
    results = {}
    for c in _CHANNELS:
        status, error = statuses[c], row[f"{c}_last_error"]
        if status == "sent":
            results[c] = outbox.sent(warning=row.get("email_warning") if c == "email" else None)
        elif status in ("failed", "abandoned"):
            results[c] = outbox.failed(error or status)
        else:  # skipped, superseded, standby
            results[c] = outbox.skipped(error or status, bool(w["config_problem"].get(c)) and status == "skipped")
    return outbox.state(results), True


def _settle(row: dict, now: datetime) -> None:
    """Recompute state, delivered_at, finished_at, next_attempt_at and the
    claim from the channel statuses."""
    state, finished = row_state(row)
    row["state"] = state
    sent_times = [row[f"{c}_sent_at"] for c in _CHANNELS if row[f"{c}_status"] == "sent" and row[f"{c}_sent_at"]]
    if sent_times and state in ("delivered", "degraded"):
        row["delivered_at"] = max(sent_times)
    sending = any(row[f"{c}_status"] == "sending" for c in _CHANNELS)
    if not sending:
        row["claimed_by"] = None
        row["claim_expires_at"] = None
    if finished:
        row["finished_at"] = row.get("finished_at") or now
        return
    dues = [_due(row, c) or now for c in _CHANNELS if row[f"{c}_status"] == "pending"]
    if dues:
        row["next_attempt_at"] = min(dues)
    elif sending:
        row["next_attempt_at"] = now + timedelta(seconds=LEASE_SECS)


def _attempt_summary(row: dict, channel: str, error) -> str:
    n = row[f"{channel}_attempts"]
    return outbox.clean_error(f"{n} attempt{'s' if n != 1 else ''}, last error: {error or 'none'}")


def _activate_fallback(row: dict, now: datetime) -> None:
    """Standby Teams card -> pending now. A rule row's fallback also needs
    rule_teams_fallback and teams_enabled; otherwise the card is skipped
    with the reason."""
    if row["teams_status"] != "standby":
        return
    if row["routing"] == "owner":
        row["teams_status"] = "pending"
        _w(row)["due"]["teams"] = now.isoformat()
    elif _rule_fallback(row):
        cfg = _settings()
        if not cfg.get("rule_teams_fallback", True):
            _skip_standby(row, TEAMS_OFF_NO_FALLBACK)
        elif not cfg.get("teams_enabled"):
            _skip_standby(row, TEAMS_OFF_GLOBAL)
        else:
            row["teams_status"] = "pending"
            _w(row)["due"]["teams"] = now.isoformat()


def _skip_standby(row: dict, reason: str) -> None:
    if row["teams_status"] == "standby":
        row["teams_status"] = "skipped"
        row["teams_last_error"] = reason
        _w(row)["due"].pop("teams", None)


def _apply(row: dict, channel: str, res: dict, now: datetime, rand=random.random) -> None:
    """Apply one channel result (outcome, error, warning, retry_after,
    config_problem) to the row dict."""
    w = _w(row)
    w["inflight"].pop(channel, None)
    outcome_ = res["outcome"]
    error = outbox.clean_error(res.get("error"))
    status_key, error_key = f"{channel}_status", f"{channel}_last_error"
    if outcome_ == "sent":
        row[status_key] = "sent"
        row[f"{channel}_sent_at"] = now
        row[error_key] = None
        w["due"].pop(channel, None)
        w["config_problem"].pop(channel, None)
        if channel == "email":
            row["email_warning"] = outbox.clean_error(res.get("warning"))
    elif outcome_ == "skipped":
        row[status_key] = "skipped"
        row[error_key] = error
        w["due"].pop(channel, None)
        if res.get("config_problem"):
            w["config_problem"][channel] = True
    elif outcome_ == "unknown":
        if channel in w["resend"]:
            row[status_key] = "abandoned"
            row[error_key] = _attempt_summary(row, channel, f"outcome unknown twice ({error}); not re-sent again")
            w["due"].pop(channel, None)
        else:
            row[status_key] = "pending"
            row[error_key] = error
            w["resend"].append(channel)
            w["due"][channel] = now.isoformat()
    elif outcome_ in _RETRY:
        if outcome_ == "failed_config":
            w["config_problem"][channel] = True
        due = now + timedelta(seconds=backoff_secs(row[f"{channel}_attempts"], outcome_, res.get("retry_after"), rand))
        if due >= row["expires_at"]:
            row[status_key] = "abandoned"
            row[error_key] = _attempt_summary(row, channel, error)
            w["due"].pop(channel, None)
        else:
            row[status_key] = "pending"
            row[error_key] = error
            w["due"][channel] = due.isoformat()
    else:  # failed_permanent
        row[status_key] = "failed"
        row[error_key] = error
        w["due"].pop(channel, None)

    if _email_first(row) and channel == "email":
        if row["email_status"] == "sent":
            if row["teams_status"] == "standby":
                row["teams_status"] = "skipped"
                row["teams_last_error"] = FALLBACK_NOT_NEEDED
            elif row["teams_status"] == "pending":
                row["teams_status"] = "superseded"
                row["teams_last_error"] = FALLBACK_SUPERSEDED
                w["due"].pop("teams", None)
        elif row["routing"] == "owner":
            if outcome_ != "unknown" or row["email_status"] == "abandoned":
                _activate_fallback(row, now)
        elif res.get("teams_reason"):
            # The rule's own Email toggle is off, or the rule is gone: not a delivery failure.
            _skip_standby(row, res["teams_reason"])
        elif (row["email_status"] in ("failed", "abandoned")
              or (outcome_ == "skipped" and res.get("config_problem"))
              or (outcome_ in _RETRY and row["email_attempts"] >= RULE_FALLBACK_AFTER_ATTEMPTS)):
            _activate_fallback(row, now)
        elif row["email_status"] == "skipped":
            _skip_standby(row, TEAMS_OFF_RULE)


def _expire(row: dict, now: datetime) -> None:
    """Max age reached: every channel not yet done is abandoned."""
    w = _w(row)
    for c in _CHANNELS:
        if row[f"{c}_status"] in ("pending", "standby"):
            last = row[f"{c}_last_error"] if row[f"{c}_attempts"] else "max age reached before a send"
            row[f"{c}_status"] = "abandoned"
            row[f"{c}_last_error"] = _attempt_summary(row, c, last)
            w["due"].pop(c, None)


# ── Database ─────────────────────────────────────────────────────────────────

_COLS = (
    "id", "created_at", "kind", "routing", "priority", "alert_config_id", "trigger_ids", "payload",
    "email_status", "teams_status", "email_attempts", "teams_attempts", "email_last_error",
    "teams_last_error", "email_warning", "email_sent_at", "teams_sent_at", "email_message_id",
    "next_attempt_at", "claimed_by", "claim_expires_at", "expires_at", "state", "delivered_at", "finished_at",
)
_MUTABLE = (
    "email_status", "teams_status", "email_attempts", "teams_attempts", "email_last_error",
    "teams_last_error", "email_warning", "email_sent_at", "teams_sent_at", "email_message_id",
    "next_attempt_at", "claimed_by", "claim_expires_at", "state", "delivered_at", "finished_at",
)
_SELECT = f"SELECT {', '.join(_COLS)} FROM kafka_notification_outbox"


def _session():
    from database import SessionLocal
    if SessionLocal is None:
        raise RuntimeError("database not configured")
    return SessionLocal()


async def _db_now(s) -> datetime:
    return (await s.execute(text("SELECT now()"))).scalar_one()


async def _rows(s, where: str, **params) -> list:
    result = await s.execute(text(f"{_SELECT} WHERE {where}"), params)
    return [dict(r, payload=dict(r["payload"] or {})) for r in result.mappings().all()]


async def _write(s, row: dict, cond: str = "", **params) -> int:
    sets = ", ".join(f"{c} = :{c}" for c in _MUTABLE)
    values = {c: row[c] for c in _MUTABLE}
    values.update(params, id=row["id"], payload=json.dumps(row["payload"], default=str))
    result = await s.execute(text(
        f"UPDATE kafka_notification_outbox SET {sets}, payload = CAST(:payload AS jsonb), updated_at = now() "
        f"WHERE id = :id {cond}"
    ), values)
    return result.rowcount


async def _close_attempt(s, attempt_id: int, outcome_, error=None, phase=None, duration_ms=None, detail=None) -> None:
    await s.execute(text(
        "UPDATE kafka_notification_attempts SET ended_at = now(), duration_ms = :d, phase_reached = :p, "
        "outcome = :o, error = :e, detail = COALESCE(detail, '{}'::jsonb) || CAST(:det AS jsonb) WHERE id = :id"
    ), {"id": attempt_id, "d": duration_ms, "p": phase, "o": outcome_, "e": outbox.clean_error(error),
        "det": json.dumps(detail or {}, default=str)})


async def _start_attempt(s, row: dict, channel: str) -> int:
    """Mark channel 'sending' under this boot and insert its attempt row."""
    w = _w(row)
    row[f"{channel}_attempts"] += 1
    row[f"{channel}_status"] = "sending"
    detail = {"resend_after_unknown": True} if channel in w["resend"] else None
    attempt_id = (await s.execute(text(
        "INSERT INTO kafka_notification_attempts (outbox_id, channel, attempt_no, boot_id, started_at, detail) "
        "VALUES (:o, :c, :n, :b, now(), CAST(:d AS jsonb)) RETURNING id"
    ), {"o": row["id"], "c": channel, "n": row[f"{channel}_attempts"], "b": _boot(),
        "d": json.dumps(detail) if detail else None})).scalar_one()
    w["inflight"][channel] = attempt_id
    return attempt_id


async def _recover() -> int:
    """(a) Channels left 'sending' by another boot or past their lease."""
    count = 0
    async with _session() as s, s.begin():
        now = await _db_now(s)
        rows = await _rows(
            s, "finished_at IS NULL AND (email_status = 'sending' OR teams_status = 'sending') "
               "AND (claimed_by IS DISTINCT FROM :boot OR claim_expires_at < now()) "
               "ORDER BY id FOR UPDATE SKIP LOCKED", boot=_boot())
        for row in rows:
            w = _w(row)
            for c in _CHANNELS:
                if row[f"{c}_status"] != "sending":
                    continue
                error = (f"no result recorded: boot {str(row['claimed_by'])[:8]} "
                         f"{'ended' if row['claimed_by'] != _boot() else 'lease expired'} mid-send")
                await s.execute(text(
                    "UPDATE kafka_notification_attempts SET ended_at = now(), outcome = 'unknown', error = :e, "
                    "detail = COALESCE(detail, '{}'::jsonb) || CAST(:det AS jsonb) "
                    "WHERE outbox_id = :o AND channel = :c AND outcome IS NULL"
                ), {"e": error, "o": row["id"], "c": c,
                    "det": json.dumps({"resend_after_unknown": c not in w["resend"]})})
                _apply(row, c, {"outcome": "unknown", "error": error}, now)
                count += 1
                logger.warning("outbox worker: row %s %s %s: outcome unknown (%s), %s", row["id"], row["kind"], c,
                               error, "abandoned" if row[f"{c}_status"] == "abandoned" else "resend once, labelled")
            _settle(row, now)
            await _write(s, row)
    return count


async def _claim(limit: int = BATCH) -> list:
    """(b) Claim up to limit due rows. Returns [(row, {channel: attempt_id})]."""
    claimed, settled = [], []
    async with _session() as s, s.begin():
        now = await _db_now(s)
        rows = await _rows(
            s, "finished_at IS NULL AND origin = 'outbox' AND next_attempt_at <= now() "
               "AND email_status <> 'sending' AND teams_status <> 'sending' "
               "AND (claim_expires_at IS NULL OR claim_expires_at < now()) "
               "ORDER BY priority, next_attempt_at, id LIMIT :n FOR UPDATE SKIP LOCKED", n=limit)
        for row in rows:
            if row["expires_at"] <= now:
                _expire(row, now)
                _settle(row, now)
                await _write(s, row)
                settled.append(row)
                continue
            due = [c for c in _CHANNELS if row[f"{c}_status"] == "pending" and (_due(row, c) or now) <= now]
            if due:
                due = await _not_waiting(s, row, due, now)
            if _email_first(row) and "email" in due:
                due = ["email"]  # Teams only after this email attempt, in the same run
            if not due:
                _settle(row, now)
                await _write(s, row)
                continue
            if "email" in due and not row["email_message_id"]:
                row["email_message_id"] = make_msgid()
            attempts = {c: await _start_attempt(s, row, c) for c in due}
            row["claimed_by"] = _boot()
            row["claim_expires_at"] = now + timedelta(seconds=LEASE_SECS)
            _settle(row, now)
            await _write(s, row)
            claimed.append((row, attempts))
    for row in settled:
        await _writeback(row)
    return claimed


_DEPENDENT_KINDS = ("rule_resolve", "rule_escalate", "rule_reminder")


async def _not_waiting(s, row: dict, due: list, now: datetime) -> list:
    """The due channels of row that need not wait. A rule resolve, escalate
    or reminder row waits for every earlier fire/escalate row sharing a
    trigger id to finish the same channel, at most DEPENDENCY_WAIT after its
    own creation; a waiting channel is looked at again in
    DEPENDENCY_POLL_SECS. No earlier row: nothing to wait for."""
    if (row["routing"] != "rule" or row["kind"] not in _DEPENDENT_KINDS or not row.get("trigger_ids")
            or now - row["created_at"] >= DEPENDENCY_WAIT):
        return due
    open_ = (await s.execute(text(
        "SELECT bool_or(email_status <> ALL(CAST(:done AS text[]))) AS email, "
        "bool_or(teams_status <> ALL(CAST(:done AS text[]))) AS teams "
        "FROM kafka_notification_outbox WHERE id < :id AND routing = 'rule' AND origin = 'outbox' "
        "AND kind IN ('rule_fire', 'rule_escalate') AND trigger_ids && CAST(:ids AS integer[])"
    ), {"id": row["id"], "ids": list(row["trigger_ids"]), "done": list(_DONE)})).mappings().one()
    waiting = [c for c in due if open_[c]]
    for c in waiting:
        _w(row)["due"][c] = (now + timedelta(seconds=DEPENDENCY_POLL_SECS)).isoformat()
    return [c for c in due if c not in waiting]


async def _writeback(row: dict) -> None:
    """A rule row's Teams channel has settled: the triggers in trigger_ids
    still showing queued_detail(row id) get teams_post_success and the
    reason. Idempotent; a failure is logged and changes nothing else."""
    status = row["teams_status"]
    if row["routing"] != "rule" or not row.get("trigger_ids") or status not in _DONE:
        return
    if status == "sent":
        ok, detail = True, None
    elif row["teams_last_error"] in (FALLBACK_NOT_NEEDED, FALLBACK_SUPERSEDED):
        ok, detail = False, TEAMS_OFF_RULE  # the rule's Teams toggle is off; its email went out
    else:
        ok, detail = False, row["teams_last_error"] or status
    try:
        async with _session() as s, s.begin():
            await s.execute(text(
                "UPDATE kafka_alert_triggers SET teams_post_success = :ok, error_detail = :d "
                "WHERE id = ANY(CAST(:ids AS integer[])) AND teams_post_success = false AND error_detail = :queued"
            ), {"ok": ok, "d": detail, "ids": list(row["trigger_ids"]), "queued": queued_detail(row["id"])})
    except Exception as exc:  # noqa: BLE001 -- never fails the send
        logger.warning("outbox worker: row %s: trigger write-back failed: %s", row["id"], outbox._exc_text(exc))


async def _claim_fallback(row_id: int):
    """The owner Teams fallback, claimed for the same run once email failed."""
    async with _session() as s, s.begin():
        now = await _db_now(s)
        rows = await _rows(s, "id = :id AND finished_at IS NULL FOR UPDATE", id=row_id)
        if not rows:
            return None, None
        row = rows[0]
        if row["teams_status"] != "pending" or (_due(row, "teams") or now) > now or row["email_status"] == "sent":
            return None, None
        attempt_id = await _start_attempt(s, row, "teams")
        row["claimed_by"] = _boot()
        row["claim_expires_at"] = now + timedelta(seconds=LEASE_SECS)
        _settle(row, now)
        await _write(s, row)
        return row, attempt_id


async def _release(items: list) -> None:
    """Give back claimed channels whose send never started (run ended)."""
    async with _session() as s, s.begin():
        now = await _db_now(s)
        for row_id, channel, attempt_id, boot in items:
            rows = await _rows(s, "id = :id FOR UPDATE", id=row_id)
            if not rows:
                continue
            row = rows[0]
            w = _w(row)
            if row[f"{channel}_status"] != "sending" or row["claimed_by"] != boot or w["inflight"].get(channel) != attempt_id:
                continue
            row[f"{channel}_status"] = "pending"
            row[f"{channel}_attempts"] -= 1
            w["inflight"].pop(channel, None)
            w["due"][channel] = now.isoformat()
            await s.execute(text("DELETE FROM kafka_notification_attempts WHERE id = :id"), {"id": attempt_id})
            _settle(row, now)
            await _write(s, row)


async def _supersede_older_summaries(s, row: dict, now: datetime) -> None:
    older = await _rows(s, "kind = 'owner_summary' AND finished_at IS NULL AND id < :id "
                           "ORDER BY id FOR UPDATE SKIP LOCKED", id=row["id"])
    for o in older:
        for c in _CHANNELS:
            if o[f"{c}_status"] in ("pending", "standby"):
                o[f"{c}_status"] = "superseded"
                o[f"{c}_last_error"] = f"superseded: a newer summary (row {row['id']}) was delivered"
                _w(o)["due"].pop(c, None)
        _settle(o, now)
        await _write(s, o)


async def _record(row_id: int, channel: str, attempt_id: int, boot: str, res: dict) -> bool:
    """(c) Record one attempt and, if the claim is still ours, the channel
    result. Returns True when the channel was updated."""
    detail = dict(res.get("detail") or {})
    if res.get("late"):
        detail["late_result"] = True
    if res.get("warning"):
        detail["warning"] = outbox.clean_error(res["warning"])
    async with _session() as s, s.begin():
        await _close_attempt(s, attempt_id, res["outcome"], res.get("error"), res.get("phase"),
                             res.get("duration_ms"), detail)
        now = await _db_now(s)
        rows = await _rows(s, "id = :id FOR UPDATE", id=row_id)
        row = rows[0] if rows else None
        if (row is None or row[f"{channel}_status"] != "sending" or row["claimed_by"] != boot
                or _w(row)["inflight"].get(channel) != attempt_id):
            logger.info("outbox worker: row %s %s: late result %s kept on attempt %s; the channel was reclaimed",
                        row_id, channel, res["outcome"], attempt_id)
            return False
        _apply(row, channel, res, now)
        _settle(row, now)
        updated = await _write(s, row, f"AND {channel}_status = 'sending' AND claimed_by = :boot", boot=boot)
        if updated and res["outcome"] == "sent" and row["kind"] == "owner_summary":
            await _supersede_older_summaries(s, row, now)
    if updated:
        await _writeback(row)
    log = logger.info if res["outcome"] == "sent" else logger.warning
    log("outbox worker: row %s %s %s: %s%s -> %s, row %s", row_id, row["kind"], channel, res["outcome"],
        f" ({outbox.clean_error(res.get('error'))})" if res.get("error") else "", row[f"{channel}_status"], row["state"])
    return bool(updated)


async def _note_cap(row_id: int, attempt_id: int, boot: str, phase: str, cap: float = EMAIL_CAP_SECS) -> None:
    """The email send outlived EMAIL_CAP_SECS: note it on the attempt (still
    in flight; the late result is recorded when the thread ends) and start
    the owner Teams fallback."""
    async with _session() as s, s.begin():
        await s.execute(text(
            "UPDATE kafka_notification_attempts SET detail = COALESCE(detail, '{}'::jsonb) || CAST(:det AS jsonb) "
            "WHERE id = :id"
        ), {"id": attempt_id, "det": json.dumps({"cap_fired_after_s": cap, "phase_at_cap": phase})})
        now = await _db_now(s)
        rows = await _rows(s, "id = :id FOR UPDATE", id=row_id)
        if not rows:
            return
        row = rows[0]
        if (row["routing"] == "owner" and row["email_status"] == "sending" and row["claimed_by"] == boot
                and row["teams_status"] == "standby"):
            _activate_fallback(row, now)
            _settle(row, now)
            await _write(s, row)
    logger.warning("outbox worker: row %s email: no result within %ss (phase %s); waiting for the late result",
                   row_id, cap, phase)


def _spawn(coro) -> None:
    task = asyncio.get_running_loop().create_task(coro)
    _late_tasks.add(task)
    task.add_done_callback(_late_tasks.discard)


async def _record_late(fut, row_id, channel, attempt_id, boot, t0) -> None:
    try:
        res = fut.result()
    except BaseException as exc:  # noqa: BLE001
        res = {"outcome": "failed_transient", "error": f"send failed ({type(exc).__name__})"}
    res = dict(res, late=True, duration_ms=int((time.monotonic() - t0) * 1000))
    try:
        await _record(row_id, channel, attempt_id, boot, res)
    except Exception as exc:  # noqa: BLE001
        logger.warning("outbox worker: row %s %s: late result not recorded: %s", row_id, channel, outbox._exc_text(exc))


def _record_when_done(fut, row_id, channel, attempt_id, boot, t0) -> None:
    _late_futures.add(fut)
    fut.add_done_callback(_late_futures.discard)
    fut.add_done_callback(lambda f: _spawn(_record_late(f, row_id, channel, attempt_id, boot, t0)))


# ── Email ────────────────────────────────────────────────────────────────────

def _smtp_outcome(exc: BaseException, phase: str) -> str:
    if phase == "data":
        if isinstance(exc, smtplib.SMTPResponseException):
            return "failed_permanent" if exc.smtp_code >= 500 else "failed_transient"
        return "unknown"  # connection lost while the message was going out
    if isinstance(exc, (smtplib.SMTPAuthenticationError, smtplib.SMTPSenderRefused, smtplib.SMTPRecipientsRefused,
                        smtplib.SMTPNotSupportedError, socket.gaierror, ssl.SSLError)):
        return "failed_config"
    if isinstance(exc, (socket.timeout, TimeoutError)) or isinstance(exc.__context__, (socket.timeout, TimeoutError)):
        return "timeout"
    return "failed_transient"


def _smtp_send_sync(host, port, username, password, from_address, recipients, message: bytes, phase: dict,
                    timeout: float) -> dict:
    """One SMTP session, step by step, in an outbox-smtp thread. phase["phase"]
    is the step reached. Never raises."""
    from shared.escalation.email_notifier import _short_error

    server = smtplib.SMTP(timeout=timeout)
    ok = False
    try:
        phase["phase"] = "connect"
        server.connect(host, port)
        phase["phase"] = "ehlo"
        server.ehlo()
        if server.has_extn("starttls"):
            phase["phase"] = "starttls"
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        if username and password and server.has_extn("auth"):
            phase["phase"] = "login"
            server.login(username, password)
        phase["phase"] = "mail"
        code, resp = server.mail(from_address)
        if code != 250:
            raise smtplib.SMTPSenderRefused(code, resp, "")
        phase["phase"] = "rcpt"
        refused = 0
        for address in recipients:
            code, _resp = server.rcpt(address)
            if code not in (250, 251):
                refused += 1
        if refused == len(recipients):
            raise smtplib.SMTPRecipientsRefused({})
        phase["phase"] = "data"
        server.data(message)
        phase["phase"] = "done"
        ok = True
        warning = f"{refused} recipient(s) refused by server" if refused else None
        return {"outcome": "sent", "warning": warning, "phase": "done",
                "detail": {"recipients": len(recipients) - refused}}
    except Exception as exc:  # noqa: BLE001 -- contract: never raise
        error = _short_error(exc, timeout)
        if password and password in error:
            error = error.replace(password, "***")
        return {"outcome": _smtp_outcome(exc, phase["phase"]), "error": error, "phase": phase["phase"]}
    finally:
        try:
            if ok and server.sock is not None:
                server.sock.settimeout(2)
                server.quit()
            else:
                server.close()
        except Exception:  # noqa: BLE001
            server.close()


def _email_target(row: dict, cfg: dict):
    """(recipients, None) or (None, result) from the live settings."""
    from shared.escalation.email_notifier import (MAX_RECIPIENTS, RULE_EMAIL_ALLOWED_DOMAINS, is_valid_address,
                                                  parse_recipients)

    host = str(cfg.get("smtp_host") or "").strip()
    from_address = str(cfg.get("smtp_from_address") or "").strip()
    if row["routing"] == "owner":
        raw = str(cfg.get("health_email_recipients") or "").strip()
        if not raw:
            return None, {"outcome": "skipped", "error": "health_email_recipients is blank (Teams-only)"}
        missing = [k for k, v in (("smtp_host", host), ("smtp_from_address", from_address)) if not v]
        if missing:
            return None, {"outcome": "skipped", "error": f"not configured: {', '.join(missing)} missing",
                          "config_problem": True}
        candidates = parse_recipients(raw)
    else:
        # A rule row with a policy (Teams fallback possible): "not configured"
        # is a skip that starts the fallback at once, not a retry at the cap.
        not_configured = ({"outcome": "skipped", "config_problem": True} if _policy(row)
                          else {"outcome": "failed_config"})
        if not cfg.get("email_enabled"):
            if _policy(row):
                return None, dict(not_configured, error="not configured: email_enabled (global) off")
            return None, {"outcome": "skipped", "error": "email_enabled (global) off"}
        candidates = parse_recipients(cfg.get("email_recipients") or "")
        missing = [k for k, v in (("email_recipients", candidates), ("smtp_host", host),
                                  ("smtp_from_address", from_address)) if not v]
        if missing:
            return None, dict(not_configured, error=f"not configured: {', '.join(missing)} missing")
        candidates = [a for a in candidates if a.lower().endswith(RULE_EMAIL_ALLOWED_DOMAINS)]
        if not candidates:
            return None, dict(not_configured, error="no allowed recipients")
    valid = [a for a in candidates if is_valid_address(a)][:MAX_RECIPIENTS]
    if not valid:
        return None, {"outcome": "failed_config", "error": "no valid recipients"}
    if not is_valid_address(from_address):
        return None, {"outcome": "failed_config", "error": "invalid from address"}
    try:
        int(cfg.get("smtp_port") or 587)
    except (TypeError, ValueError):
        return None, {"outcome": "failed_config", "error": "invalid SMTP port"}
    return valid, None


def _build_message(row: dict, from_address: str, recipients: list) -> bytes:
    from shared.escalation.email_notifier import _strip_crlf

    content = row["payload"].get("email") or {}
    subject = _strip_crlf(content.get("subject") or row["payload"].get("title") or row["kind"])
    if "email" in _w(row)["resend"]:
        subject = label_subject(subject)
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=False, usegmt=True)
    msg["Message-ID"] = row["email_message_id"]
    msg.set_content(content.get("text") or "", charset="utf-8")
    if content.get("html"):
        msg.add_alternative(content["html"], subtype="html", charset="utf-8")
    return msg.as_bytes(policy=SMTP_POLICY)


async def _send_email(row: dict, attempt_id: int, boot: str, cap: float = None):
    """The email result, or None when the cap (EMAIL_CAP_SECS unless given)
    fired and the late result will be recorded by a done-callback."""
    from routes_settings import _config

    cfg = dict(_config)
    if row["routing"] == "rule":
        result = await _rule_check(row, "email", cfg)
        if result is not None:
            return dict(result, phase="config")
    recipients, result = _email_target(row, cfg)
    if result is not None:
        return dict(result, phase="config")
    from_address = str(cfg.get("smtp_from_address") or "").strip()
    message = _build_message(row, from_address, recipients)
    phase = {"phase": "queued"}
    t0 = time.monotonic()
    fut = asyncio.get_running_loop().run_in_executor(_SMTP_POOL, functools.partial(
        _smtp_send_sync, str(cfg.get("smtp_host") or "").strip(), int(cfg.get("smtp_port") or 587),
        str(cfg.get("smtp_username") or ""), str(cfg.get("smtp_password") or ""), from_address, recipients,
        message, phase, SMTP_SOCKET_TIMEOUT_SECS,
    ))
    try:
        return await asyncio.wait_for(asyncio.shield(fut), cap or EMAIL_CAP_SECS)
    except asyncio.TimeoutError:
        _record_when_done(fut, row["id"], "email", attempt_id, boot, t0)
        await _note_cap(row["id"], attempt_id, boot, phase["phase"], cap or EMAIL_CAP_SECS)
        return None
    except asyncio.CancelledError:
        _record_when_done(fut, row["id"], "email", attempt_id, boot, t0)
        raise


# ── Teams ────────────────────────────────────────────────────────────────────

async def _webhook(row: dict, cfg: dict):
    """(url, None) or (None, result), resolved now from the stored reference."""
    default = str(cfg.get("teams_webhook_url") or "")
    if row["routing"] == "owner":
        if not (cfg.get("teams_enabled") and default):
            return None, {"outcome": "failed_config", "error": "not configured: teams_enabled off or no teams_webhook_url"}
        return default, None
    if not cfg.get("teams_enabled"):
        return None, {"outcome": "skipped", "error": TEAMS_OFF_GLOBAL}
    ref = str(row["payload"].get("teams_ref") or "agent_default")
    url = default
    if ref.startswith("rule:"):
        async with _session() as s:
            found = (await s.execute(text("SELECT webhook_url FROM kafka_alert_configs WHERE id = :id"),
                                     {"id": int(ref[5:])})).first()
        if found is None:
            return None, {"outcome": "skipped", "error": f"rule {ref[5:]} no longer exists"}
        url = found[0] or default
    if not url:
        if _rule_fallback(row):
            return None, {"outcome": "skipped", "config_problem": True,
                          "error": "no webhook URL configured (per-alert or agent-level)"}
        return None, {"outcome": "failed_config", "error": "no webhook URL configured (per-alert or agent-level)"}
    return url, None


async def _send_teams(row: dict) -> dict:
    from routes_settings import _config

    cfg = dict(_config)
    if row["routing"] == "rule":
        result = await _rule_check(row, "teams", cfg)
        if result is None and row["kind"] == "rule_resolve":
            result = await _resolve_gate(row)
        if result is not None:
            return dict(result, phase="config")
    url, result = await _webhook(row, cfg)
    if result is not None:
        return dict(result, phase="config")
    card = row["payload"].get("teams_card") or {}
    if "teams" in _w(row)["resend"]:
        card = label_card(card)
    try:
        async with httpx.AsyncClient(timeout=TEAMS_TIMEOUT) as client:
            resp = await client.post(url, json=card)
    except httpx.ConnectTimeout:
        return {"outcome": "timeout", "error": "connect timed out after 5s", "phase": "connect"}
    except (httpx.ConnectError, httpx.PoolTimeout) as exc:
        return {"outcome": "failed_transient", "error": f"connect failed ({type(exc).__name__})", "phase": "connect"}
    except httpx.TransportError as exc:  # request may have reached Teams
        return {"outcome": "unknown", "error": f"no response ({type(exc).__name__}); may have been delivered",
                "phase": "post"}
    except Exception as exc:  # noqa: BLE001
        return {"outcome": "failed_transient", "error": f"post failed ({type(exc).__name__})", "phase": "post"}
    outcome_ = http_outcome(resp.status_code)
    res = {"outcome": outcome_, "phase": "done" if outcome_ == "sent" else "response",
           "detail": {"http_status": resp.status_code}}
    if outcome_ != "sent":
        res["error"] = f"HTTP {resp.status_code}"
        res["retry_after"] = parse_retry_after(resp.headers.get("Retry-After"))
    return res


# ── Rule rows: send-time checks ───────────────────────────────────────────────

async def _rule_check(row: dict, channel: str, cfg: dict):
    """A skip result when this rule row's channel must not go out now, else
    None. Reads the rule (policy rows) and the settings at send time: rule
    deleted -> both channels skipped; its Email toggle off -> email skipped;
    its Teams toggle off -> Teams skipped unless the card is the fallback;
    teams_enabled off -> Teams skipped, fallback included."""
    policy = _policy(row)
    if channel == "teams" and not cfg.get("teams_enabled"):
        return {"outcome": "skipped", "error": TEAMS_OFF_GLOBAL}
    if policy is None:
        return None
    async with _session() as s:
        rule = (await s.execute(text("SELECT enabled, email_enabled FROM kafka_alert_configs WHERE id = :id"),
                                {"id": int(policy.get("rule_id") or 0)})).mappings().first()
    if rule is None:
        gone = f"rule {policy.get('rule_id')} no longer exists"
        return {"outcome": "skipped", "error": gone, "teams_reason": gone}
    if channel == "email":
        if not rule["email_enabled"]:
            return {"outcome": "skipped", "error": RULE_EMAIL_OFF, "teams_reason": TEAMS_OFF_RULE}
        return None
    if _rule_fallback(row):
        if not cfg.get("rule_teams_fallback", True):
            return {"outcome": "skipped", "error": TEAMS_OFF_NO_FALLBACK}
        return None
    if not rule["enabled"]:
        return {"outcome": "skipped", "error": TEAMS_OFF_RULE}
    return None


async def _resolve_gate(row: dict):
    """A resolve card goes out only if the opening card did: an earlier
    outbox fire row sharing a trigger id with its Teams channel sent; with
    no such row (the fire went inline), the trigger's own teams_post_success.
    No trigger ids: no gate."""
    ids = list(row.get("trigger_ids") or [])
    if not ids:
        return None
    async with _session() as s:
        parents = (await s.execute(text(
            "SELECT teams_status FROM kafka_notification_outbox WHERE id < :id AND routing = 'rule' "
            "AND origin = 'outbox' AND kind = 'rule_fire' AND trigger_ids && CAST(:ids AS integer[])"
        ), {"id": row["id"], "ids": ids})).scalars().all()
        if parents:
            reached = "sent" in parents
        else:
            reached = bool((await s.execute(text(
                "SELECT bool_or(teams_post_success) FROM kafka_alert_triggers WHERE id = ANY(CAST(:ids AS integer[]))"
            ), {"ids": ids})).scalar())
    return None if reached else {"outcome": "skipped", "error": NO_OPENING_CARD}


# ── The run ──────────────────────────────────────────────────────────────────

async def _send_channel(row: dict, channel: str, attempt_id: int, boot: str, email_cap: float = None) -> None:
    t0 = time.monotonic()
    try:
        res = await (_send_email(row, attempt_id, boot, email_cap) if channel == "email" else _send_teams(row))
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        res = {"outcome": "failed_transient", "error": f"send failed ({type(exc).__name__})"}
    if res is None:
        return
    res = dict(res, duration_ms=int((time.monotonic() - t0) * 1000))
    await _record(row["id"], channel, attempt_id, boot, res)


async def _process(row: dict, attempts: dict, boot: str, started: set, email_cap: float = None) -> None:
    for channel in _CHANNELS:
        if channel in attempts:
            started.add((row["id"], channel))
            await _send_channel(row, channel, attempts[channel], boot, email_cap)
    if _email_first(row):
        fresh, attempt_id = await _claim_fallback(row["id"])
        if fresh is not None:
            started.add((row["id"], "teams"))
            await _send_channel(fresh, "teams", attempt_id, boot)


async def run_once(claim_window: float = None) -> dict:
    """One delivery run (a-d). Returns counts."""
    window = CLAIM_WINDOW_SECS if claim_window is None else claim_window
    t0 = time.monotonic()
    stats = {"recovered": await _recover(), "claimed": 0}
    while time.monotonic() - t0 < window:
        batch = await _claim(BATCH)
        if not batch:
            break
        stats["claimed"] += len(batch)
        boot = _boot()
        started: set = set()
        try:
            for row, attempts in batch:
                await _process(row, attempts, boot, started)
        finally:
            unstarted = [(row["id"], c, aid, boot) for row, attempts in batch for c, aid in attempts.items()
                         if (row["id"], c) not in started]
            if unstarted:
                await asyncio.shield(_release(unstarted))
    if outbox.owner_via_outbox():
        try:
            stats["deadman"] = await _deadman_check()
        except Exception as exc:  # noqa: BLE001 -- never stops delivery
            logger.warning("outbox dead-man check failed: %s", outbox._exc_text(exc))
    global _last_success
    _last_success = time.time()
    if stats["recovered"] or stats["claimed"] or stats.get("deadman"):
        logger.info("outbox worker: run done in %.1fs: %s", time.monotonic() - t0, stats)
    return stats


async def deliver_due_notifications() -> None:
    """Job handler (cron, manual run, and the wrapper's retry)."""
    async with _lock:
        await run_once()


async def _kick_loop() -> None:
    global _run_again
    try:
        while _run_again:
            _run_again = False
            async with _lock:
                await run_once()
    except Exception as exc:  # noqa: BLE001 -- the cron job is the safety net
        logger.warning("outbox worker: kicked run failed: %s", outbox._exc_text(exc))


def kick() -> None:
    """Start a run now (after an enqueue commit); if one is running, run
    again when it ends. Never raises."""
    global _run_again, _kick_task
    try:
        _run_again = True
        if _kick_task is None or _kick_task.done():
            _kick_task = asyncio.get_running_loop().create_task(_kick_loop())
    except Exception as exc:  # noqa: BLE001
        logger.warning("outbox worker: kick failed: %s", type(exc).__name__)


async def drain(timeout: float = 60) -> None:
    """Wait for a kicked run, sends still in flight and their late results
    (tests, shutdown)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pending = list(_late_futures) + list(_late_tasks)
        if _kick_task is not None and not _kick_task.done():
            pending.append(_kick_task)
        if not pending:
            return
        await asyncio.wait(pending, timeout=max(0.0, deadline - time.monotonic()))
        await asyncio.sleep(0)


def last_success_age() -> float:
    """Seconds since the last completed run (or since startup)."""
    return time.time() - (_last_success or _started_at)


# ── Enqueue ──────────────────────────────────────────────────────────────────

_INSERT_SQL = (
    "INSERT INTO kafka_notification_outbox (kind, source, routing, origin, dedupe_key, priority, "
    "alert_config_id, cluster_id, trigger_ids, severity, payload, email_status, teams_status, "
    "email_last_error, teams_last_error, email_message_id, next_attempt_at, expires_at, state) "
    "VALUES (:kind, :source, :routing, 'outbox', :dedupe_key, :priority, :alert_config_id, :cluster_id, "
    ":trigger_ids, :severity, CAST(:payload AS jsonb), :email_status, :teams_status, :email_skip, "
    ":teams_skip, :message_id, now(), now() + make_interval(secs => :max_age), 'pending') "
    "ON CONFLICT (dedupe_key) DO NOTHING RETURNING id"
)


def _row_values(kind, routing, dedupe_key, *, email=None, teams_card=None, teams_ref=None, severity=None,
                alert_config_id=None, cluster_id=None, trigger_ids=None, source="", payload=None,
                policy=None, rule_name=None) -> dict:
    """Parameters of _INSERT_SQL for one pending row. A channel with no
    content is skipped; the owner Teams card waits in standby behind email.
    A rule row with a policy follows the rule's toggles (module docstring):
    Email off -> email skipped "rule email off"; Teams off with Email on ->
    the card waits in standby as the fallback."""
    email_status = "pending" if email else "skipped"
    email_skip = None if email else "no email content"
    teams_skip = None if teams_card else "no card content"
    if not teams_card:
        teams_status = "skipped"
    elif routing == "owner" and email:
        teams_status = "standby"
    else:
        teams_status = "pending"
    if routing == "rule" and policy is not None:
        if email and not policy.get("email_toggle"):
            email_status, email_skip = "skipped", RULE_EMAIL_OFF
        if teams_card and not policy.get("teams_toggle"):
            if email_status == "pending":
                teams_status = "standby"
            else:
                teams_status, teams_skip = "skipped", TEAMS_OFF_RULE
    body = {"title": outbox.clean_error((email or {}).get("subject") or kind), "mode": "on",
            "boot_id": _boot(), **(payload or {})}
    if email:
        body["email"] = email
    if teams_card:
        body["teams_card"] = teams_card
        body["teams_ref"] = teams_ref or (f"rule:{policy['rule_id']}" if policy else "agent_default")
    if policy is not None:
        body["policy"] = {"rule_id": policy.get("rule_id"), "teams_toggle": bool(policy.get("teams_toggle")),
                          "email_toggle": bool(policy.get("email_toggle"))}
        body["rule_name"] = rule_name
    return {
        "kind": kind, "source": source or "outbox", "routing": routing, "dedupe_key": dedupe_key,
        "priority": outbox.priority(kind, severity), "alert_config_id": alert_config_id,
        "cluster_id": cluster_id, "trigger_ids": list(trigger_ids) if trigger_ids else None,
        "severity": severity, "payload": json.dumps(body, default=str),
        "email_status": email_status, "teams_status": teams_status,
        "email_skip": email_skip if email_status == "skipped" else None,
        "teams_skip": teams_skip if teams_status == "skipped" else None,
        "message_id": make_msgid() if email_status == "pending" else None,
        "max_age": outbox._MAX_AGE.get(kind, timedelta(hours=24)).total_seconds(),
    }


def _positional(sql: str, values: dict) -> tuple:
    """:name placeholders as $n for raw asyncpg."""
    names: list = []

    def sub(m):
        if m.group(1) not in names:
            names.append(m.group(1))
        return f"${names.index(m.group(1)) + 1}"

    return re.sub(r"(?<![:\w]):([a-z_]+)", sub, sql), [values[n] for n in names]


async def enqueue(kind, routing, dedupe_key, *, email=None, teams_card=None, teams_ref=None, severity=None,
                  alert_config_id=None, cluster_id=None, trigger_ids=None, source="", payload=None,
                  kick_after=True):
    """Insert one outbox-driven row (pending) and kick the worker after the
    commit. email is {"subject", "text", "html"}; teams_card the card;
    teams_ref "rule:<id>" or "agent_default" (rule routing). Returns the row
    id, or None when dedupe_key exists. Raises on a database failure."""
    values = _row_values(kind, routing, dedupe_key, email=email, teams_card=teams_card, teams_ref=teams_ref,
                         severity=severity, alert_config_id=alert_config_id, cluster_id=cluster_id,
                         trigger_ids=trigger_ids, source=source, payload=payload)
    async with _session() as s, s.begin():
        row_id = (await s.execute(text(_INSERT_SQL), values)).scalar_one_or_none()
    if row_id is not None and kick_after:
        kick()
    return row_id


async def enqueue_in(session, kind, dedupe_key, *, policy, email=None, teams_card=None, rule_name=None,
                     severity=None, cluster_id=None, trigger_ids=None, source="", payload=None, teams_ref=None):
    """Insert one rule row (routing "rule", pending) on the CALLER's session:
    same transaction as the caller's trigger write, so both commit or roll
    back together. Never commits and never kicks: call kick() after the
    caller's commit. policy is {"rule_id", "teams_toggle", "email_toggle"};
    with both toggles off nothing is inserted. Returns the row id, or None
    (both off, or dedupe_key exists). Raises on a database failure."""
    if not policy.get("teams_toggle") and not policy.get("email_toggle"):
        return None
    values = _row_values(kind, "rule", dedupe_key, email=email, teams_card=teams_card, teams_ref=teams_ref,
                         severity=severity, alert_config_id=policy.get("rule_id"), cluster_id=cluster_id,
                         trigger_ids=trigger_ids, source=source, payload=payload, policy=policy,
                         rule_name=rule_name)
    return (await session.execute(text(_INSERT_SQL), values)).scalar_one_or_none()


def rule_dedupe_key(kind: str, *, trigger_id=None, tier=None, eval_ts=None, config_id=None,
                    grouped: bool = False) -> str:
    """The dedupe key of a rule row. One per trigger for a fire or resolve,
    per trigger, tier and evaluator run for an escalation; a grouped (lag)
    notice is one per rule and run."""
    ts = eval_ts.isoformat() if isinstance(eval_ts, datetime) else eval_ts
    if grouped:
        if kind == "rule_resolve":
            return f"lag_resolve:{config_id}:{ts}"
        return f"lag:{config_id}:{kind.removeprefix('rule_')}:{ts}"
    if kind == "rule_escalate":
        return f"rule_escalate:{trigger_id}:{tier}:{ts}"
    if kind in ("rule_fire", "rule_resolve"):
        return f"{kind}:{trigger_id}"
    raise ValueError(f"no per-trigger dedupe key for {kind}")


def owner_content(severity: str, category: str, description: str, subject: str = None) -> tuple:
    """(email dict, Teams card) of an agent-wide owner notice, built like the
    watchdog notices."""
    from shared.escalation.email_notifier import build_watchdog_email
    from shared.escalation.notifier import build_adaptive_card

    card = build_adaptive_card(
        agent_name="Kafka Analyser", cluster_name="(agent-wide, not cluster-specific)",
        anomaly={"severity": severity, "category": category, "description": description, "recommended_action": ""},
    )
    default_subject, text_body, html_body = build_watchdog_email("Kafka Analyser", severity, category, description)
    return {"subject": subject or default_subject, "text": text_body, "html": html_body}, card


# ── Watchdog notices: durable before the exit ────────────────────────────────

def _raw_dsn() -> str:
    from config import settings
    return (settings.database_url or "").replace("postgresql+asyncpg://", "postgresql://")


async def insert_claimed_with_event(event: tuple, dedupe_key: str, *, email, teams_card, severity, source,
                                    kind="owner_watchdog", payload=None) -> tuple:
    """ONE raw asyncpg transaction (1 s connect timeout, 1 s statement
    timeout): the kafka_watchdog_events row (event = (watchdog, reason,
    details)) and the owner outbox row, already claimed by this boot for its
    first channel, with that channel's attempt row -- so the worker cannot
    send it too. Returns (row_id, {channel: attempt_id}), or (None, {}) when
    the dedupe key exists. Raises on any failure; nothing is written then."""
    import asyncpg

    dsn = _raw_dsn()
    if not dsn:
        raise RuntimeError("database not configured")
    sql, args = _positional(_INSERT_SQL, _row_values(kind, "owner", dedupe_key, email=email, teams_card=teams_card,
                                                     severity=severity, source=source, payload=payload))
    channel, boot = ("email" if email else "teams"), _boot()
    watchdog, reason, details = event
    conn = await asyncpg.connect(dsn, timeout=1)
    try:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = 1000")
            await conn.execute("INSERT INTO kafka_watchdog_events (watchdog, reason, details) VALUES ($1, $2, $3::jsonb)",
                               watchdog, reason, json.dumps(details, default=str))
            row_id = await conn.fetchval(sql, *args)
            if row_id is None:
                return None, {}
            attempt_id = await conn.fetchval(
                "INSERT INTO kafka_notification_attempts (outbox_id, channel, attempt_no, boot_id, started_at) "
                "VALUES ($1, $2, 1, $3, now()) RETURNING id", row_id, channel, boot)
            worker = {"due": {}, "inflight": {channel: attempt_id}, "resend": [], "config_problem": {}}
            await conn.execute(
                f"UPDATE kafka_notification_outbox SET payload = jsonb_set(payload, '{{worker}}', $2::jsonb), "
                f"{channel}_status = 'sending', {channel}_attempts = 1, claimed_by = $3, "
                f"claim_expires_at = now() + make_interval(secs => $4) WHERE id = $1",
                row_id, json.dumps(worker), boot, float(LEASE_SECS))
        return row_id, {channel: attempt_id}
    finally:
        conn.terminate()


async def send_claimed(row_id: int, attempts: dict, budget: float, email_cap: float = None) -> bool:
    """Send a row claimed by insert_claimed_with_event now, in the caller's
    task, within budget seconds (the owner Teams fallback included). An email
    still in flight at the end is recorded by its done-callback if the
    process lives; otherwise the next boot resends it once, labelled. True
    when the send finished in time. Never raises."""
    boot = _boot()

    async def go():
        async with _session() as s:
            rows = await _rows(s, "id = :id", id=row_id)
        await _process(rows[0], attempts, boot, set(), email_cap)

    try:
        await asyncio.wait_for(go(), timeout=max(0.05, budget))
        return True
    except asyncio.TimeoutError:
        logger.warning("outbox: row %s not finished within %.1fs; the next boot completes it", row_id, budget)
    except Exception as exc:  # noqa: BLE001
        logger.warning("outbox: row %s inline send failed: %s", row_id, outbox._exc_text(exc))
    return False


def spool_entry(dedupe_key: str, source: str, severity: str, title: str, event: tuple, report: dict) -> dict:
    """One spool line: what the inline send did, and the event. No address,
    password or URL (errors are cleaned like the outbox rows)."""
    channels = {}
    for channel in _CHANNELS:
        r = outbox.from_report((report or {}).get(channel))
        channels[channel] = {
            "status": r["status"],
            "error": outbox.clean_error(r.get("error")),
            "reason": outbox.clean_error(r.get("reason")),
            "warning": outbox.clean_error(r.get("warning")),
            "duration_ms": r.get("duration_ms"),
            "started_at": r["started_at"].isoformat() if isinstance(r.get("started_at"), datetime) else None,
            "config_problem": bool(r.get("config_problem")),
            "http_status": (r.get("detail") or {}).get("http_status"),
        }
    watchdog, reason, details = event
    return {"v": 1, "at": datetime.now(timezone.utc).isoformat(), "boot_id": _boot(), "kind": "owner_watchdog",
            "dedupe_key": dedupe_key, "source": source, "severity": severity, "title": outbox.clean_error(title),
            "event": {"watchdog": watchdog, "reason": reason, "details": details}, "channels": channels}


def append_spool(entry: dict, path: str = None) -> bool:
    """Append one JSON line to the spool file (fsync'd). Never raises."""
    try:
        with open(path or SPOOL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("outbox: spool write failed: %s", type(exc).__name__)
        return False


def _spooled_result(r: dict) -> dict:
    started_at = datetime.fromisoformat(r["started_at"]) if r.get("started_at") else None
    detail = {"http_status": r["http_status"]} if r.get("http_status") else None
    if r.get("status") == "sent":
        return outbox.sent(r.get("duration_ms"), started_at, r.get("warning"), detail)
    if r.get("status") == "skipped":
        return outbox.skipped(r.get("reason") or "no reason given", bool(r.get("config_problem")))
    return outbox.failed(r.get("error") or "failed", r.get("duration_ms"), started_at, detail,
                         bool(r.get("config_problem")))


async def ingest_spool(path: str = None) -> int:
    """Record each spooled watchdog notice (an inline outbox record and its
    kafka_watchdog_events row), deduped on the key, then remove the file. A
    database failure stops the ingest and keeps the file for the next start."""
    path = path or SPOOL_PATH
    if not os.path.exists(path):
        return 0
    with open(path, encoding="utf-8") as f:
        lines = [line for line in f.read().splitlines() if line.strip()]
    added = 0
    for line in lines:
        try:
            e = json.loads(line)
            results = {c: _spooled_result((e.get("channels") or {}).get(c) or {}) for c in _CHANNELS}
            at = datetime.fromisoformat(e["at"])
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning("outbox: spool line skipped: %s", type(exc).__name__)
            continue
        row, attempts = outbox.build_row(
            e.get("kind") or "owner_watchdog", "owner", e["dedupe_key"], None, None, None, e.get("severity"),
            e.get("title"), results, source=e.get("source") or "spool", now=at,
            payload={"spooled": True, "spool_boot_id": e.get("boot_id")},
        )
        async with _session() as s, s.begin():
            row_id = await outbox._insert(s, row, attempts)
            if row_id is not None:
                ev = e.get("event") or {}
                await s.execute(text(
                    "INSERT INTO kafka_watchdog_events (created_at, watchdog, reason, details) "
                    "VALUES (:at, :w, :r, CAST(:d AS jsonb))"
                ), {"at": at, "w": ev.get("watchdog") or "unknown", "r": ev.get("reason") or "unknown",
                    "d": json.dumps(dict(ev.get("details") or {}, spooled=True), default=str)})
                added += 1
    os.remove(path)
    logger.info("outbox: spool ingested: %d new of %d line(s)", added, len(lines))
    return added


# ── Startup and dead-man ─────────────────────────────────────────────────────

async def _startup_check() -> str:
    """Mode on: one owner notice when the previous run ended without a
    watchdog event or left abandoned notifications unreported."""
    if not outbox.owner_via_outbox():
        return "skipped (mode not on)"
    async with _session() as s:
        now = await _db_now(s)
        pending = (await s.execute(text(
            "SELECT count(*) FROM kafka_notification_outbox WHERE origin = 'outbox' AND finished_at IS NULL"
        ))).scalar_one()
        abandoned = (await s.execute(text(
            "SELECT count(*) FROM kafka_notification_outbox WHERE origin = 'outbox' AND reported_at IS NULL "
            "AND (email_status = 'abandoned' OR teams_status = 'abandoned')"
        ))).scalar_one()
        prev_end = (await s.execute(text(
            "SELECT max(GREATEST(started_at, COALESCE(ended_at, started_at))) FROM kafka_job_runs "
            "WHERE started_at < to_timestamp(:t)"
        ), {"t": _started_at})).scalar_one()
        last_event = (await s.execute(text(
            "SELECT max(created_at) FROM kafka_watchdog_events WHERE watchdog <> :clean"
        ), {"clean": CLEAN_SHUTDOWN})).scalar_one()
        clean_stop = (await s.execute(text(
            "SELECT max(created_at) FROM kafka_watchdog_events WHERE watchdog = :clean "
            "AND created_at > now() - make_interval(mins => :m)"
        ), {"clean": CLEAN_SHUTDOWN, "m": CLEAN_SHUTDOWN_WINDOW_MINUTES})).scalar_one()
    # A planned stop (clean_shutdown marker within the window) or a watchdog
    # restart (its event near the end of the previous run) is expected.
    unexplained = prev_end is not None and clean_stop is None and (
        last_event is None or last_event < prev_end - timedelta(minutes=10))
    if not unexplained and not abandoned:
        return f"normal ({pending} pending)"
    ended = (f"about {prev_end:%Y-%m-%d %H:%M} UTC ({(now - prev_end).total_seconds() / 60:.0f} min before)"
             if prev_end is not None else "at an unknown time")
    description = (f"Agent restarted at {now:%Y-%m-%d %H:%M} UTC; previous activity ended {ended}; "
                   f"{pending} pending, {abandoned} abandoned notification(s).")
    if unexplained:
        description += (" No watchdog event was recorded for the previous run (a deploy, a crash or an "
                        "out-of-memory kill ends a run that way).")
    email, card = owner_content(
        "warning", "agent_restart", description,
        subject="[ISSUE] Kafka Analyser restarted " + ("unexpectedly" if unexplained else "with undelivered notifications"),
    )
    row_id = await enqueue("deadman", "owner", f"deadman:startup:{_boot()}", email=email, teams_card=card,
                           severity="warning", source="startup_check", kick_after=False)
    return f"notice enqueued (row {row_id})"


CLEAN_SHUTDOWN = "clean_shutdown"
CLEAN_SHUTDOWN_WINDOW_MINUTES = 10


async def write_clean_shutdown_marker(timeout: float = 1.0) -> bool:
    """Lifespan shutdown: one kafka_watchdog_events row (watchdog
    "clean_shutdown") so the next startup sees a planned stop. A watchdog
    os._exit(1) or a crash never gets here. Raw asyncpg, capped at timeout
    seconds in all; never raises. True when written."""
    async def write():
        import asyncpg
        dsn = _raw_dsn()
        if not dsn:
            raise RuntimeError("database not configured")
        conn = await asyncpg.connect(dsn, timeout=timeout)
        try:
            await conn.execute("INSERT INTO kafka_watchdog_events (watchdog, reason, details) VALUES ($1, $2, $3::jsonb)",
                               CLEAN_SHUTDOWN, "lifespan_shutdown", json.dumps({"boot_id": _boot()}))
        finally:
            conn.terminate()

    try:
        await asyncio.wait_for(write(), timeout=timeout)
        logger.info("outbox: clean shutdown marker written")
        return True
    except Exception as exc:  # noqa: BLE001 -- best effort; shutdown goes on
        logger.warning("outbox: clean shutdown marker not written: %s", outbox._exc_text(exc))
        return False


async def startup() -> dict:
    """Lifespan hook, after the config load and before the scheduler starts:
    a new boot_id, recovery of orphaned claims, the spool ingest, the startup
    check, then a kick. Each step is capped and never stops the startup."""
    global _started_at
    outbox.BOOT_ID = uuid.uuid4().hex
    _started_at = time.time()
    result = {"boot_id": outbox.BOOT_ID}
    for name, step in (("recovered", _recover), ("spool", ingest_spool), ("check", _startup_check)):
        try:
            result[name] = await asyncio.wait_for(step(), timeout=15)
        except Exception as exc:  # noqa: BLE001
            logger.warning("outbox startup: %s failed: %s", name, outbox._exc_text(exc))
            result[name] = "failed"
    kick()
    logger.info("outbox startup: %s", result)
    return result


def _deadman_line(r) -> str:
    def channel(c):
        status, error = r[f"{c}_status"], r[f"{c}_last_error"]
        return f"{c} {status}" + (f" ({error})" if error and status not in ("sent", "skipped") else "")
    return f"row {r['id']} {r['kind']}, created {r['created_at']:%H:%M} UTC: {channel('email')}, {channel('teams')}"


async def _deadman_check() -> int:
    """Mode on, every run: rows unfinished for over DEADMAN_UNFINISHED_MINUTES
    or with an abandoned channel, not yet reported, become one deadman owner
    notice per hour; the rows in it are marked reported_at. Returns the
    number reported."""
    async with _session() as s, s.begin():
        rows = (await s.execute(text(
            "SELECT id, kind, email_status, teams_status, email_last_error, teams_last_error, created_at "
            "FROM kafka_notification_outbox WHERE origin = 'outbox' AND reported_at IS NULL AND ("
            "(finished_at IS NULL AND created_at < now() - make_interval(mins => :m)) "
            "OR email_status = 'abandoned' OR teams_status = 'abandoned') "
            "ORDER BY id LIMIT :n FOR UPDATE SKIP LOCKED"
        ), {"m": DEADMAN_UNFINISHED_MINUTES, "n": DEADMAN_ROW_LIMIT})).mappings().all()
        if not rows:
            return 0
        now = await _db_now(s)
        description = (f"{len(rows)} notification(s) not delivered (unfinished for over "
                       f"{DEADMAN_UNFINISHED_MINUTES} min, or a channel abandoned):\n"
                       + "\n".join(_deadman_line(r) for r in rows))
        email, card = owner_content("critical", "notification_delivery", description,
                                    subject=f"[ISSUE] Kafka Analyser: {len(rows)} notification(s) not delivered")
        row_id = (await s.execute(text(_INSERT_SQL), _row_values(
            "deadman", "owner", f"deadman:{now:%Y-%m-%dT%H}", email=email, teams_card=card, severity="critical",
            source="deadman_check"))).scalar_one_or_none()
        if row_id is None:
            return 0  # this hour's notice exists; these rows go into the next one
        await s.execute(text("UPDATE kafka_notification_outbox SET reported_at = now() WHERE id = ANY(:ids)"),
                        {"ids": [r["id"] for r in rows]})
    logger.warning("outbox dead-man: %d undelivered notification(s) reported in row %s", len(rows), row_id)
    kick()
    return len(rows)
