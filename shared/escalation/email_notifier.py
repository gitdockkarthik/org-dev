"""SMTP email sender for escalation notifications.

Plain smtplib run in a worker thread so the event loop is never blocked.
Designed for an internal relay (mail.operative.com:25) that offers neither
STARTTLS nor AUTH, but uses both when a server offers them:

  * STARTTLS only if the server advertises it.
  * Login only if a username AND password are configured AND the server
    advertises AUTH.

send_email() never raises and never logs or returns the password. It returns
(ok, message): on failure message is a short error string; on success it is
None, or a short warning (e.g. invalid recipients were dropped).
"""
from __future__ import annotations

import asyncio
import html
import logging
import re
import smtplib
import socket
import ssl
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Iterable, Optional, Tuple, Union

logger = logging.getLogger(__name__)

SMTP_TIMEOUT_SECS = 10
MAX_RECIPIENTS = 20

# Deliberately simple: one "@", no whitespace or header/list punctuation in
# the local part, and a dotted domain. Not RFC 5322 -- just enough to catch
# typos and anything that could smuggle extra headers or recipients.
_ADDRESS_RE = re.compile(r"^[^@\s<>,;:\"()\[\]\\]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$")


def _strip_crlf(value) -> str:
    """Neutralise header injection: CR and LF become spaces."""
    return str(value or "").replace("\r", " ").replace("\n", " ").strip()


def is_valid_address(address: str) -> bool:
    address = _strip_crlf(address)
    return len(address) <= 254 and bool(_ADDRESS_RE.match(address))


def parse_recipients(recipients: Union[str, Iterable[str], None]) -> list:
    """Accept a comma/semicolon-separated string (the email_recipients
    setting format) or a list. Returns stripped, non-empty, de-duplicated
    entries in order; validity is checked separately."""
    if recipients is None:
        return []
    if isinstance(recipients, str):
        items = re.split(r"[,;]", recipients)
    else:
        items = list(recipients)
    out, seen = [], set()
    for item in items:
        addr = _strip_crlf(item)
        if addr and addr.lower() not in seen:
            seen.add(addr.lower())
            out.append(addr)
    return out


def _short_error(exc: BaseException, timeout: float) -> str:
    # smtplib re-raises a read timeout as SMTPServerDisconnected; the original
    # socket timeout is kept as the implicit exception context.
    if isinstance(exc, (socket.timeout, TimeoutError)) or isinstance(exc.__context__, (socket.timeout, TimeoutError)):
        return f"timed out after {timeout:g}s"
    if isinstance(exc, smtplib.SMTPAuthenticationError):
        return f"authentication failed (SMTP {exc.smtp_code})"
    if isinstance(exc, smtplib.SMTPRecipientsRefused):
        return "all recipients refused by server"
    if isinstance(exc, smtplib.SMTPSenderRefused):
        return f"sender address refused (SMTP {exc.smtp_code})"
    if isinstance(exc, smtplib.SMTPServerDisconnected):
        return "server disconnected unexpectedly"
    if isinstance(exc, smtplib.SMTPResponseException):
        return f"SMTP error {exc.smtp_code}"
    if isinstance(exc, smtplib.SMTPNotSupportedError):
        return "server does not support a required command"
    if isinstance(exc, ssl.SSLError):
        return "STARTTLS failed (TLS error)"
    if isinstance(exc, socket.gaierror):
        return "could not resolve SMTP host"
    if isinstance(exc, ConnectionRefusedError):
        return "connection refused"
    if isinstance(exc, OSError):
        return f"connection error ({type(exc).__name__})"
    return f"send failed ({type(exc).__name__})"


def _build_message(from_address, recipients, subject, text_body, html_body) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=False, usegmt=True)
    msg["Message-ID"] = make_msgid()
    msg.set_content(text_body or "", charset="utf-8")
    msg.add_alternative(html_body or "", subtype="html", charset="utf-8")
    return msg


def _send_sync(host, port, username, password, from_address, recipients, msg, timeout) -> dict:
    with smtplib.SMTP(host, port, timeout=timeout) as server:
        server.ehlo()
        if server.has_extn("starttls"):
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        if username and password and server.has_extn("auth"):
            server.login(username, password)
        return server.send_message(msg, from_addr=from_address, to_addrs=recipients)


async def send_email(
    host: str,
    port: int,
    username: str,
    password: str,
    from_address: str,
    recipients: Union[str, Iterable[str]],
    subject: str,
    text_body: str,
    html_body: str,
    timeout: float = SMTP_TIMEOUT_SECS,
) -> Tuple[bool, Optional[str]]:
    """Send one multipart/alternative (text + HTML) UTF-8 message. No retry.

    Never raises. Returns (ok, message) -- see the module docstring.
    """
    try:
        host = _strip_crlf(host)
        from_address = _strip_crlf(from_address)
        subject = _strip_crlf(subject)
        username = _strip_crlf(username)
        password = password or ""
        try:
            port = int(port)
        except (TypeError, ValueError):
            return False, "invalid SMTP port"
        if not host:
            return False, "SMTP host not configured"
        if not is_valid_address(from_address):
            return False, "invalid from address"

        warnings = []
        candidates = parse_recipients(recipients)
        valid = [a for a in candidates if is_valid_address(a)]
        dropped = len(candidates) - len(valid)
        if dropped:
            warnings.append(f"dropped {dropped} invalid address(es)")
        if len(valid) > MAX_RECIPIENTS:
            warnings.append(f"capped at {MAX_RECIPIENTS} recipients ({len(valid) - MAX_RECIPIENTS} not sent)")
            valid = valid[:MAX_RECIPIENTS]
        if not valid:
            return False, "; ".join(["no valid recipients"] + warnings)

        msg = _build_message(from_address, valid, subject, text_body, html_body)
        refused = await asyncio.to_thread(
            _send_sync, host, port, username, password, from_address, valid, msg, timeout,
        )
        if refused:
            warnings.append(f"{len(refused)} recipient(s) refused by server")
        warning = "; ".join(warnings) or None
        logger.info(
            "send_email: sent to %d recipient(s) via %s:%s%s",
            len(valid) - len(refused or {}), host, port, f" ({warning})" if warning else "",
        )
        return True, warning
    except Exception as exc:  # noqa: BLE001 -- contract: never raise (task cancellation still propagates)
        error = _short_error(exc, timeout)
        if password and password in error:  # belt and braces; never expected
            error = error.replace(password, "***")
        logger.warning("send_email: failed via %s:%s: %s", host, port, error)
        return False, error


def build_email_test(from_address: str, now: Optional[datetime] = None) -> Tuple[str, str, str]:
    """Subject, text body and HTML body for a settings-page test message.
    Contains no secrets; every inserted value is HTML-escaped."""
    now = now or datetime.now(timezone.utc)
    when = now.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    from_address = _strip_crlf(from_address)
    subject = "Kafka Analyser - test email"
    text_body = (
        "This is a test email from the Kafka Analyser.\n"
        "If you received it, email delivery is configured correctly.\n"
        "No action is required.\n\n"
        f"Sent at: {when}\n"
        f"From address: {from_address}\n"
    )
    html_body = (
        "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;font-size:14px\">"
        "<p><strong>This is a test email from the Kafka Analyser.</strong></p>"
        "<p>If you received it, email delivery is configured correctly. No action is required.</p>"
        "<table style=\"border-collapse:collapse\">"
        f"<tr><td style=\"padding:2px 12px 2px 0;color:#666\">Sent at</td><td>{html.escape(when)}</td></tr>"
        f"<tr><td style=\"padding:2px 12px 2px 0;color:#666\">From address</td><td>{html.escape(from_address)}</td></tr>"
        "</table></body></html>"
    )
    return subject, text_body, html_body


# ── Agent-owner notices (health summary, watchdog) ──────────────────────────

_HTML_STYLE_TABLE = "border-collapse:collapse;font-size:13px"
_HTML_STYLE_CELL = "border:1px solid #ccc;padding:4px 8px;text-align:left;vertical-align:top"
_HTML_RED = "color:#c50f1f;font-weight:bold"
_HTML_AMBER = "color:#9d5d00"
_HTML_GREEN = "color:#107c10"


def _utc_text(now: Optional[datetime]) -> str:
    now = now or datetime.now(timezone.utc)
    return now.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _health_row_cells(r: dict) -> list:
    """(text, style) per column for one cluster row. Colour thresholds
    mirror build_health_summary_card / health_card_has_issue in
    notifier.py; keep them in step."""
    age = r.get("data_age_minutes")
    if age is None:
        age_cell = ("no data", _HTML_AMBER)
    else:
        age_cell = (f"{age:.0f}m ago", _HTML_GREEN if age < 5 else _HTML_AMBER if age < 10 else _HTML_RED)

    ok_n, failed_n = r.get("success_count", 0), r.get("failed_count", 0)
    jobs_cell = (f"{ok_n} ok, {failed_n} failed", _HTML_RED if failed_n > 0 else _HTML_GREEN)

    agent, real = r.get("brokers_online_agent"), r.get("brokers_online_real")
    agent_cell = (agent if agent is not None else "n/a", "")
    if agent is not None and real is not None and agent != real:
        real_cell = (real, _HTML_RED)
    else:
        real_cell = (real if real is not None else "n/a", "")

    # Unlike the card (every broker with its count), the email lists only
    # brokers with stale connections, in broker order, or "none" when all
    # are 0; a missing count counts as 0. "n/a" when the check gave no data.
    conns = r.get("broker_connections")
    if conns:
        stale = [(c.get("label", "?"), c.get("close_wait") or 0) for c in conns if (c.get("close_wait") or 0) > 0]
        if stale:
            conn_cell = (" ".join(f"{label}:{count}" for label, count in stale), _HTML_RED)
        else:
            conn_cell = ("none", "")
    else:
        conn_cell = ("n/a", "")

    return [(str(r.get("name", "Unknown")), ""), age_cell, jobs_cell, agent_cell, real_cell, conn_cell]


_HEALTH_COLUMNS = [
    "Cluster", "Data age", "Jobs (recent)",
    "Brokers online (collector)", "Brokers online (live check)", "Stale connections (CLOSE_WAIT)",
]


def build_health_summary_email(
    agent_name: str,
    process_count: int,
    health_status: str,
    rows: list,
    now: Optional[datetime] = None,
) -> Tuple[str, str, str]:
    """Email twin of notifier.build_health_summary_card, from the same
    inputs (see that function for the row keys). Subject is "[OK] ..." or
    "[ISSUE] ..." per health_card_has_issue. Every value is HTML-escaped."""
    from shared.escalation.notifier import health_card_has_issue

    has_issue = health_card_has_issue(health_status, rows)
    subject = _strip_crlf(f"[{'ISSUE' if has_issue else 'OK'}] {agent_name} health summary")
    when = _utc_text(now)
    status_text = str(health_status).upper()
    table = [_health_row_cells(r) for r in rows]

    lines = [
        f"{agent_name} health summary",
        f"Time: {when}",
        f"Process count: {process_count}",
        f"Health status: {status_text}",
        "",
    ]
    if table:
        widths = [max(len(h), *(len(row[i][0]) for row in table)) for i, h in enumerate(_HEALTH_COLUMNS)]
        lines.append("  ".join(h.ljust(w) for h, w in zip(_HEALTH_COLUMNS, widths)).rstrip())
        lines.append("  ".join("-" * w for w in widths))
        for row in table:
            lines.append("  ".join(text.ljust(w) for (text, _), w in zip(row, widths)).rstrip())
    else:
        lines.append("No enabled clusters.")
    text_body = "\n".join(lines) + "\n"

    esc = html.escape
    head = "".join(f"<th style=\"{_HTML_STYLE_CELL};background:#f3f3f3\">{esc(h)}</th>" for h in _HEALTH_COLUMNS)
    body_rows = "".join(
        "<tr>" + "".join(
            f"<td style=\"{_HTML_STYLE_CELL};{style}\">{esc(text)}</td>" for text, style in row
        ) + "</tr>"
        for row in table
    ) or f"<tr><td colspan=\"{len(_HEALTH_COLUMNS)}\" style=\"{_HTML_STYLE_CELL}\">No enabled clusters.</td></tr>"
    status_style = _HTML_GREEN if health_status == "alive" else _HTML_RED
    html_body = (
        "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;font-size:14px\">"
        f"<p><strong>{esc(agent_name)} health summary</strong></p>"
        f"<p>Time: {esc(when)}<br>Process count: {esc(str(process_count))}<br>"
        f"Health status: <span style=\"{status_style}\">{esc(status_text)}</span></p>"
        f"<table style=\"{_HTML_STYLE_TABLE}\"><tr>{head}</tr>{body_rows}</table>"
        "</body></html>"
    )
    return subject, text_body, html_body


_WATCHDOG_LABEL = {
    "process_watchdog": "process count watchdog",
    "data_freshness_watchdog": "data freshness watchdog",
}


def build_watchdog_email(
    agent_name: str,
    severity: str,
    category: str,
    description: str,
    now: Optional[datetime] = None,
) -> Tuple[str, str, str]:
    """Email twin of the agent-wide watchdog Teams card
    (notifier.build_adaptive_card). Subject is "[ISSUE] ..." for critical
    or warning severity, "[OK] ..." otherwise. Every value is HTML-escaped."""
    severity = str(severity or "info")
    label = _WATCHDOG_LABEL.get(category, str(category).replace("_", " "))
    tag = "ISSUE" if severity.lower() in ("critical", "warning") else "OK"
    subject = _strip_crlf(f"[{tag}] {agent_name} {label}: {severity.upper()}")
    when = _utc_text(now)
    text_body = (
        f"{agent_name} {label}\n"
        f"Severity: {severity.upper()}\n"
        f"Scope: agent-wide, not cluster-specific\n"
        f"Time: {when}\n\n"
        f"{description}\n"
    )
    esc = html.escape
    sev_style = _HTML_RED if tag == "ISSUE" else _HTML_GREEN
    html_body = (
        "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;font-size:14px\">"
        f"<p><strong>{esc(agent_name)} {esc(label)}</strong></p>"
        f"<p>Severity: <span style=\"{sev_style}\">{esc(severity.upper())}</span><br>"
        "Scope: agent-wide, not cluster-specific<br>"
        f"Time: {esc(when)}</p>"
        f"<p>{esc(str(description))}</p>"
        "</body></html>"
    )
    return subject, text_body, html_body


def owner_email_configured(config: dict) -> bool:
    return bool(
        str(config.get("health_email_recipients") or "").strip()
        and str(config.get("smtp_host") or "").strip()
        and str(config.get("smtp_from_address") or "").strip()
    )


def owner_teams_configured(config: dict) -> bool:
    return bool(config.get("teams_enabled") and config.get("teams_webhook_url"))


async def notify_owner(
    subject: str,
    text_body: str,
    html_body: str,
    teams_card: dict,
    config: dict,
    email_timeout: float = SMTP_TIMEOUT_SECS,
) -> Tuple[Optional[str], bool]:
    """Agent-owner notice: email first, Teams as the fallback.

    Emails health_email_recipients when it and smtp_host/smtp_from_address
    are set; on any email failure logs a warning (never the password) and
    posts teams_card instead. With no recipients it is exactly the Teams
    post the callers made before (only if teams_enabled and a webhook URL
    are set). config is the agent's settings dict, passed in because this
    shared module cannot import a particular agent's settings.

    email_timeout caps the whole email attempt (and each SMTP step), so a
    caller with its own deadline still leaves time for the Teams fallback.
    If the cap fires, the worker thread can still finish the send in the
    background, so the owner may get both the email and the Teams card.

    Returns (channel_used, ok): channel_used is "email", "teams" or None
    (nothing configured). Never raises."""
    try:
        if owner_email_configured(config):
            try:
                ok, error = await asyncio.wait_for(
                    send_email(
                        host=config.get("smtp_host") or "",
                        port=config.get("smtp_port") or 587,
                        username=str(config.get("smtp_username") or ""),
                        password=str(config.get("smtp_password") or ""),
                        from_address=str(config.get("smtp_from_address") or ""),
                        recipients=str(config.get("health_email_recipients") or ""),
                        subject=subject,
                        text_body=text_body,
                        html_body=html_body,
                        timeout=email_timeout,
                    ),
                    timeout=email_timeout,
                )
            except asyncio.TimeoutError:
                ok, error = False, f"timed out after {email_timeout:g}s"
            if ok:
                if error:
                    logger.warning("notify_owner: email sent with warning: %s", error)
                logger.info("owner notice sent via email")
                return "email", True
            logger.warning("notify_owner: email failed (%s); falling back to Teams", error)
            email_failed = True
        else:
            email_failed = False

        if not owner_teams_configured(config):
            return None, False
        from shared.escalation import notifier as _notifier
        ok = await _notifier.send_to_teams(webhook_url=config.get("teams_webhook_url"), card=teams_card)
        if ok:
            logger.info("owner notice sent via teams%s", " (email failed)" if email_failed else "")
        return "teams", bool(ok)
    except Exception as exc:  # noqa: BLE001 -- contract: never raise
        logger.warning("notify_owner: failed (%s)", type(exc).__name__)
        return None, False


# ── Alert rule emails (fire / escalate / resolve) ───────────────────────────

# Rule emails only go to internal addresses; others in email_recipients are
# dropped with a warning.
RULE_EMAIL_ALLOWED_DOMAINS = ("@operative.com", "@sintecmedia.com")
_ALERT_KINDS = ("fire", "escalate", "resolve", "reminder")


def _format_open_minutes(minutes: Optional[float]) -> Optional[str]:
    if minutes is None:
        return None
    minutes = max(0.0, float(minutes))
    if minutes < 60:
        return f"{minutes:.0f} min"
    hours, mins = divmod(int(round(minutes)), 60)
    return f"{hours} h {mins} min"


def _affected_value(value) -> Optional[str]:
    """The "Affected" row value (a trigger's subject, e.g. a broker), or None
    to hide the row: empty, or the generic "cluster" of cluster-wide rules."""
    if value is None:
        return None
    text = str(value)
    if not text.strip() or text.strip().lower() == "cluster":
        return None
    return text


def _alert_group_table(extra: dict) -> Tuple[list, list]:
    """(header, rows as strings) of a grouped lag email's table, or ([], [])."""
    if extra.get("offenders"):
        return ["Name", "Lag", "Tier", "Thresholds"], [
            [str(o.get("name", "")), f"{int(o.get('lag') or 0):,}", str(o.get("tier") or ""), str(o.get("source") or "")]
            for o in extra["offenders"]
        ]
    if extra.get("cleared"):
        return ["Name", "Was", "Open for"], [
            [str(c.get("name", "")), str(c.get("was") or "").upper(), _format_open_minutes(c.get("open_minutes")) or ""]
            for c in extra["cleared"]
        ]
    return [], []


def build_alert_email(
    kind: str,
    alert_type: str,
    cluster_name: str,
    severity: Optional[str],
    description: str = "",
    recommended_action: str = "",
    extra: Optional[dict] = None,
    now: Optional[datetime] = None,
) -> Tuple[str, str, str]:
    """Email twin of an alert rule's Teams card. kind is "fire", "escalate"
    or "resolve". Fire/escalate carry the build_adaptive_card fields
    (severity, category, cluster, time, description, recommended action);
    resolve carries the build_resolve_card fields (rule, cluster, subject,
    was, open for, time). extra may hold rule_name, subject (e.g. a
    broker), open_minutes (resolve) and test (marks a sample email).
    Grouped lag rules add "reminder" (still firing, no change) and a full
    table: extra["offenders"] ({"name", "lag", "tier", "source"}) or, on a
    resolve, extra["cleared"] ({"name", "was", "open_minutes"}), each shown
    in the order given.
    Subject: "[SEVERITY] Kafka Analyser - <label> - <cluster>", or
    "[RESOLVED] ..." for a resolve; CR/LF stripped, every value escaped."""
    from shared.escalation.notifier import CATEGORY_LABEL

    if kind not in _ALERT_KINDS:
        raise ValueError(f"unknown alert email kind: {kind!r}")
    extra = extra or {}
    alert_type = str(alert_type or "unknown")
    label = CATEGORY_LABEL.get(alert_type, alert_type.replace("_", " ").title())
    sev = str(severity or "info").upper()
    cluster_name = str(cluster_name or "")
    tag = "RESOLVED" if kind == "resolve" else sev
    subject = f"[{tag}] Kafka Analyser - {label} - {cluster_name}"
    if extra.get("test"):
        subject = "[TEST] " + subject
    subject = _strip_crlf(subject)
    when = _utc_text(now)

    status = {"fire": "FIRING", "escalate": "ESCALATED", "resolve": "RESOLVED", "reminder": "REMINDER"}[kind]
    rows = [("Status", status)]
    if extra.get("rule_name"):
        rows.append(("Rule", str(extra["rule_name"])))
    if kind == "resolve":
        rows.append(("Category", label))
        rows.append(("Cluster", cluster_name))
        affected = _affected_value(extra.get("subject"))
        if affected is not None:
            rows.append(("Affected", affected))
        if severity:
            rows.append(("Was", sev))
        open_text = _format_open_minutes(extra.get("open_minutes"))
        if open_text:
            rows.append(("Open for", open_text))
        rows.append(("Time", when))
        message = description or "The condition that triggered this alert has cleared."
    else:
        rows.append(("Severity", sev))
        rows.append(("Category", label))
        rows.append(("Cluster", cluster_name))
        affected = _affected_value(extra.get("subject"))
        if affected is not None:
            rows.append(("Affected", affected))
        rows.append(("Time", when))
        message = description or ""

    heading = f"Kafka Analyser alert {status.lower()}: {label}"
    test_note = "This is a TEST email with sample values, not a real alert." if extra.get("test") else ""

    width = max(len(k) for k, _ in rows)
    lines = [heading]
    if test_note:
        lines.append(test_note)
    lines.append("")
    lines += [f"{k.ljust(width)}  {v}" for k, v in rows]
    if message:
        lines += ["", message]
    group_head, group_rows = _alert_group_table(extra)
    if group_rows:
        widths = [max(len(h), *(len(r[i]) for r in group_rows)) for i, h in enumerate(group_head)]
        lines += ["", "  ".join(h.ljust(w) for h, w in zip(group_head, widths)).rstrip(),
                  "  ".join("-" * w for w in widths)]
        lines += ["  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip() for r in group_rows]
    if recommended_action and kind != "resolve":
        lines += ["", f"Recommended action: {recommended_action}"]
    text_body = "\n".join(lines) + "\n"

    esc = html.escape
    colour = _HTML_GREEN if kind == "resolve" else (
        _HTML_RED if sev == "CRITICAL" else _HTML_AMBER if sev == "WARNING" else ""
    )
    table_rows = "".join(
        f"<tr><th style=\"{_HTML_STYLE_CELL};background:#f3f3f3\">{esc(k)}</th>"
        f"<td style=\"{_HTML_STYLE_CELL};{colour if k in ('Status', 'Severity') else ''}\">{esc(v)}</td></tr>"
        for k, v in rows
    )
    html_body = (
        "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;font-size:14px\">"
        f"<p><strong>{esc(heading)}</strong></p>"
        + (f"<p style=\"{_HTML_RED}\">{esc(test_note)}</p>" if test_note else "")
        + f"<table style=\"{_HTML_STYLE_TABLE}\">{table_rows}</table>"
        + (f"<p>{esc(message)}</p>" if message else "")
        + (
            f"<table style=\"{_HTML_STYLE_TABLE}\"><tr>"
            + "".join(f"<th style=\"{_HTML_STYLE_CELL};background:#f3f3f3\">{esc(h)}</th>" for h in group_head)
            + "</tr>"
            + "".join("<tr>" + "".join(f"<td style=\"{_HTML_STYLE_CELL}\">{esc(c)}</td>" for c in r) + "</tr>"
                      for r in group_rows)
            + "</table>"
            if group_rows else ""
        )
        + (f"<p><em>Recommended action:</em> {esc(recommended_action)}</p>"
           if recommended_action and kind != "resolve" else "")
        + "</body></html>"
    )
    return subject, text_body, html_body


async def notify_rule_email(
    config_row,
    cluster_name: str,
    kind: str,
    severity: Optional[str],
    description: str = "",
    recommended_action: str = "",
    extra: Optional[dict] = None,
    *,
    settings: dict,
) -> Optional[bool]:
    """Email for one alert rule event, sent next to the Teams card.

    Does nothing (returns None) unless the global email_enabled setting and
    the rule's own email_enabled are on, email_recipients is non-empty, and
    smtp_host and smtp_from_address are set. Recipients outside
    RULE_EMAIL_ALLOWED_DOMAINS are dropped with a warning. settings is the
    agent's settings dict (this shared module cannot import it). Logs one
    INFO "rule email sent" or WARNING "rule email failed" line with the
    rule id, kind and recipient count only. Returns True/False for a send
    attempt. Never raises."""
    rule_id = getattr(config_row, "id", None)
    try:
        if not getattr(config_row, "email_enabled", False) or not settings.get("email_enabled"):
            return None
        host = str(settings.get("smtp_host") or "").strip()
        from_address = str(settings.get("smtp_from_address") or "").strip()
        candidates = parse_recipients(settings.get("email_recipients") or "")
        if not candidates or not host or not from_address:
            return None

        allowed = [a for a in candidates if a.lower().endswith(RULE_EMAIL_ALLOWED_DOMAINS)]
        if len(allowed) < len(candidates):
            logger.warning(
                "rule email: rule_id=%s kind=%s dropped %d recipient(s) outside %s",
                rule_id, kind, len(candidates) - len(allowed), " / ".join(RULE_EMAIL_ALLOWED_DOMAINS),
            )
        if not allowed:
            logger.warning("rule email failed: rule_id=%s kind=%s: no allowed recipients", rule_id, kind)
            return False

        extra = dict(extra or {})
        extra.setdefault("rule_name", getattr(config_row, "name", None))
        subject, text_body, html_body = build_alert_email(
            kind, getattr(config_row, "alert_type", ""), cluster_name, severity,
            description, recommended_action, extra,
        )
        ok, error = await send_email(
            host=host,
            port=settings.get("smtp_port") or 587,
            username=str(settings.get("smtp_username") or ""),
            password=str(settings.get("smtp_password") or ""),
            from_address=from_address,
            recipients=allowed,
            subject=subject,
            text_body=text_body,
            html_body=html_body,
            timeout=SMTP_TIMEOUT_SECS,
        )
        if ok:
            logger.info(
                "rule email sent: rule_id=%s kind=%s recipients=%d%s",
                rule_id, kind, len(allowed), f" ({error})" if error else "",
            )
            return True
        logger.warning("rule email failed: rule_id=%s kind=%s: %s", rule_id, kind, error)
        return False
    except Exception as exc:  # noqa: BLE001 -- contract: never raise
        logger.warning("rule email failed: rule_id=%s kind=%s: %s", rule_id, kind, type(exc).__name__)
        return False
