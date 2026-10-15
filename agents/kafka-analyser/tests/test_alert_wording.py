"""The "Affected" row of alert rule cards and emails (notifier.affected_text).

Stub test: the real card and email builders (shared/escalation), and the real
teams_alerts._send_resolve_cards / _resolve_emails with a fake database
module, fake settings and a fake Teams post; no database, network, SMTP or
Teams is touched. For every alert type, its fire, escalate and resolve card
and email: the row is "Affected" on both channels, shows "Broker <id>" for
the per-broker types, host:port for broker reachability, the stored subject
for unknown types, and is omitted for cluster-wide, close_wait and lag rules;
the value is escaped in the HTML email. Prints one [PASS]/[FAIL] line per
check and "N/N checks passed"; exits 1 if any check fails.
Run from the repo root inside the agent image:
  docker run --rm -v $PWD/agents/kafka-analyser:/app:ro -v $PWD/shared:/opt/shared:ro \
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_alert_wording.py"""
import asyncio, json, logging, os, re, sys, types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS
sys.dont_write_bytecode = True
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..")); sys.path.insert(0, os.path.join(_HERE, "..", "..", ".."))  # agent dir, repo root (shared/)
logging.basicConfig(level=logging.WARNING, stream=sys.stdout, format="    [log %(levelname)s] %(message)s")

from shared.escalation.notifier import (  # noqa: E402
    CATEGORY_LABEL, affected_text, build_adaptive_card, build_lag_card, build_resolve_card,
)
from shared.escalation.email_notifier import build_alert_email  # noqa: E402

RESULTS = []


def check(label, cond, extra=""):
    RESULTS.append(bool(cond))
    print(f"[{'PASS' if cond else 'FAIL'}] {label}" + (f"  {extra}" if extra and not cond else ""))


def facts(card):
    content = card["attachments"][0]["content"]
    out = []
    for block in content["body"]:
        if block.get("type") == "FactSet":
            out += [(f["title"], f["value"]) for f in block["facts"]]
    return out


def email_rows(text_body):
    """(label, value) rows of the plain-text email's key/value block."""
    lines = text_body.split("\n")
    start = lines.index("") + 1
    rows = []
    for line in lines[start:]:
        if not line:
            break
        m = re.match(r"(\S+(?: \S+)?)\s{2,}(.*)$", line)
        if m:
            rows.append((m.group(1), m.group(2)))
    return rows


def labels(rows):
    return [k for k, _ in rows]


# Every alert type the Teams tab offers (teams.html ALERT_TYPES) plus one the
# code does not know, with the subject its evaluator stores on a trigger.
LAG = ("cluster.consumer_lag", "cluster.connector_lag")
CLUSTER_WIDE = (
    "cluster.urp_total", "cluster.rf_below_min", "cluster.leader_skew", "cluster.msg_rate_in", "cluster.data_gb_max",
    "cluster.data_spread_pct", "cluster.zk_ensemble", "cluster.connect_failed_connectors", "cluster.connect_failed_tasks",
    "cluster.connect_workers_down", "cluster.sr_nodes_down", "cluster.sr_soft_deleted", "cluster.slo_compliance",
)
CASES = (  # alert_type, stored subject, expected Affected (None = row omitted)
    [("broker.cpu_pct", "3", "Broker 3"), ("broker.heap_pct", "12", "Broker 12"),
     ("broker_unreachable", "kafka-1.internal:9092", "kafka-1.internal:9092"),
     ("close_wait_spike", None, None)]
    + [(t, "cluster", None) for t in CLUSTER_WIDE]
    + [(t, None, None) for t in LAG] + [(t, "orders-consumer", None) for t in LAG]
    + [("future.type", "node-7", "node-7")]
)


def main_builders():
    check(f"cases cover all 19 known alert types plus an unknown one",
          len({c[0] for c in CASES}) == 20)
    for alert_type, subject, want in CASES:
        got = affected_text(alert_type, subject)
        check(f"affected_text({alert_type!r}, {subject!r}) -> {want!r}", got == want, repr(got))
        for kind in ("fire", "escalate"):
            # The evaluators pass affected_text(...) as anomaly["affected"].
            card = build_adaptive_card("Kafka Analyser", "prod-4", {
                "severity": "critical", "category": alert_type, "description": "d", "recommended_action": "a",
                "affected": affected_text(alert_type, subject)})
            f = facts(card)
            _subject, text_body, html_body = build_alert_email(
                kind, alert_type, "prod-4", "critical", "d", "a", {"rule_name": "R", "subject": subject})
            e = email_rows(text_body)
            if want is None:
                ok = "Affected" not in labels(f) and "Affected" not in labels(e) and ">Affected<" not in html_body
            else:
                ok = (("Affected", want) in f and ("Affected", want) in e
                      and labels(f) == ["Severity", "Category", "Cluster", "Affected", "Time"]
                      and labels(e)[labels(e).index("Cluster") + 1] == "Affected")
            check(f"{kind} card+email {alert_type}: " + (f"Affected = {want}" if want else "row omitted"),
                  ok and "Subject" not in labels(f) + labels(e), f"card={f} email={e}")
        card = build_resolve_card("Kafka Analyser", "prod-4", "R", subject, "warning", 12.0, alert_type=alert_type)
        f = facts(card)
        _subject, text_body, html_body = build_alert_email(
            "resolve", alert_type, "prod-4", "warning", extra={"rule_name": "R", "subject": subject, "open_minutes": 12})
        e = email_rows(text_body)
        if want is None:
            ok = "Affected" not in labels(f) and "Affected" not in labels(e)
        else:
            ok = (("Affected", want) in f and ("Affected", want) in e
                  and labels(f) == ["Status", "Rule", "Cluster", "Affected", "Was", "Open for", "Time"])
        check(f"resolve card+email {alert_type}: " + (f"Affected = {want}" if want else "row omitted"),
              ok and "Subject" not in labels(f) + labels(e), f"card={f} email={e}")

    # Unchanged rows around it
    f = facts(build_resolve_card("Kafka Analyser", "prod-4", "R", "3", "warning", 12.0, alert_type="broker.cpu_pct"))
    check("resolve card keeps Status, Rule, Cluster, Was, Open for, Time", f[:3] == [("Status", "RESOLVED"), ("Rule", "R"), ("Cluster", "prod-4")]
          and ("Was", "WARNING") in f and ("Open for", "12 min") in f)
    f = facts(build_adaptive_card("Kafka Analyser", "prod-4", {"severity": "info", "category": "x", "description": "d"}))
    check("card without anomaly['affected'] (other callers, owner notices) is unchanged: no Affected row",
          labels(f) == ["Severity", "Category", "Cluster", "Time"])
    f = facts(build_resolve_card("Kafka Analyser", "prod-4", "R", "kafka-1:9092", None, None))
    check("resolve card without alert_type: stored subject under Affected", ("Affected", "kafka-1:9092") in f)
    f = facts(build_resolve_card("Kafka Analyser", "prod-4", "R", "cluster", None, None))
    check("resolve card without alert_type: 'cluster' still omitted", "Affected" not in labels(f))
    check("empty and blank subjects omitted for every type",
          all(affected_text(t, s) is None for t, *_ in CASES for s in ("", "  ", None)))
    check("an already labelled broker subject is not doubled (settings sample 'Broker 1')",
          affected_text("broker.cpu_pct", "Broker 1") == "Broker 1")
    _s, text_body, _h = build_alert_email("fire", "broker.cpu_pct", "sample-cluster", "warning", "d", "a",
                                          {"test": True, "rule_name": "TEST", "subject": "Broker 1"})
    check("settings test email still shows 'Affected  Broker 1'", ("Affected", "Broker 1") in email_rows(text_body))

    # Lag cards keep their own lists
    card = build_lag_card("Kafka Analyser", "prod-4", "cluster.consumer_lag", "warning", "2 groups",
                          [{"name": "g1", "lag": 20000, "tier": "warning", "source": "generic"}])
    check("lag card: its own group rows, no Affected row", ("g1", "20,000 · warning · generic") in facts(card)
          and "Affected" not in labels(facts(card)))
    _s, text_body, _h = build_alert_email("fire", "cluster.consumer_lag", "prod-4", "warning", "2 groups", "",
                                          {"offenders": [{"name": "g1", "lag": 20000, "tier": "warning", "source": "generic"}]})
    check("lag email: its own table, no Affected row", "g1" in text_body and "Affected" not in text_body)

    # Escaping: HTML-escaped in the email, verbatim (JSON) on the card
    nasty = "<b>x</b>&'\"9092"
    for alert_type, want in (("broker_unreachable", nasty), ("broker.cpu_pct", "Broker " + nasty), ("future.type", nasty)):
        _s, text_body, html_body = build_alert_email("fire", alert_type, "c", "warning", "d", "a", {"subject": nasty})
        _s, _t, html_resolve = build_alert_email("resolve", alert_type, "c", "warning", extra={"subject": nasty})
        esc = want.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#x27;")
        check(f"email {alert_type}: Affected value HTML-escaped (fire and resolve), plain in the text body",
              f">{esc}</td>" in html_body and f">{esc}</td>" in html_resolve and "<b>x</b>" not in html_body + html_resolve
              and ("Affected", want) in email_rows(text_body), html_body[:0])
        card = build_resolve_card("K", "c", "R", nasty, None, None, alert_type=alert_type)
        check(f"card {alert_type}: value kept verbatim in the card JSON (no markup added)",
              ("Affected", want) in facts(json.loads(json.dumps(card))))


async def main_teams_alerts():
    """The real resolve path: _send_resolve_cards passes the rule type, so the
    posted card has the Affected row; _resolve_emails gives the email the
    stored subject, rendered the same way."""
    sys.modules["database"] = types.ModuleType("database")
    sys.modules["database"].SessionLocal = None
    rs = types.ModuleType("routes_settings"); rs._config = {"teams_enabled": True, "teams_webhook_url": "https://hook/agent"}
    sys.modules["routes_settings"] = rs
    import teams_alerts as ta
    ta.outbox.enabled = lambda: False
    posted = []

    async def fake_post(webhook_url, card):
        posted.append(card)
        return True, {"status": "sent"}
    ta._post_card = fake_post
    now = datetime.now(timezone.utc)
    items = []
    for i, (alert_type, subject, want) in enumerate(CASES):
        config = NS(id=i + 1, name=f"Rule {i}", alert_type=alert_type, severity="warning", config={},
                    webhook_url=None, enabled=True, email_enabled=True, cluster_id=4)
        trigger = NS(subject=subject, severity="critical", triggered_at=now - timedelta(minutes=30), resolved_at=now,
                     teams_post_success=True)
        items.append({"config": config, "trigger": trigger, "cluster_name": "prod-4"})
    await ta._send_resolve_cards(items)
    got = [dict(facts(c)).get("Affected") for c in posted]
    want = [w for _t, _s, w in CASES]
    check(f"_send_resolve_cards: {len(posted)} real resolve cards, Affected per type as expected", got == want,
          f"{got} != {want}")
    check("_send_resolve_cards: no card has a Subject row", all("Subject" not in dict(facts(c)) for c in posted))
    emails = ta._resolve_emails(items)
    rendered = []
    for e in emails:
        _s, text_body, _h = build_alert_email(e["kind"], e["config_row"].alert_type, e["cluster_name"], e["severity"],
                                              e["description"], e["recommended_action"], e["extra"])
        rendered.append(dict(email_rows(text_body)).get("Affected"))
    check(f"_resolve_emails: {len(emails)} resolve emails, same Affected as the cards", rendered == want, f"{rendered}")
    check("stored data untouched: trigger subjects and severities unchanged",
          [it["trigger"].subject for it in items] == [s for _t, s, _w in CASES]
          and all(it["trigger"].severity == "critical" for it in items))

    # Fire/escalate call sites in teams_alerts pass affected_text(...) to the card
    # (through the rule_alert_card builder since step 2; tests/test_rule_notice_golden.py):
    # reachability and broker thresholds inline, and the outbox path for the per-broker rules.
    src = open(ta.__file__).read()
    check("teams_alerts: reachability and broker-threshold fire/escalate cards pass 'affected'",
          src.count('affected=affected_text(') == 2
          and '{"affected": affected_text(config.alert_type, subject)} if with_affected' in src
          and 'affected=affected_text(BROKER_UNREACHABLE_ALERT_TYPE, subject)' in src
          and 'affected=affected_text(config.alert_type, subject)' in src
          and 'anomaly["affected"] = affected' in src)


main_builders()
asyncio.run(main_teams_alerts())
print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
sys.exit(0 if RESULTS and all(RESULTS) else 1)
