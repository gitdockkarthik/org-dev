"""Golden test of the rule notice content: the exact Teams card JSON and email
(subject, text, html) that the inline send paths of teams_alerts produce for
each kind (fire, escalate, reminder, resolve) and alert type family
(close-wait, reachability, broker thresholds per broker, cluster-wide
metrics, lag). The real evaluators run against an in-memory fake database;
the clock is frozen in teams_alerts and both shared builders; the Teams post
(_post_card) and the SMTP send (email_notifier.send_email) are replaced by
recorders, so the content is captured exactly as it would be sent.

The golden file (tests/golden/rule_notices.json) was written from the code
before the builders were extracted; the test fails on any byte of
difference. To regenerate it on purpose: GOLDEN_WRITE=1.
Run: python -B agents/kafka-analyser/tests/test_rule_notice_golden.py
(Python 3.10+ with the agent's requirements; no database, network or Teams)."""
import asyncio
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

sys.dont_write_bytecode = True
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))
sys.path.insert(0, os.path.join(_HERE, "..", "..", ".."))  # repo root (shared/)
GOLDEN = os.path.join(_HERE, "golden", "rule_notices.json")

from sqlalchemy.sql.elements import TextClause  # noqa: E402

FROZEN = datetime(2026, 10, 15, 6, 45, 0, tzinfo=timezone.utc)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return FROZEN if tz is not None else FROZEN.replace(tzinfo=None)


# ── Fake database ────────────────────────────────────────────────────────────

class World:
    def reset(self):
        self.configs, self.triggers, self.next_id = [], {}, 100
        self.metrics, self.urp, self.fresh, self.lag = [], {}, set(), {}


world = World()
world.reset()


class Res:
    def __init__(self, rows=None):
        self.rows = rows or []

    def all(self):
        return self.rows

    def scalars(self):
        return self

    def scalar_one(self):
        return self.rows[0]


class Nested:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class Session:
    def __init__(self):
        self.pending = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute(self, stmt, params=None):
        if isinstance(stmt, TextClause):
            sql = stmt.text
            if "kafka_job_runs" in sql:
                return Res([j for j in params["job_ids"] if int(j.rsplit("-", 1)[1]) in world.fresh])
            if "urp_count" in sql:
                return Res([NS(cluster_id=c, value=world.urp[c]) for c in params["cids"] if c in world.urp])
            if "kafka_consumer_group_lag" in sql:
                return Res([NS(cluster_id=c, group_id=g, total_lag=v) for (c, g), v in sorted(world.lag.items())
                            if c in params["cids"]])
            return Res([])
        s = str(stmt)
        open_ = [t for t in world.triggers.values() if t.resolved_at is None]
        if "JOIN" in s and "resolved_at IS NULL" in s:  # resolve_cleared_close_wait_triggers
            by_id = {c.id: c for c in world.configs}
            return Res([(t, by_id[t.alert_config_id]) for t in open_ if t.alert_config_id in by_id])
        if "kafka_broker_metrics" in s:
            return Res(list(world.metrics))
        if "max(" in s:
            rows = {}
            for t in world.triggers.values():
                if t.resolved_at is not None:
                    k = (t.alert_config_id, t.cluster_id, t.subject)
                    rows[k] = max(rows.get(k, t.resolved_at), t.resolved_at)
            return Res([NS(alert_config_id=k[0], cluster_id=k[1], subject=k[2], last_resolved_at=v)
                        for k, v in rows.items()])
        if "resolved_at IS NULL" in s:
            return Res([NS(id=t.id, alert_config_id=t.alert_config_id, cluster_id=t.cluster_id, subject=t.subject,
                           severity=t.severity) for t in open_])
        return Res([c for c in world.configs if c.enabled or c.email_enabled])

    def begin_nested(self):
        return Nested()

    def add(self, t):
        self.pending.append(t)

    async def get(self, model, tid):
        return world.triggers.get(tid)

    async def commit(self):
        for t in self.pending:
            t.id = world.next_id
            world.next_id += 1
            world.triggers[t.id] = t
        self.pending = []

    async def rollback(self):
        self.pending = []


db_mod = types.ModuleType("database")
db_mod.SessionLocal = Session
sys.modules["database"] = db_mod
rs = types.ModuleType("routes_settings")
rs._config = {"teams_enabled": True, "teams_webhook_url": "https://hook/agent", "email_enabled": True,
              "smtp_host": "127.0.0.1", "smtp_port": 25, "smtp_from_address": "alerts@sintecmedia.com",
              "email_recipients": "ops@operative.com"}
sys.modules["routes_settings"] = rs

REACH = {}
collectors_mod = types.ModuleType("collectors")


async def _enabled_clusters():
    return [{"id": 4}]


collectors_mod._get_enabled_clusters = _enabled_clusters
collectors_mod._cid = lambda c: c["id"]
sys.modules["collectors"] = collectors_mod
dashboard_mod = types.ModuleType("routes_dashboard")


async def _reachability(cid):
    return REACH[int(cid)]


dashboard_mod.get_broker_reachability = _reachability
sys.modules["routes_dashboard"] = dashboard_mod

import teams_alerts as ta  # noqa: E402
from shared.escalation import email_notifier, notifier  # noqa: E402

ta.datetime = notifier.datetime = email_notifier.datetime = FrozenDatetime
ta.outbox.enabled = lambda: False


async def _names():
    return {4: "prod-4"}


ta._get_cluster_names = _names

CAPTURED: list = []


async def fake_post(webhook_url, card):
    CAPTURED.append({"channel": "teams", "card": card})
    return True, ta.outbox.sent()


async def fake_send_email(**kw):
    CAPTURED.append({"channel": "email", "subject": kw["subject"], "text": kw["text_body"], "html": kw["html_body"]})
    return True, None


ta._post_card = fake_post
email_notifier.send_email = fake_send_email

# Every queued email is also rendered with the builder the outbox path uses;
# it must equal what notify_rule_email sent.
BUILT: list = []
_queue = ta._queue_rule_emails


def recording_queue(emails):
    if hasattr(ta, "rule_email_content"):
        BUILT.extend(ta.rule_email_content(e) for e in emails if e)
    _queue(emails)


ta._queue_rule_emails = recording_queue
BUILDER_CHECKS: list = []


def rule(id, alert_type, cluster_id, config, severity="warning", name=None):
    return NS(id=id, name=name or f"Rule {id}", alert_type=alert_type, cluster_id=cluster_id, severity=severity,
              config=config, webhook_url=None, cooldown_minutes=0, enabled=True, email_enabled=True)


RESULTS: dict = {}


async def step(label, coro):
    CAPTURED.clear()
    BUILT.clear()
    await coro
    if ta._rule_email_tasks:
        await asyncio.gather(*list(ta._rule_email_tasks))
    RESULTS[label] = sorted(CAPTURED, key=lambda c: c["channel"])
    sent = [{k: c[k] for k in ("subject", "text", "html")} for c in CAPTURED if c["channel"] == "email"]
    if hasattr(ta, "rule_email_content") and sent:
        BUILDER_CHECKS.append((label, BUILT == sent))
    kinds = ", ".join(c["channel"] for c in RESULTS[label])
    print(f"  {label}: {kinds or 'nothing'}")


def age_triggers(minutes):
    for t in world.triggers.values():
        if t.resolved_at is None:
            t.triggered_at = FROZEN - timedelta(minutes=minutes)


async def scenarios():
    # Close-wait (cluster-wide, count rule): fire, then resolve after 42 min.
    world.reset()
    world.configs = [rule(1, "close_wait_spike", None, {"threshold": 1}, name="CLOSE_WAIT spike")]
    await step("close_wait fire", ta.fire_close_wait_alerts({4: 3}))
    age_triggers(42)
    await step("close_wait resolve", ta.resolve_cleared_close_wait_triggers({4: 0}))

    # Reachability (per broker, binary): two failed checks fire, a reachable broker resolves.
    world.reset()
    ta._broker_fail_counts.clear()
    world.configs = [rule(2, "broker_unreachable", None, {}, severity="critical", name="Broker reachability")]
    down = {"cluster_name": "prod-4", "total": 1, "reachable_count": 0,
            "brokers": [{"host": "kafka-1.internal", "port": 9092, "reachable": False}]}
    REACH[4] = down
    await step("reachability check 1", ta._evaluate_broker_reachability())
    await step("reachability fire", ta._evaluate_broker_reachability())
    age_triggers(17)
    REACH[4] = dict(down, reachable_count=1, brokers=[dict(down["brokers"][0], reachable=True)])
    await step("reachability resolve", ta._evaluate_broker_reachability())

    # Broker thresholds (per broker, tiers): fire at warning, escalate to critical, resolve.
    world.reset()
    ta._threshold_breach_counts.clear()
    world.configs = [rule(3, "broker.cpu_pct", 4, {"mode": "simple", "warning": 70, "critical": 90}, name="Broker CPU %")]

    def cpu(value):
        world.metrics = [NS(cluster_id=4, broker_id=1, cpu_pct=value, heap_pct=40.0, time=FROZEN)]
    cpu(75.5)
    await step("broker_thresholds check 1", ta._evaluate_broker_thresholds())
    await step("broker_thresholds fire", ta._evaluate_broker_thresholds())
    cpu(96.0)
    await step("broker_thresholds escalate", ta._evaluate_broker_thresholds())
    age_triggers(65)
    cpu(10.0)
    await step("broker_thresholds resolve", ta._evaluate_broker_thresholds())

    # Cluster metrics (cluster-wide, tiers): URP fire, escalate, resolve.
    world.reset()
    ta._threshold_breach_counts.clear()
    world.fresh = {4}
    world.configs = [rule(4, "cluster.urp_total", 4, {"mode": "simple", "warning": 10, "critical": 100},
                          name="Under-replicated partitions")]
    world.urp[4] = 25
    await step("cluster_metrics check 1", ta._evaluate_cluster_metrics())
    await step("cluster_metrics fire", ta._evaluate_cluster_metrics())
    world.urp[4] = 150
    await step("cluster_metrics escalate", ta._evaluate_cluster_metrics())
    age_triggers(3)
    world.urp[4] = 0
    await step("cluster_metrics resolve", ta._evaluate_cluster_metrics())

    # Lag (grouped per rule): fire, escalate, reminder, grouped resolve.
    world.reset()
    ta._threshold_breach_counts.clear()
    ta._lag_last_card_at.clear()
    world.configs = [rule(5, "cluster.consumer_lag", 4, {"warning": 100, "critical": 1000}, name="Consumer lag")]
    world.lag = {(4, "orders-app"): 500, (4, "billing"): 250, (4, "audit"): 3}
    await step("lag check 1", ta._evaluate_cluster_lag())
    await step("lag fire", ta._evaluate_cluster_lag())
    world.lag[(4, "orders-app")] = 5000
    await step("lag escalate", ta._evaluate_cluster_lag())
    ta._lag_last_card_at[5] = FROZEN - timedelta(hours=7)
    await step("lag reminder", ta._evaluate_cluster_lag())
    age_triggers(95)
    world.lag = {(4, "orders-app"): 1, (4, "billing"): 2, (4, "audit"): 3}
    await step("lag resolve", ta._evaluate_cluster_lag())


def main():
    asyncio.run(scenarios())
    expect_nonempty = [k for k in RESULTS if "check 1" not in k]
    empty = [k for k in expect_nonempty if len(RESULTS[k]) != 2]
    assert not empty, f"each notice should give one card and one email: {empty}"
    text = json.dumps(RESULTS, indent=1, sort_keys=True, ensure_ascii=False) + "\n"
    if os.environ.get("GOLDEN_WRITE") or not os.path.exists(GOLDEN):
        os.makedirs(os.path.dirname(GOLDEN), exist_ok=True)
        with open(GOLDEN, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"[PASS] golden written: {GOLDEN} ({len(RESULTS)} steps)")
        return
    with open(GOLDEN, encoding="utf-8") as fh:
        want = fh.read()
    golden = json.loads(want)
    fails = 0
    for label in sorted(set(golden) | set(RESULTS)):
        same = json.dumps(golden.get(label), sort_keys=True) == json.dumps(RESULTS.get(label), sort_keys=True)
        fails += not same
        print(f"[{'PASS' if same else 'FAIL'}] {label}: byte-identical card and email")
    whole = text == want
    fails += not whole
    print(f"[{'PASS' if whole else 'FAIL'}] whole golden file byte-identical")
    for label, same in BUILDER_CHECKS:
        fails += not same
        print(f"[{'PASS' if same else 'FAIL'}] {label}: rule_email_content equals the email notify_rule_email sent")
    total = len(golden) + 1 + len(BUILDER_CHECKS)
    print(f"{total - fails}/{total} checks passed" + ("" if not fails else "  <-- FAILURES"))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
