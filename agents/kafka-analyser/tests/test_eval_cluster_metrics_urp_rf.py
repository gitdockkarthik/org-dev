"""teams_alerts._evaluate_cluster_metrics for under-replicated partitions (cluster.urp_total) and topics below minimum RF (cluster.rf_below_min).

Stub test: the real teams_alerts runs against an in-memory fake DB session
(database.SessionLocal), fake routes_settings, and fake Teams card builders
and sender; no database, network or Teams is touched. Prints one [PASS]/[FAIL]
line per check and "N/N checks passed"; exits 1 if any check fails.
Outbox recording is switched off here (outbox.enabled -> False): the fake
session does not model kafka_notification_outbox, and recording is covered
by tests/test_outbox*.py.
Run from the repo root with Python 3.10+ and the agent's requirements
(sqlalchemy), e.g. inside the agent image:
  docker run --rm -v $PWD/agents/kafka-analyser:/app:ro -v $PWD/shared:/opt/shared:ro \
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_urp_rf.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_urp_rf.py"""
import asyncio, logging, sys, types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS
sys.dont_write_bytecode = True
import os
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..")); sys.path.insert(0, os.path.join(_HERE, "..", "..", ".."))  # agent dir, repo root (shared/)
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="    [log %(levelname)s] %(message)s")
from sqlalchemy.sql.elements import TextClause
from unittest.mock import MagicMock
try: import httpx
except ImportError: sys.modules["httpx"] = MagicMock()

class W:  # world state
    def reset(self):
        self.configs, self.triggers, self.fresh, self.urp, self.rf_topics = [], {}, set(), {}, {}
        self.fail_query, self.fail_add, self.fail_commit_ids, self.sql, self.events = None, False, set(), [], []
        self.next_id = 500
world = W(); world.reset()

class Res:
    def __init__(self, rows=None, scalar=None): self.rows, self.scalar = rows or [], scalar
    def all(self): return self.rows
    def scalars(self): return self
    def scalar_one(self): return self.scalar
class Nested:
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
class Session:
    def __init__(self): self.pending, self.dirty = [], {}
    async def __aenter__(self): world.events.append("session open"); return self
    async def __aexit__(self, *a): world.events.append("session closed"); return False
    async def execute(self, stmt, params=None):
        if isinstance(stmt, TextClause):
            sql = stmt.text; world.sql.append((sql, params))
            if world.fail_query and world.fail_query in sql: raise RuntimeError("simulated query failure")
            if "kafka_job_runs" in sql:
                return Res([j for j in params["job_ids"] if int(j.rsplit("-", 1)[1]) in world.fresh])
            if "urp_count" in sql:
                return Res([NS(cluster_id=c, value=world.urp[c]) for c in params["cids"] if c in world.urp])
            if "replication_factor" in sql:
                return Res(scalar=sum(1 for rf in world.rf_topics.get(params["cid"], []) if rf < params["min_rf"]))
        s = str(stmt)
        if "max(" in s:
            rows = {}
            for t in world.triggers.values():
                if t.resolved_at is not None:
                    k = (t.alert_config_id, t.cluster_id, t.subject)
                    rows[k] = max(rows.get(k, t.resolved_at), t.resolved_at)
            return Res([NS(alert_config_id=k[0], cluster_id=k[1], subject=k[2], last_resolved_at=v) for k, v in rows.items()])
        if "resolved_at IS NULL" in s:
            return Res([NS(id=t.id, alert_config_id=t.alert_config_id, cluster_id=t.cluster_id, subject=t.subject, severity=t.severity)
                        for t in world.triggers.values() if t.resolved_at is None])
        return Res([c for c in world.configs if c.enabled])
    def begin_nested(self): return Nested()
    def add(self, t):
        if world.fail_add: raise RuntimeError("simulated insert failure")
        self.pending.append(t)
    async def get(self, model, tid):
        t = world.triggers.get(tid)
        if t is not None: self.dirty[tid] = dict(t.__dict__)
        return t
    async def commit(self):
        for tid in list(self.dirty):
            if tid in world.fail_commit_ids: raise RuntimeError(f"simulated commit failure for {tid}")
        for t in self.pending:
            t.id = world.next_id; world.next_id += 1; world.triggers[t.id] = t
            world.events.append(f"saved trigger {t.id} sev={t.severity} value={t.metric_value} posted={t.teams_post_success}")
        self.pending, self.dirty = [], {}
    async def rollback(self):
        for tid, before in self.dirty.items(): world.triggers[tid].__dict__.update(before)
        self.pending, self.dirty = [], {}; world.events.append("rollback")

db_mod = types.ModuleType("database"); db_mod.SessionLocal = Session; sys.modules["database"] = db_mod
rs = types.ModuleType("routes_settings"); rs._config = {"teams_enabled": True, "teams_webhook_url": "https://hook/agent"}; sys.modules["routes_settings"] = rs
import teams_alerts as ta

POST_OK = [True]
def fake_alert_card(agent_name, cluster_name, anomaly): return {"kind": "ALERT", "cluster": cluster_name, **anomaly}
def fake_resolve_card(**kw): return {"kind": "RESOLVE", **kw}
async def fake_send(webhook_url, card, report=None, **kw):
    world.events.append(f"TEAMS {card['kind']} " + (f"[{card['severity']}] {card['description']} | action: {card['recommended_action']}"
                        if card["kind"] == "ALERT" else f"rule={card['rule_name']} subject={card['subject']} severity={card['severity']}"))
    return POST_OK[0]
ta.build_adaptive_card, ta.build_resolve_card, ta.send_to_teams = fake_alert_card, fake_resolve_card, fake_send
ta.outbox.enabled = lambda: False  # the fake session does not model the outbox table
async def names(): return {4: "prod-4", 8: "prod-8"}
ta._get_cluster_names = names

def rule(id, t, cl, tiers, min_rf=None, cooldown=30):
    c = {"mode": "simple", **tiers}
    if min_rf is not None: c["min_rf"] = min_rf
    return NS(id=id, alert_type=t, cluster_id=cl, enabled=True, name=f"rule{id}", severity="warning", config=c, webhook_url=None, cooldown_minutes=cooldown)

RESULTS = []
def check(label, cond):
    RESULTS.append(cond); print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
def cards(kind): return [e for e in world.events if e.startswith(f"TEAMS {kind}")]

async def step(label):
    world.events, world.sql = [], []
    print(f"--- {label}")
    await ta._evaluate_cluster_metrics()
    for e in world.events: print("    event:", e)

def fresh_world(configs, fresh=(4,)):
    world.reset(); ta._threshold_breach_counts.clear(); POST_OK[0] = True
    world.configs, world.fresh = configs, set(fresh)

async def main():
    URP = {"warning": 10, "critical": 100}
    fresh_world([rule(1, "cluster.urp_total", 4, URP)])
    world.urp[4] = 25
    await step("URP 25 (warning), check 1")
    check("no card after 1 check", not cards("ALERT") and not world.triggers)
    await step("URP 25, check 2")
    check("fires after 2 checks", len(cards("ALERT")) == 1 and "25 under-replicated partitions (warning, threshold 10)" in cards("ALERT")[0]
          and "Check broker health and replica sync." in cards("ALERT")[0])
    t = next(iter(world.triggers.values()))
    check("trigger subject=cluster value='25' severity=warning posted", (t.subject, t.metric_value, t.severity, t.teams_post_success) == ("cluster", "25", "warning", True))
    check("card posted before the write session", world.events.index(cards("ALERT")[0]) < world.events.index("session open", 2))
    world.urp[4] = 150
    await step("URP 150 -> escalation")
    check("escalation card, severity critical", len(cards("ALERT")) == 1 and "ESCALATED from warning" in cards("ALERT")[0] and t.severity == "critical" and t.metric_value == "150")
    world.urp[4] = 40
    await step("URP 40 -> de-escalation")
    check("silent, severity back to warning", not cards("ALERT") and t.severity == "warning" and t.metric_value == "40")
    world.urp[4] = 0
    await step("URP 0 -> resolve")
    check("resolved with exactly one resolve card after the write session", t.resolved_at is not None and len(cards("RESOLVE")) == 1
          and "subject=cluster" in cards("RESOLVE")[0] and world.events.index(cards("RESOLVE")[0]) > world.events.index("session closed", 2))
    world.urp[4] = 25
    await step("URP 25 again, check 1 (cooldown 30 min)")
    await step("URP 25 again, check 2 -> still in cooldown")
    check("no card during cooldown", not cards("ALERT") and len(world.triggers) == 1)
    t.resolved_at -= timedelta(minutes=31)
    await step("cooldown over -> fires as RECURRING")
    check("fires again, recurrence flagged", len(cards("ALERT")) == 1 and "RECURRING" in cards("ALERT")[0] and len(world.triggers) == 2)

    fresh_world([rule(1, "cluster.urp_total", 4, URP)])
    world.urp[4] = 25; POST_OK[0] = False
    await step("teams post fails: fire (2 checks) then resolve")
    await ta._evaluate_cluster_metrics()
    t = next(iter(world.triggers.values()))
    world.urp[4] = 0; world.events = []
    await ta._evaluate_cluster_metrics()
    check("trigger saved with teams_post_success=False, resolved, no resolve card", t.teams_post_success is False and t.resolved_at and not cards("RESOLVE"))

    fresh_world([rule(1, "cluster.urp_total", 4, URP), rule(2, "cluster.urp_total", 8, URP)], fresh=(4,))
    world.urp.update({4: 25, 8: 25})
    world.triggers[90] = NS(id=90, alert_config_id=2, cluster_id=8, subject="cluster", severity="warning",
                            triggered_at=datetime.now(timezone.utc), resolved_at=None, teams_post_success=True, metric_value="25")
    ta._threshold_breach_counts[(2, 8, "cluster")] = 1
    world.urp[8] = 0
    await step("cluster 8 structure job stale (value 0, open trigger, count 1)")
    await ta._evaluate_cluster_metrics()
    check("cluster 8 ignored: no resolve, counter unchanged, not queried",
          world.triggers[90].resolved_at is None and ta._threshold_breach_counts[(2, 8, "cluster")] == 1
          and all(8 not in (p or {}).get("cids", []) for _, p in world.sql))
    check("cluster 4 still evaluated and fired", any(tr.cluster_id == 4 for tr in world.triggers.values()))
    fq = next((s, p) for s, p in world.sql if "kafka_job_runs" in s)
    check("freshness SQL: status success, last 15 min, bound job ids",
          "status = 'success'" in fq[0] and ":since" in fq[0] and sorted(fq[1]["job_ids"]) == ["kafka-topic-structure-4", "kafka-topic-structure-8"]
          and timedelta(minutes=14, seconds=59) < datetime.now(timezone.utc) - fq[1]["since"] < timedelta(minutes=15, seconds=5))

    RF = {"warning": 1, "critical": 5}
    fresh_world([rule(3, "cluster.rf_below_min", 4, RF, min_rf=3), rule(4, "cluster.rf_below_min", 8, RF, min_rf=2)], fresh=(4, 8))
    world.rf_topics = {4: [1, 2, 2, 3, 3], 8: [1, 2, 2, 3]}
    await step("RF rules: cluster 4 min_rf=3, cluster 8 min_rf=2, two checks")
    await ta._evaluate_cluster_metrics()
    rq = [(s, p) for s, p in world.sql if "replication_factor" in s]
    check("one query per rule, own min_rf bound", sorted((p["cid"], p["min_rf"]) for _, p in rq) == [(4, 3), (4, 3), (8, 2), (8, 2)])
    sql = rq[0][0]
    check("exclusions in SQL text", r"topic NOT LIKE '\_\_%'" in sql and "topic <> '_schemas'" in sql and "partition_count > 0" in sql and ":min_rf" in sql)
    by_cluster = {tr.cluster_id: tr for tr in world.triggers.values()}
    check("values: cluster 4 -> '3', cluster 8 -> '1'", by_cluster[4].metric_value == "3" and by_cluster[8].metric_value == "1")
    check("RF description and action", any("3 topics have replication factor below 3 (system topics excluded; min.insync.replicas is not checked)" in c
          and "Check the topic replication factors with the Kafka team." in c for c in cards("ALERT")))
    uq = [(s, p) for s, p in world.sql if "urp_count" in s]
    check("no URP query when no URP rules", not uq)

    fresh_world([rule(1, "cluster.urp_total", 4, URP), rule(2, "cluster.urp_total", 8, URP)], fresh=(4, 8))
    world.urp.update({4: 25, 8: 25})
    await step("URP rules on 4 and 8: one grouped query")
    uq = [(s, p) for s, p in world.sql if "urp_count" in s]
    check("single grouped query, both clusters bound", len(uq) == 1 and uq[0][1] == {"cids": [4, 8]} and "GROUP BY cluster_id" in uq[0][0] and "partition_count > 0" in uq[0][0])

    fresh_world([rule(1, "cluster.urp_total", 4, URP)])
    world.urp[4] = 25; world.fail_add = True
    await step("insert save fails (2 checks)")
    await ta._evaluate_cluster_metrics()
    check("insert failure logged, nothing saved, no raise", not world.triggers)
    world.fail_add = False
    fresh_world([rule(1, "cluster.urp_total", 4, URP)])
    world.triggers[91] = NS(id=91, alert_config_id=1, cluster_id=4, subject="cluster", severity="warning",
                            triggered_at=datetime.now(timezone.utc), resolved_at=None, teams_post_success=True, metric_value="25")
    world.urp[4] = 0; world.fail_commit_ids = {91}
    await step("resolve save fails")
    check("no resolve card, trigger still open", not cards("RESOLVE") and world.triggers[91].resolved_at is None)

    for frag in ("kafka_job_runs", "urp_count"):
        fresh_world([rule(1, "cluster.urp_total", 4, URP)])
        world.urp[4] = 25; world.fail_query = frag
        await step(f"query on {frag} fails")
        check("did not raise, nothing fired", not world.triggers)
    fresh_world([rule(3, "cluster.rf_below_min", 4, RF, min_rf=3)]); world.fail_query = "replication_factor"
    await step("rf query fails")
    check("did not raise", True)

    fresh_world([rule(3, "cluster.rf_below_min", 4, RF), rule(5, "cluster.urp_total", None, URP)])
    await step("rf rule without min_rf, urp rule without cluster")
    check("both skipped with warnings, no queries for them", not [s for s, _ in world.sql if "replication_factor" in s or "urp_count" in s])

    world.reset(); rs._config["teams_enabled"] = False
    await step("teams_enabled off"); rs._config["teams_enabled"] = True
    world.configs = []
    await step("no enabled rules")
    db_mod.SessionLocal = None; ta.SessionLocal = None
    await step("no database"); ta.SessionLocal = Session

    print("--- evaluate_alerts: order and isolation")
    calls = []
    def mk(n, boom):
        async def f():
            calls.append(n)
            if boom: raise RuntimeError(f"{n} boom")
        return f
    names_ = ["_evaluate_broker_reachability", "_evaluate_broker_thresholds", "_evaluate_cluster_metrics"]
    orig = {n: getattr(ta, n) for n in names_}
    for raising in [None] + names_:
        calls.clear()
        for n in names_: setattr(ta, n, mk(n, raising == "all" or n == raising))
        await ta.evaluate_alerts()
        check(f"all three called in order (raising: {raising})", calls == names_)
    for n in names_: setattr(ta, n, mk(n, True))
    calls.clear(); await ta.evaluate_alerts()
    check("survives all three raising", calls == names_)
    for n, f in orig.items(): setattr(ta, n, f)

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
