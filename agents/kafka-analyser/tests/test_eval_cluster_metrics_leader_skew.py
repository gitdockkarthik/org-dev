"""teams_alerts._evaluate_cluster_metrics for leader partition skew (cluster.leader_skew).

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
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_leader_skew.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_leader_skew.py"""
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
        self.next_id = 500; self.dist = {}
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
            if "kafka_broker_distribution" in sql:
                out = []
                for c in params["cids"]:
                    if c in world.dist:
                        counts, last = world.dist[c]
                        out.append(NS(cluster_id=c, brokers=len(counts), max_leaders=max(counts) if counts else None,
                                      avg_leaders=(__import__("decimal").Decimal(sum(counts)) / len(counts)) if counts else None, last_updated=last))
                return Res(out)
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
        return Res([c for c in world.configs if c.enabled or getattr(c, "email_enabled", False)])
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
def fresh_world(configs, fresh=()):
    world.reset(); ta._threshold_breach_counts.clear(); POST_OK[0] = True
    world.configs, world.fresh = configs, set(fresh)
NOW = lambda: datetime.now(timezone.utc)
SKEW = {"warning": 1.5, "critical": 2.5}
def types_fired(): return {next(c for c in world.configs if c.id == t.alert_config_id).alert_type for t in world.triggers.values()}

async def main():
    # Ratio math
    fresh_world([rule(1, "cluster.leader_skew", 4, SKEW), rule(2, "cluster.leader_skew", 8, SKEW),
                 rule(3, "cluster.leader_skew", 9, SKEW), rule(4, "cluster.leader_skew", 10, SKEW)])
    world.dist = {4: ([1, 99, 7886], NOW()), 8: ([10712, 10198, 10195], NOW()), 9: ([500], NOW()), 10: ([0, 0, 0], NOW())}
    await step("ratio math, check 1")
    await step("ratio math, check 2")
    trig = {t.cluster_id: t for t in world.triggers.values()}
    check("cluster 4 (1, 99, 7886) -> 2.96, critical, stored '2.96'", 4 in trig and trig[4].metric_value == "2.96" and trig[4].severity == "critical")
    ws = {"warning": 1.0}
    fresh_world([rule(2, "cluster.leader_skew", 8, ws)]); world.dist = {8: ([10712, 10198, 10195], NOW())}
    await step("cluster 8 with warning 1.0, check 1"); await step("cluster 8 with warning 1.0, check 2")
    t8 = next(iter(world.triggers.values()), None)
    check("cluster 8 (10712, 10198, 10195) -> 1.03", t8 is not None and t8.metric_value == "1.03")
    fresh_world([rule(3, "cluster.leader_skew", 9, ws), rule(4, "cluster.leader_skew", 10, ws)])
    world.dist = {9: ([500], NOW()), 10: ([0, 0, 0], NOW())}
    await step("1-broker cluster 9, zero-leader cluster 10 (warning 1.0), check 1"); await step("check 2")
    check("both skipped: no trigger, counters untouched", not world.triggers and not ta._threshold_breach_counts)
    fresh_world([rule(1, "cluster.leader_skew", 4, SKEW), rule(2, "cluster.leader_skew", 8, SKEW)])
    world.dist = {4: ([1, 99, 7886], NOW()), 8: ([10712, 10198, 10195], NOW())}
    await step("two clusters, check 1"); await step("two clusters, check 2")
    sk = [(s, p) for s, p in world.sql if "kafka_broker_distribution" in s]
    check("one grouped query with bound cluster ids", len(sk) == 1 and sk[0][1] == {"cids": [4, 8]} and "GROUP BY cluster_id" in sk[0][0] and "IN :cids" in sk[0][0])
    check("structure-job query not run for skew-only rules", not any("kafka_job_runs" in s for s, _ in world.sql))
    a = cards("ALERT")
    check("card text and action", len(a) == 1 and "Leader partitions are skewed: busiest broker has 2.96 times the cluster average (critical, threshold 2.5)" in a[0]
          and "Review leader distribution with the Kafka team; a preferred leader election may rebalance it." in a[0])
    print("    card:", a[0] if a else None)

    # Fire on the 2nd check, escalate, resolve
    fresh_world([rule(1, "cluster.leader_skew", 4, SKEW)])
    world.dist = {4: ([100, 100, 200], NOW())}   # 200 / 133.33 = 1.5 -> warning
    await step("1.5 (warning), check 1")
    check("no card after 1 check", not cards("ALERT") and not world.triggers)
    await step("1.5, check 2")
    t = next(iter(world.triggers.values()), None)
    check("fires on 2nd check, warning, value '1.5'", len(cards("ALERT")) == 1 and t and t.severity == "warning" and t.metric_value == "1.5" and t.subject == "cluster")
    world.dist = {4: ([10, 10, 300], NOW())}     # 300 / 106.67 = 2.81 -> critical
    await step("2.81 -> escalation")
    check("escalation card, critical, '2.81'", len(cards("ALERT")) == 1 and "ESCALATED from warning" in cards("ALERT")[0] and t.severity == "critical" and t.metric_value == "2.81")
    world.dist = {4: ([100, 100, 100], NOW())}   # 1.0 -> below every tier
    await step("1.0 -> resolve")
    check("resolved with one resolve card", t.resolved_at is not None and len(cards("RESOLVE")) == 1)

    # Stale distribution (structure job fresh)
    fresh_world([rule(1, "cluster.leader_skew", 4, SKEW)], fresh=(4,))
    world.dist = {4: ([100, 100, 300], NOW() - timedelta(minutes=16))}
    world.triggers[90] = NS(id=90, alert_config_id=1, cluster_id=4, subject="cluster", severity="warning",
                            triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="1.8")
    ta._threshold_breach_counts[(1, 4, "cluster")] = 1
    await step("distribution updated 16 min ago"); await step("again")
    check("stale: no fire, no resolve, counter unchanged",
          not cards("ALERT") and world.triggers[90].resolved_at is None and ta._threshold_breach_counts[(1, 4, "cluster")] == 1)
    world.dist = {4: ([100, 100, 100], NOW() - timedelta(minutes=16))}
    await step("stale and balanced")
    check("no resolve from stale data", world.triggers[90].resolved_at is None)
    world.dist = {}
    await step("no distribution rows")
    check("no rows -> ignored", world.triggers[90].resolved_at is None and not cards("ALERT"))

    # Guards are per type
    fresh_world([rule(1, "cluster.leader_skew", 4, SKEW), rule(2, "cluster.urp_total", 4, {"warning": 10})], fresh=())
    world.dist = {4: ([10, 10, 300], NOW())}; world.urp[4] = 50
    await step("structure stale, distribution fresh, check 1"); await step("check 2")
    check("skew fires; URP still ignored by the structure guard", types_fired() == {"cluster.leader_skew"})
    fresh_world([rule(1, "cluster.leader_skew", 4, SKEW), rule(2, "cluster.urp_total", 4, {"warning": 10})], fresh=(4,))
    world.dist = {4: ([10, 10, 300], NOW() - timedelta(hours=1))}; world.urp[4] = 50
    await step("structure fresh, distribution stale, check 1"); await step("check 2")
    check("URP fires; skew ignored", types_fired() == {"cluster.urp_total"})

    fresh_world([rule(1, "cluster.leader_skew", 4, SKEW)]); world.fail_query = "kafka_broker_distribution"
    await step("skew query fails")
    check("did not raise", True)

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
