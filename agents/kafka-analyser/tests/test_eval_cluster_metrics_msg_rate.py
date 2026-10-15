"""teams_alerts._evaluate_cluster_metrics for message rate in (cluster.msg_rate_in, fires BELOW a tier).

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
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_msg_rate.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_msg_rate.py"""
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
        self.next_id = 500; self.dist = {}; self.snap = []
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
            if "kafka_topic_message_rate_snapshots" in sql:
                rows = [r for r in world.snap if r["cluster_id"] in params["cids"]]
                if "inflow IS NOT NULL" in sql:
                    rows = [r for r in rows if r["inflow"] is not None]
                if "MAX(collected_at)" in sql:
                    rows = [r for r in rows if r["collected_at"] >= params["since"]]
                    out = {}
                    for r in rows: out[r["cluster_id"]] = max(out.get(r["cluster_id"], r["collected_at"]), r["collected_at"])
                    return Res([NS(cluster_id=c, collected_at=t) for c, t in out.items()])
                rows = [r for r in rows if r["collected_at"] in params["runs"]]
                groups = {}
                for r in rows:
                    g = groups.setdefault((r["cluster_id"], r["collected_at"]), {"msgs": None, "iv": None})
                    if r["inflow"] is not None: g["msgs"] = (g["msgs"] or 0) + r["inflow"]
                    if r["interval_seconds"] is not None: g["iv"] = max(g["iv"] or r["interval_seconds"], r["interval_seconds"])
                return Res([NS(cluster_id=k[0], collected_at=k[1], msgs=v["msgs"], interval_seconds=v["iv"]) for k, v in groups.items()])
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
    world.reset(); ta._threshold_breach_counts.clear(); ta._msg_rate_last_run.clear(); POST_OK[0] = True
    world.configs, world.fresh = configs, set(fresh)
NOW = lambda: datetime.now(timezone.utc)
TIERS = {"info": 1, "warning": 0.5, "critical": 0}
def below_rule(id, cl, tiers=TIERS): return rule(id, "cluster.msg_rate_in", cl, {**tiers, "direction": "below"})
def run(cl, per_topic, interval=600.0, age_min=2, outflow_too=True):
    """One inflow run: rows per topic sharing collected_at and interval, plus outflow-only rows."""
    at = NOW() - timedelta(minutes=age_min)
    for i, msgs in enumerate(per_topic):
        world.snap.append({"cluster_id": cl, "topic": f"t{i}", "inflow": msgs, "interval_seconds": interval, "collected_at": at})
    if outflow_too:  # consumer-lag rows: huge outflow, its own interval, newer timestamp
        world.snap.append({"cluster_id": cl, "topic": "t0", "inflow": None, "outflow": 10**9, "interval_seconds": 180.0, "collected_at": NOW() - timedelta(seconds=30)})
    return at

async def main():
    print("--- below-tier helper (info 1 / warning 0.5 / critical 0)")
    got = {v: ta._threshold_tier_below(TIERS, v) for v in (0, 0.3, 0.7, 5)}
    print("   ", got)
    check("0 -> critical, 0.3 -> warning, 0.7 -> info, 5 -> None", got == {0: "critical", 0.3: "warning", 0.7: "info", 5: None})
    check("above-direction helper unchanged (cpu tiers 70/90: 95 -> critical, 75 -> warning, 10 -> None)",
          [ta._threshold_tier({"warning": 70, "critical": 90}, v) for v in (95, 75, 10)] == ["critical", "warning", None])

    # Value, outflow rows ignored, queries
    fresh_world([below_rule(1, 8)])
    run(8, [120, 60], interval=600.0)   # 180 msgs / 600 s = 0.3 msgs/sec
    await step("run A (0.3 = warning), evaluation 1")
    lat = next(((s, p) for s, p in world.sql if "MAX(collected_at)" in s), None)
    agg = next(((s, p) for s, p in world.sql if "SUM(inflow)" in s), None)
    check("latest-run query: IN :cids, inflow IS NOT NULL, :since, GROUP BY", lat and "cluster_id IN :cids" in lat[0] and "inflow IS NOT NULL" in lat[0]
          and "collected_at >= :since" in lat[0] and "GROUP BY cluster_id" in lat[0] and lat[1]["cids"] == [8])
    check("run query: bound cids and runs, inflow IS NOT NULL", agg and "inflow IS NOT NULL" in agg[0] and agg[1]["cids"] == [8] and len(agg[1]["runs"]) == 1)
    check("outflow-only rows ignored (latest run is the inflow run, not the newer outflow row)", agg and agg[1]["runs"][0] < NOW() - timedelta(minutes=1))
    check("no card after the first run", not cards("ALERT") and not world.triggers)
    await step("run A again (same run), evaluation 2")
    check("same run evaluated twice: no fire, counter kept at 1", not cards("ALERT") and ta._threshold_breach_counts.get((1, 8, "cluster")) == 1)
    await step("run A again, evaluation 3")
    check("still 1 after a third look", ta._threshold_breach_counts.get((1, 8, "cluster")) == 1 and not world.triggers)
    world.snap = []; run(8, [150, 30], interval=600.0, age_min=1)   # 0.3 again, new run
    await step("run B (new collected_at, 0.3)")
    t = next(iter(world.triggers.values()), None)
    check("fires on the 2nd distinct run: warning, value '0.3'", len(cards("ALERT")) == 1 and t and t.severity == "warning" and t.metric_value == "0.3")
    print("    card:", cards("ALERT")[0] if cards("ALERT") else None)
    check("card text, action", cards("ALERT") and "Cluster inflow is 0.3 msgs/sec over the last 10.0 minutes (warning, threshold 0.5)" in cards("ALERT")[0]
          and "Check producers and connectors writing to this cluster." in cards("ALERT")[0])

    # Escalate toward critical, de-escalate silently, resolve with card
    world.snap = []; run(8, [0, 0], interval=600.0, age_min=0.5)
    await step("run C: 0 msgs/sec -> critical")
    check("escalation card toward critical", len(cards("ALERT")) == 1 and "ESCALATED from warning" in cards("ALERT")[0] and t.severity == "critical" and t.metric_value == "0.0")
    world.snap = []; run(8, [420], interval=600.0, age_min=0.4)   # 0.7 -> info
    await step("run D: 0.7 -> info")
    check("de-escalation silent, severity info", not cards("ALERT") and t.severity == "info" and t.metric_value == "0.7")
    world.snap = []; run(8, [3000], interval=600.0, age_min=0.3)  # 5.0 -> above every tier
    await step("run E: 5.0 -> resolve")
    check("resolved with one resolve card", t.resolved_at is not None and len(cards("RESOLVE")) == 1)

    # Resolve may happen on a repeated evaluation of the same run
    fresh_world([below_rule(1, 8)])
    at = run(8, [3000], interval=600.0)
    world.triggers[91] = NS(id=91, alert_config_id=1, cluster_id=8, subject="cluster", severity="warning", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="0.3")
    ta._msg_rate_last_run[(1, 8, "cluster")] = at   # this run was already seen
    await step("open trigger, same run seen before, value 5.0")
    check("resolves on a repeated evaluation", world.triggers[91].resolved_at is not None and len(cards("RESOLVE")) == 1)

    # Guards
    for label, kw in [("run older than 15 min", dict(age_min=16)), ("interval 1201 s", dict(interval=1201.0)),
                      ("interval missing", dict(interval=None)), ("interval 0", dict(interval=0.0))]:
        fresh_world([below_rule(1, 8)])
        run(8, [0], **kw)
        world.triggers[92] = NS(id=92, alert_config_id=1, cluster_id=8, subject="cluster", severity="warning", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="0.3")
        ta._threshold_breach_counts[(1, 8, "cluster")] = 1
        await step(label); await step(label + " (again)")
        check(f"{label}: ignored (no fire, no resolve, counter unchanged, no run recorded)",
              not cards("ALERT") and not cards("RESOLVE") and world.triggers[92].resolved_at is None and world.triggers[92].severity == "warning"
              and ta._threshold_breach_counts[(1, 8, "cluster")] == 1 and (1, 8, "cluster") not in ta._msg_rate_last_run)
    fresh_world([below_rule(1, 8)])
    world.snap = [{"cluster_id": 8, "topic": "t0", "inflow": None, "outflow": 5, "interval_seconds": 180.0, "collected_at": NOW()}]
    await step("only outflow rows")
    check("only outflow rows -> no inflow run -> ignored", not world.triggers and not ta._msg_rate_last_run)
    fresh_world([below_rule(1, 8)])
    await step("no rows")
    check("no rows -> ignored", not world.triggers)
    fresh_world([below_rule(1, 8)])
    run(8, [100], interval=1200.0)
    await step("interval exactly 1200 s, check 1")
    check("1200 s accepted (value 0.08 -> critical, counter 1)", ta._threshold_breach_counts.get((1, 8, "cluster")) == 1)

    # Two clusters with different latest run times
    fresh_world([below_rule(1, 8), below_rule(2, 10)])
    run(8, [60], interval=600.0, age_min=3); run(10, [6000], interval=600.0, age_min=7)
    await step("two clusters, different run times")
    agg = next(((s, p) for s, p in world.sql if "SUM(inflow)" in s), None)
    check("one run query for both clusters, two run times", agg and agg[1]["cids"] == [8, 10] and len(agg[1]["runs"]) == 2)
    check("per-cluster counters: 8 (0.1, critical) counted, 10 (10.0, above) not", ta._threshold_breach_counts.get((1, 8, "cluster")) == 1 and ta._threshold_breach_counts.get((2, 10, "cluster")) == 0)
    check("no structure-job query for rate-only rules", not any("kafka_job_runs" in s for s, _ in world.sql))

    fresh_world([below_rule(1, 8)]); run(8, [0]); world.fail_query = "kafka_topic_message_rate_snapshots"
    await step("rate query fails")
    check("did not raise", True)

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
