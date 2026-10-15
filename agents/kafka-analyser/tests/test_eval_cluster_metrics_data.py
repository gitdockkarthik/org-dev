"""teams_alerts._evaluate_cluster_metrics for data on disk (cluster.data_gb_max) and data spread between brokers (cluster.data_spread_pct).

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
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_data.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_data.py"""
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
        self.next_id = 500; self.dist = {}; self.snap = []; self.bm = []
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
            if "FROM kafka_broker_metrics" in sql:
                return Res([NS(cluster_id=c, broker_id=b, data_gb_true=gb, time=t) for (c, b, gb, t) in world.bm if c in params["cids"]])
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


LOGS = []
class _Cap(logging.Handler):
    def emit(self, rec): LOGS.append(rec.getMessage())
logging.getLogger("teams_alerts").addHandler(_Cap())
RESULTS = []
def check(label, cond):
    RESULTS.append(cond); print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
def cards(kind): return [e for e in world.events if e.startswith(f"TEAMS {kind}")]
async def step(label):
    world.events, world.sql = [], []; LOGS.clear()
    print(f"--- {label}")
    await ta._evaluate_cluster_metrics()
    for e in world.events: print("    event:", e)
def fresh_world(configs):
    world.reset(); ta._threshold_breach_counts.clear(); ta._msg_rate_last_run.clear(); POST_OK[0] = True
    world.configs = configs
NOW = lambda: datetime.now(timezone.utc)
def brokers(cl, gbs, age_min=1, overrides=None):
    for i, gb in enumerate(gbs, start=1):
        t = NOW() - timedelta(minutes=age_min)
        if overrides and i in overrides: gb, t = overrides[i].get("gb", gb), overrides[i].get("time", t)
        world.bm.append((cl, str(i), gb, t))
MAX_T = {"warning": 800, "critical": 1000}
SPREAD_T = {"warning": 10, "critical": 50}
def ignored_log(): return [l for l in LOGS if "data on disk ignored" in l]

async def main():
    # Values
    fresh_world([rule(1, "cluster.data_gb_max", 9, MAX_T), rule(2, "cluster.data_spread_pct", 9, SPREAD_T)])
    brokers(9, [813.7, 812.0, 812.2])
    await step("cluster 9 (813.7, 812.0, 812.2), check 1"); await step("check 2")
    by_type = {next(c for c in world.configs if c.id == t.alert_config_id).alert_type: t for t in world.triggers.values()}
    check("cluster 9: max fires with '813.7' (warning); spread 0.2% below 10 -> no trigger",
          "cluster.data_gb_max" in by_type and by_type["cluster.data_gb_max"].metric_value == "813.7" and "cluster.data_spread_pct" not in by_type)
    check("max card names the broker", any("Broker 1 holds 813.7 GB (warning, threshold 800)" in c and "Check disk capacity and retention with the Kafka team." in c for c in cards("ALERT")))
    print("    card:", cards("ALERT")[0] if cards("ALERT") else None)
    q = [(s, p) for s, p in world.sql if "FROM kafka_broker_metrics" in s]
    check("one bound query for both types on the cluster", len(q) == 1 and q[0][1] == {"cids": [9]} and "IN :cids" in q[0][0])
    check("no structure-job query", not any("kafka_job_runs" in s for s, _ in world.sql))
    fresh_world([rule(2, "cluster.data_spread_pct", 9, {"warning": 0.1})]); brokers(9, [813.7, 812.0, 812.2])
    await step("cluster 9 spread with warning 0.1, check 1"); await step("check 2")
    t = next(iter(world.triggers.values()), None)
    check("cluster 9 spread value '0.2'", t is not None and t.metric_value == "0.2")

    # Skewed cluster: both types in one run, card text
    fresh_world([rule(1, "cluster.data_gb_max", 4, MAX_T), rule(2, "cluster.data_spread_pct", 4, SPREAD_T)])
    brokers(4, [500, 800, 810])
    await step("skewed (500, 800, 810), check 1")
    check("no card after 1 check", not cards("ALERT"))
    await step("check 2")
    by_type = {next(c for c in world.configs if c.id == t.alert_config_id).alert_type: t for t in world.triggers.values()}
    check("both types fire on one cluster in one run: max '810.0' warning, spread '38.3' warning",
          by_type.get("cluster.data_gb_max") and by_type["cluster.data_gb_max"].metric_value == "810.0"
          and by_type.get("cluster.data_spread_pct") and by_type["cluster.data_spread_pct"].metric_value == "38.3" and len(cards("ALERT")) == 2)
    sp = [c for c in cards("ALERT") if "spread" in c]
    check("spread card names both brokers", sp and "Broker 1 holds 500.0 GB vs broker 3 at 810.0 GB: spread 38.3% (warning, threshold 10)" in sp[0]
          and "Check whether the low broker is replicating correctly (rebuilt, lost disks or not catching up)." in sp[0])
    print("    card:", sp[0] if sp else None)
    ts = by_type["cluster.data_spread_pct"]
    world.bm = []; brokers(4, [300, 800, 810])   # spread 63.0 -> critical
    await step("spread 63.0 -> escalation")
    check("spread escalates to critical with a card", ts.severity == "critical" and ts.metric_value == "63.0" and any("ESCALATED from warning" in c and "spread" in c for c in cards("ALERT")))
    world.bm = []; brokers(4, [790, 795, 799])   # spread 1.1, max 799 -> below every tier for both
    await step("balanced and below 800 -> both resolve")
    check("both resolve with one resolve card each", all(t.resolved_at for t in by_type.values()) and len(cards("RESOLVE")) == 2)

    # Ignore cases
    cases = [
        ("NULL broker", lambda: brokers(8, [500, 800, 810], overrides={2: {"gb": None}}), "8: broker 2 has no data_gb_true"),
        ("stale broker (7 min)", lambda: brokers(8, [500, 800, 810], overrides={3: {"time": NOW() - timedelta(minutes=7)}}), "8: broker 3 has no metrics in the last 6 min (last 7 min ago)"),
        ("single broker", lambda: brokers(8, [900]), "8: only broker 1 has a metrics row"),
        ("no rows", lambda: None, "8: no broker metrics rows"),
    ]
    for label, setup, expect in cases:
        fresh_world([rule(1, "cluster.data_gb_max", 8, MAX_T), rule(2, "cluster.data_spread_pct", 8, SPREAD_T)])
        setup()
        for rid in (1, 2):
            world.triggers[90 + rid] = NS(id=90 + rid, alert_config_id=rid, cluster_id=8, subject="cluster", severity="warning", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="1")
            ta._threshold_breach_counts[(rid, 8, "cluster")] = 1
        await step(label); await step(label + " (again)")
        check(f"{label}: ignored, no fire/resolve, counters unchanged, log names it",
              not cards("ALERT") and not cards("RESOLVE") and all(world.triggers[90 + r].resolved_at is None for r in (1, 2))
              and all(ta._threshold_breach_counts[(r, 8, "cluster")] == 1 for r in (1, 2)) and any(expect in l for l in ignored_log()))
        print("    log:", ignored_log())
    fresh_world([rule(1, "cluster.data_gb_max", 8, MAX_T), rule(2, "cluster.data_spread_pct", 8, SPREAD_T)])
    brokers(8, [0, 0, 0])
    world.triggers[92] = NS(id=92, alert_config_id=2, cluster_id=8, subject="cluster", severity="warning", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="40")
    ta._threshold_breach_counts[(2, 8, "cluster")] = 1
    await step("max 0 (all brokers 0 GB)")
    check("max 0: spread skipped (open trigger kept, counter unchanged), log names the broker; max rule evaluates 0.0 (no fire)",
          world.triggers[92].resolved_at is None and ta._threshold_breach_counts[(2, 8, "cluster")] == 1
          and any("8: spread skipped: largest broker 1 holds 0 GB" in l for l in ignored_log()) and not cards("ALERT"))
    print("    log:", ignored_log())

    fresh_world([rule(1, "cluster.data_gb_max", 8, MAX_T)]); brokers(8, [1, 2]); world.fail_query = "FROM kafka_broker_metrics"
    await step("query fails")
    check("did not raise", True)

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
