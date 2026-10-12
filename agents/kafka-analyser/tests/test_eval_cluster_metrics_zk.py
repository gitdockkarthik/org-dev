"""teams_alerts._evaluate_cluster_metrics for ZooKeeper ensemble problems (cluster.zk_ensemble, live mntr probes faked).

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
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_zk.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_zk.py"""
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
        self.next_id = 500; self.dist = {}; self.snap = []; self.bm = []; self.zk_urls = {}; self.session_open = 0
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
    async def __aenter__(self): world.session_open += 1; world.events.append("session open"); return self
    async def __aexit__(self, *a): world.session_open -= 1; world.events.append("session closed"); return False
    async def execute(self, stmt, params=None):
        if isinstance(stmt, TextClause):
            sql = stmt.text; world.sql.append((sql, params))
            if world.fail_query and world.fail_query in sql: raise RuntimeError("simulated query failure")
            if "kafka_job_runs" in sql:
                return Res([j for j in params["job_ids"] if int(j.rsplit("-", 1)[1]) in world.fresh])
            if "urp_count" in sql:
                return Res([NS(cluster_id=c, value=world.urp[c]) for c in params["cids"] if c in world.urp])
            if "FROM kafka_clusters" in sql:
                return Res([NS(id=c, zookeeper_url=world.zk_urls[c]) for c in params["cids"] if c in world.zk_urls])
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
import time
from tools.zookeeper import _parse_mntr as real_parse_mntr
assert ta._parse_mntr is real_parse_mntr

# Fake _zk_command: node behaviour from ZK[(host, port)].
ZK, ZK_CALLS = {}, []
def mntr(state): return f"zk_version\t3.6.3\nzk_server_state\t{state}\nzk_znode_count\t42\n"
async def fake_zk_command(host, port, cmd, timeout=5.0):
    ZK_CALLS.append((host, port, cmd, timeout, world.session_open))
    b = ZK.get((host, port), "down")
    if b == "down": raise RuntimeError(f"ZooKeeper command '{cmd}' failed: [Errno 111] Connection refused")
    if b == "slow": await asyncio.sleep(30); return mntr("follower")
    if b == "garbage": return "mntr is not executed because it is not in the whitelist.\n"
    return mntr(b)
ta._zk_command = fake_zk_command

ZK_T = {"warning": 1, "critical": 2}
def ens(cl, prefix, states):
    """Cluster cl with nodes <prefix>1..n:2181 and the given behaviours."""
    world.zk_urls[cl] = ",".join(f"{prefix}{i}:2181" for i in range(1, len(states) + 1))
    for i, s in enumerate(states, start=1): ZK[(f"{prefix}{i}", 2181)] = s
def zk_world(configs):
    fresh_world(configs); ZK.clear()
def zk_rule(id, cl, tiers=ZK_T): return rule(id, "cluster.zk_ensemble", cl, tiers)
def ignored_log(): return [l for l in LOGS if "ZooKeeper ensemble ignored" in l]
async def zk_step(label):
    ZK_CALLS.clear(); await step(label)
def trig(): return next(iter(world.triggers.values()), None)

async def value_case(label, states, expect_value, expect_text):
    zk_world([zk_rule(1, 4, {"info": 1})]); ens(4, "zk", states)
    await zk_step(f"{label}, check 1"); await zk_step(f"{label}, check 2")
    t = trig()
    if expect_value == 0:
        check(f"{label}: value 0, no trigger, no card", t is None and not cards("ALERT"))
    else:
        al = cards("ALERT")
        check(f"{label}: value {expect_value}, card '{expect_text}'",
              t is not None and t.metric_value == str(expect_value) and al and expect_text in al[0])
        print("    card:", al[0] if al else None)

async def main():
    # ---- Values ----
    await value_case("healthy 3 nodes (1 leader)", ["leader", "follower", "follower"], 0, None)
    await value_case("one node down", ["leader", "down", "follower"], 1,
                     "ZooKeeper: 1 problem(s): down zk2:2181; leader zk1:2181 (info, threshold 1)")
    await value_case("two nodes down (leader survives)", ["down", "leader", "down"], 2,
                     "ZooKeeper: 2 problem(s): down zk1:2181, zk3:2181; leader zk2:2181 (info, threshold 1)")
    await value_case("two nodes down (follower survives)", ["down", "follower", "down"], 3,
                     "ZooKeeper: 3 problem(s): down zk1:2181, zk3:2181; no leader (info, threshold 1)")
    await value_case("two leaders", ["leader", "leader", "follower"], 1,
                     "ZooKeeper: 1 problem(s): 2 leaders zk1:2181, zk2:2181 (info, threshold 1)")
    await value_case("zero leaders, 3 answering", ["follower", "follower", "follower"], 1,
                     "ZooKeeper: 1 problem(s): no leader (info, threshold 1)")
    await value_case("all down", ["down", "down", "down"], 3,
                     "ZooKeeper: 3 problem(s): down zk1:2181, zk2:2181, zk3:2181; no node reachable from the agent: check the network path first (info, threshold 1)")
    await value_case("all answer without zk_server_state (whitelist)", ["garbage", "garbage", "garbage"], 3,
                     "ZooKeeper: 3 problem(s): down zk1:2181, zk2:2181, zk3:2181; no node returned zk_server_state (is mntr in 4lw.commands.whitelist?)")
    await value_case("one garbage answer + leader + follower", ["leader", "garbage", "follower"], 1,
                     "ZooKeeper: 1 problem(s): down zk2:2181; leader zk1:2181")

    # ---- Slow node: bounded by the 3 s timeout, nodes in parallel ----
    zk_world([zk_rule(1, 4, {"info": 1})]); ens(4, "zk", ["leader", "slow", "slow"])
    t0 = time.monotonic(); await zk_step("two slow nodes"); dt = time.monotonic() - t0
    check(f"slow nodes time out in parallel: run took {dt:.1f}s (3 s timeout, not 6)", 2.9 <= dt < 4.0)
    check("slow nodes counted down (breach counter 1 after 1 check)", ta._threshold_breach_counts.get((1, 4, "cluster")) == 1)
    check("each probe called with mntr and timeout=3", all(c[2] == "mntr" and c[3] == 3 for c in ZK_CALLS) and len(ZK_CALLS) == 3)

    # ---- Skips: empty URL, 1-node URL, cluster missing ----
    for label, url, expect in [("empty URL", "", "4: zookeeper_url is empty"), ("whitespace/commas URL", " , ", "4: zookeeper_url is empty"),
                               ("1-node URL", "zk1:2181", "4: zookeeper_url lists only zk1:2181"),
                               ("NULL URL", None, "4: zookeeper_url is empty"), ("cluster row missing", "MISSING", "4: cluster not found in kafka_clusters")]:
        zk_world([zk_rule(1, 4)])
        if url != "MISSING": world.zk_urls[4] = url
        ZK[("zk1", 2181)] = "down"
        world.triggers[91] = NS(id=91, alert_config_id=1, cluster_id=4, subject="cluster", severity="warning", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="1")
        ta._threshold_breach_counts[(1, 4, "cluster")] = 1
        await zk_step(label); await zk_step(label + " (again)")
        check(f"{label}: skipped (no probe, no fire/resolve, counter unchanged), INFO names it",
              not ZK_CALLS and not cards("ALERT") and not cards("RESOLVE") and world.triggers[91].resolved_at is None
              and ta._threshold_breach_counts[(1, 4, "cluster")] == 1 and any(expect in l for l in ignored_log()))
        print("    log:", ignored_log())

    # ---- Lifecycle: fire on 2nd check, escalate, de-escalate silently, resolve card ----
    zk_world([zk_rule(1, 8)]); ens(8, "stg", ["leader", "down", "follower"])
    await zk_step("lifecycle: 1 down, check 1")
    check("no card after 1 check", not cards("ALERT") and not world.triggers)
    await zk_step("lifecycle: 1 down, check 2")
    t = trig()
    check("fires on 2nd check: warning, value '1', card text + action + category",
          t is not None and t.severity == "warning" and t.metric_value == "1"
          and any("ZooKeeper: 1 problem(s): down stg2:2181; leader stg1:2181 (warning, threshold 1)" in c
                  and "action: Check the ZooKeeper nodes and ensemble quorum with the Kafka team." in c for c in cards("ALERT")))
    await zk_step("lifecycle: still 1 down")
    check("no repeat card while open", not cards("ALERT") and len(world.triggers) == 1)
    ZK[("stg3", 2181)] = "down"
    await zk_step("lifecycle: 2 down (leader alive)")
    check("escalates to critical with a card, value '2'", t.severity == "critical" and t.metric_value == "2"
          and any("ZooKeeper: 2 problem(s): down stg2:2181, stg3:2181; leader stg1:2181 (critical, threshold 2) -- ESCALATED from warning" in c for c in cards("ALERT")))
    ZK[("stg3", 2181)] = "follower"
    await zk_step("lifecycle: back to 1 down")
    check("silent de-escalation to warning", t.severity == "warning" and not cards("ALERT"))
    ZK[("stg2", 2181)] = "follower"
    await zk_step("lifecycle: healthy")
    check("resolves with one resolve card", t.resolved_at is not None and len(cards("RESOLVE")) == 1)

    # ---- One query per run for all clusters, one probe per node, no session open while probing ----
    zk_world([zk_rule(1, 4), zk_rule(2, 8), zk_rule(3, 9), zk_rule(4, 3)])
    ens(4, "a", ["leader", "follower", "follower"]); ens(8, "b", ["leader", "follower", "follower"])
    world.zk_urls[9] = "c1:2181, c2 ,c3:2182/kafka"; ZK.update({("c1", 2181): "leader", ("c2", 2181): "follower", ("c3", 2182): "follower"})
    world.zk_urls[3] = "q1:2181"
    await zk_step("4 clusters with rules (3 probed, 1 single-node)")
    q = [(s, p) for s, p in world.sql if "FROM kafka_clusters" in s]
    check("one bound kafka_clusters query for all clusters", len(q) == 1 and q[0][1] == {"cids": [3, 4, 8, 9]} and "IN :cids" in q[0][0])
    check("9 probes, one per node, none for the single-node cluster", len(ZK_CALLS) == 9 and len({(c[0], c[1]) for c in ZK_CALLS}) == 9
          and not any(c[0] == "q1" for c in ZK_CALLS))
    check("port default 2181 and chroot dropped: c2:2181, c3:2182 probed", ("c2", 2181, "mntr", 3, 0) in ZK_CALLS and ("c3", 2182, "mntr", 3, 0) in ZK_CALLS)
    check("no DB session open during any probe", all(c[4] == 0 for c in ZK_CALLS))
    check("no structure-job query", not any("kafka_job_runs" in s for s, _ in world.sql))
    zk_world([zk_rule(1, 4), zk_rule(2, 8)]); world.zk_urls[4] = world.zk_urls[8] = "s1:2181,s2:2181,s3:2181"
    ZK.update({("s1", 2181): "leader", ("s2", 2181): "follower", ("s3", 2181): "down"})
    await zk_step("two clusters sharing one ensemble"); await zk_step("again")
    check("shared nodes probed once per run; both clusters fire", len(ZK_CALLS) == 3 and len(world.triggers) == 2)

    # ---- Mixed with another cluster type; query failure ----
    zk_world([zk_rule(1, 4), rule(2, "cluster.urp_total", 4, {"warning": 1})]); ens(4, "m", ["leader", "down", "follower"])
    world.fresh = {4}; world.urp = {4: 7}
    await zk_step("zk + urp on one cluster, check 1"); await zk_step("check 2")
    check("both types fire independently", len(world.triggers) == 2 and len(cards("ALERT")) == 2)
    zk_world([zk_rule(1, 4)]); ens(4, "zk", ["leader", "down", "down"]); world.fail_query = "FROM kafka_clusters"
    await zk_step("kafka_clusters query fails")
    check("did not raise, no probes", not ZK_CALLS)

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
