"""teams_alerts._evaluate_cluster_metrics for Kafka Connect failed connectors, failed tasks and workers down (cluster.connect_*, HTTP faked).

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
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_connect.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_connect.py"""
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
        self.next_id = 500; self.dist = {}; self.snap = []; self.bm = []; self.zk_urls = {}; self.session_open = 0; self.connect_urls = {}; self.conn_snap = []; self.sql_sessions = []
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
            if "kafka_connect_url" in sql:
                world.sql_sessions.append(world.session_open)
                return Res([NS(id=c, kafka_connect_url=world.connect_urls[c]) for c in params["cids"] if c in world.connect_urls])
            if "kafka_connector_snapshots" in sql:
                assert "IN :cids" in sql and "collected_at >= :since" in sql and "ROW_NUMBER()" in sql
                out = []
                for c in params["cids"]:
                    rows = [x for x in world.conn_snap if x[0] == c and x[1] >= params["since"]]
                    runs = sorted({x[1] for x in rows}, reverse=True)
                    if not runs: continue
                    prev = sum(1 for x in rows if x[1] == runs[1]) if len(runs) > 1 else None
                    latest = [x for x in rows if x[1] == runs[0]]
                    out += [NS(cluster_id=c, collected_at=x[1], connector_name=x[2], state=x[3], failed_tasks=x[4],
                               run_rows=len(latest), previous_rows=prev) for x in latest]
                return Res(out)
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
async def names(): return {4: "prod-4", 8: "prod-8", 10: "prod-10"}
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
    cond = bool(cond); RESULTS.append(cond); print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
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

# ---- Fake httpx client: worker behaviour from WK[url] ----
WK, CALLS, INFLIGHT = {}, [], {"now": 0, "max": 0}
class FakeResp:
    def __init__(self, code): self.status_code = code
class FakeClient:
    def __init__(self, timeout=None, **kw): self.timeout = timeout; FakeClient.made.append(timeout)
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def get(self, url):
        CALLS.append((url, world.session_open))
        INFLIGHT["now"] += 1; INFLIGHT["max"] = max(INFLIGHT["max"], INFLIGHT["now"])
        try:
            b = WK.get(url[:-1], "refused")
            if b == "slow": await asyncio.sleep(30)
            if b == "busy": await asyncio.sleep(0.2); return FakeResp(200)
            if b == "refused": raise ConnectionRefusedError("[Errno 111] Connection refused")
            return FakeResp(b)
        finally:
            INFLIGHT["now"] -= 1
FakeClient.made = []
ta.httpx = NS(AsyncClient=FakeClient)

FC, FT, WD = "cluster.connect_failed_connectors", "cluster.connect_failed_tasks", "cluster.connect_workers_down"
def snap(cl, age_min, conns):
    """conns: list of (name, state, failed_tasks), all with one collected_at."""
    t = NOW() - timedelta(minutes=age_min)
    for n, st, ft in conns: world.conn_snap.append((cl, t, n, st, ft))
def cw_world(configs):
    fresh_world(configs); WK.clear(); CALLS.clear(); FakeClient.made.clear(); INFLIGHT.update(now=0, max=0)
def log_has(prefix, text): return any(prefix in l and text in l for l in LOGS)
def trig_for(cfg_id): return next((t for t in world.triggers.values() if t.alert_config_id == cfg_id), None)
async def twice(label): await step(label + ", check 1"); await step(label + ", check 2")

# cluster 8 style: 1 FAILED connector with 0 failed tasks; cluster 10 style: 0 FAILED, 6 failed tasks across RUNNING
C8 = [("jdbc-sink-orders", "FAILED", 0), ("s3-sink-a", "RUNNING", 0), ("s3-sink-b", "RUNNING", 0)]
C10 = [("debezium-a", "RUNNING", 3), ("debezium-b", "RUNNING", 2), ("es-sink", "RUNNING", 1), ("ok-sink", "RUNNING", 0)]

async def main():
    print("== failed connectors / failed tasks from sample snapshots ==")
    cw_world([rule(1, FC, 8, {"warning": 1}), rule(2, FT, 8, {"warning": 1}),
              rule(3, FC, 10, {"warning": 1}), rule(4, FT, 10, {"warning": 1})])
    world.connect_urls = {8: "http://c8w1:8083", 10: "http://c10w1:8083"}
    snap(8, 7, C8); snap(10, 7, C10)   # previous runs (same size)
    snap(8, 1, C8); snap(10, 1, C10)
    await twice("cluster 8 + cluster 10")
    al = cards("ALERT"); [print("    card:", a) for a in al]
    check("cluster 8 failed connectors = 1, card names jdbc-sink-orders",
          trig_for(1) and trig_for(1).metric_value == "1" and any("1 Connect connector(s) FAILED: jdbc-sink-orders (warning, threshold 1)" in a for a in al))
    check("cluster 8 failed tasks = 0 -> no trigger", trig_for(2) is None)
    check("cluster 10 failed connectors = 0 -> no trigger", trig_for(3) is None)
    check("cluster 10 failed tasks = 6, card lists connectors by count",
          trig_for(4) and trig_for(4).metric_value == "6"
          and any("6 failed Connect task(s) across 3 connector(s): debezium-a (3), debezium-b (2), es-sink (1) (warning, threshold 1)" in a for a in al))
    check("actions on cards", any("action: Check the failed connectors and their logs with the Kafka team." in a for a in al)
          and any("action: Check the failed tasks (restart or fix the connector) with the Kafka team." in a for a in al))
    q = [(s, p) for s, p in world.sql if "kafka_connector_snapshots" in s]
    check("one bound snapshot query per run for both clusters", len(q) == 1 and q[0][1]["cids"] == [8, 10])
    check("no structure-job query", not any("kafka_job_runs" in s for s, _ in world.sql))

    print("\n== names capped at 5 ==")
    cw_world([rule(1, FC, 8, {"info": 1}), rule(2, FT, 8, {"info": 1})]); world.connect_urls = {8: "http://w:8083"}
    snap(8, 1, [(f"conn-{i:02d}", "FAILED", 1) for i in range(1, 9)])
    await twice("8 failed connectors")
    al = cards("ALERT")
    check("failed connectors: 5 names then 'and 3 more'",
          any("8 Connect connector(s) FAILED: conn-01, conn-02, conn-03, conn-04, conn-05 and 3 more (info" in a for a in al))
    check("failed tasks: 5 names then 'and 3 more'",
          any("8 failed Connect task(s) across 8 connector(s): conn-01 (1), conn-02 (1), conn-03 (1), conn-04 (1), conn-05 (1) and 3 more" in a for a in al))

    print("\n== snapshot guards: stale, no rows, empty URL, partial ==")
    for label, setup, expect in [
        ("stale snapshot (7 min)", lambda: snap(8, 7, C8), "8: latest connector snapshot is 7 min old (max 6)"),
        ("no snapshot rows", lambda: None, "8: no connector snapshot rows in the last 15 min"),
        ("empty URL", lambda: (world.connect_urls.update({8: ""}), snap(8, 1, C8)), "8: kafka_connect_url is empty"),
        ("NULL URL", lambda: (world.connect_urls.update({8: None}), snap(8, 1, C8)), "8: kafka_connect_url is empty"),
        ("cluster row missing", lambda: (world.connect_urls.pop(8), snap(8, 1, C8)), "8: cluster not found in kafka_clusters"),
        ("partial snapshot (1 row vs 3)", lambda: (snap(8, 3, C8), snap(8, 1, C8[:1])),
         "8: latest connector snapshot looks partial: 1 rows vs 3 in the previous one"),
    ]:
        cw_world([rule(1, FC, 8, {"warning": 1})]); world.connect_urls = {8: "http://w:8083"}; setup()
        world.triggers[91] = NS(id=91, alert_config_id=1, cluster_id=8, subject="cluster", severity="warning", triggered_at=NOW(),
                                resolved_at=None, teams_post_success=True, metric_value="1")
        ta._threshold_breach_counts[(1, 8, "cluster")] = 1
        await twice(label)
        check(f"{label}: ignored (no fire/resolve, counter unchanged), INFO: '{expect}'",
              not cards("ALERT") and not cards("RESOLVE") and world.triggers[91].resolved_at is None
              and ta._threshold_breach_counts[(1, 8, "cluster")] == 1 and log_has("Connect connector snapshot ignored", expect))
    cw_world([rule(1, FC, 8, {"warning": 1})]); world.connect_urls = {8: "http://w:8083"}
    snap(8, 3, C8 + [("x", "RUNNING", 0)]); snap(8, 1, C8[:2])   # 2 vs 4 = exactly half -> not partial
    await twice("exactly half the previous rows")
    check("exactly half is accepted (rule is 'less than half') and fires", trig_for(1) is not None)
    cw_world([rule(1, FC, 8, {"warning": 1})]); world.connect_urls = {8: "http://w:8083"}
    snap(8, 20, C8 * 4); snap(8, 1, C8)   # previous run outside the 15-min window
    await twice("previous run outside window")
    check("no previous run in window -> no partial check, evaluated", trig_for(1) is not None)

    print("\n== workers down ==")
    async def wcase(label, url, behaviours, expect_value, expect_text=None, tiers={"info": 1}):
        cw_world([rule(5, WD, 4, tiers)]); world.connect_urls = {4: url}; WK.update(behaviours)
        await twice(label)
        t = trig_for(5); al = cards("ALERT")
        if expect_value == 0:
            ok = t is None and not al and ta._threshold_breach_counts.get((5, 4, "cluster")) == 0
        else:
            ok = t is not None and t.metric_value == str(expect_value) and al and expect_text in al[0]
        check(f"{label}: value {expect_value}" + (f", card '{expect_text}'" if expect_text else ""), ok)
        if al: print("    card:", al[0])
    up3 = {"http://w1:8083": 200, "http://w2:8083": 200, "http://w3:8083": 200}
    await wcase("all up", "http://w1:8083,http://w2:8083,http://w3:8083", up3, 0)
    await wcase("one timeout", "http://w1:8083,http://w2:8083,http://w3:8083", {**up3, "http://w2:8083": "slow"}, 1,
                "1 of 3 Connect worker(s) down: w2:8083 (info, threshold 1)")
    await wcase("one HTTP 500", "http://w1:8083,http://w2:8083,http://w3:8083", {**up3, "http://w3:8083": 500}, 1,
                "1 of 3 Connect worker(s) down: w3:8083 (info, threshold 1)")
    await wcase("one connection refused", "http://w1:8083,http://w2:8083,http://w3:8083", {**up3, "http://w1:8083": "refused"}, 1,
                "1 of 3 Connect worker(s) down: w1:8083 (info")
    await wcase("one 302 redirect", "http://w1:8083,http://w2:8083", {"http://w1:8083": 302, "http://w2:8083": 200}, 1, "down: w1:8083")
    await wcase("all down", "http://w1:8083,http://w2:8083,http://w3:8083", {}, 3,
                "3 of 3 Connect worker(s) down: w1:8083, w2:8083, w3:8083; no worker reachable from the agent: check the network path first (info, threshold 1)")
    await wcase("spaces, trailing slash, duplicate, https, credentials", " http://w1:8083/ , https://user:pw@w2:8443/,http://w1:8083 ,",
                {"http://w1:8083": 200, "https://user:pw@w2:8443": 500}, 1, "1 of 2 Connect worker(s) down: w2:8443 (info")
    check("  ... probed exactly GET http://w1:8083/ and https://user:pw@w2:8443/, once each per run",
          sorted(c[0] for c in CALLS) == sorted(["http://w1:8083/", "https://user:pw@w2:8443/"] * 2))
    check("  ... no credentials on the card", not any("pw" in a for a in cards("ALERT")))
    await wcase("12 workers down: names capped at 10", ",".join(f"http://n{i:02d}:8083" for i in range(1, 13)), {}, 12,
                "12 of 12 Connect worker(s) down: " + ", ".join(f"n{i:02d}:8083" for i in range(1, 11)) + " and 2 more; no worker reachable")
    # A worker with no connectors: the probe never looks at connectors; GET / = 200 -> up.
    cw_world([rule(5, WD, 4, {"info": 1})]); world.connect_urls = {4: "http://idle:8083,http://busy:8083"}
    WK.update({"http://idle:8083": 200, "http://busy:8083": 200}); snap(4, 1, [("only-on-busy", "RUNNING", 0)])
    await twice("idle worker with no connectors")
    check("worker with no connectors counts as up (value 0)", trig_for(5) is None and ta._threshold_breach_counts[(5, 4, "cluster")] == 0)

    print("\n== workers: timeout, concurrency, sharing, sessions ==")
    cw_world([rule(5, WD, 4, {"info": 1})]); world.connect_urls = {4: ",".join(f"http://b{i:02d}:8083" for i in range(25))}
    WK.update({f"http://b{i:02d}:8083": "busy" for i in range(25)})
    await step("25 workers, 0.2 s each")
    check(f"concurrency cap: max in flight = {INFLIGHT['max']} (<= 10, and reached 10)", INFLIGHT["max"] == 10)
    check("25 probes, all up", len(CALLS) == 25 and ta._threshold_breach_counts[(5, 4, "cluster")] == 0)
    check("client created with timeout=3, once per run", FakeClient.made == [3])
    cw_world([rule(5, WD, 4, {"info": 1})]); world.connect_urls = {4: ",".join(f"http://s{i:02d}:8083" for i in range(12))}
    WK.update({f"http://s{i:02d}:8083": "slow" for i in range(12)})
    t0 = time.monotonic(); await step("12 slow workers (two waves of 10 + 2)"); dt = time.monotonic() - t0
    check(f"each probe bounded by 3 s, semaphore wait not counted: run took {dt:.1f}s (~6), all 12 down",
          5.8 <= dt < 7.0 and ta._threshold_breach_counts[(5, 4, "cluster")] == 1 and INFLIGHT["max"] == 10)
    cw_world([rule(5, WD, 4, {"info": 1}), rule(6, WD, 8, {"info": 1})])
    world.connect_urls = {4: "http://shared:8083,http://only4:8083", 8: "http://shared:8083/"}
    WK.update({"http://shared:8083": 500, "http://only4:8083": 200})
    await twice("two clusters sharing a worker")
    check("shared worker probed once per run (2 probes per run), both clusters fire",
          len(CALLS) == 4 and sum(c[0] == "http://shared:8083/" for c in CALLS) == 2 and trig_for(5) and trig_for(6))
    check("no DB session open during any probe", all(c[1] == 0 for c in CALLS))
    check("kafka_clusters URL read inside the read session", world.sql_sessions and all(s == 1 for s in world.sql_sessions))
    for label, url, expect in [("empty URL", "", "4: kafka_connect_url is empty"), ("commas only", " , ", "4: kafka_connect_url is empty"),
                               ("cluster row missing", "MISSING", "4: cluster not found in kafka_clusters")]:
        cw_world([rule(5, WD, 4, {"info": 1})])
        if url != "MISSING": world.connect_urls[4] = url
        world.triggers[92] = NS(id=92, alert_config_id=5, cluster_id=4, subject="cluster", severity="warning", triggered_at=NOW(),
                                resolved_at=None, teams_post_success=True, metric_value="1")
        ta._threshold_breach_counts[(5, 4, "cluster")] = 1
        await twice(label)
        check(f"workers {label}: skipped, no probe, no resolve, counter unchanged, INFO '{expect}'",
              not CALLS and not cards("RESOLVE") and world.triggers[92].resolved_at is None
              and ta._threshold_breach_counts[(5, 4, "cluster")] == 1 and log_has("Connect workers ignored", expect))

    print("\n== lifecycle (workers): fire on 2nd check, escalate, resolve card ==")
    cw_world([rule(5, WD, 4, {"warning": 1, "critical": 2})]); world.connect_urls = {4: "http://w1:8083,http://w2:8083,http://w3:8083"}
    WK.update({**up3, "http://w2:8083": 500})
    await step("1 down, check 1"); check("no card after 1 check", not cards("ALERT") and not world.triggers)
    await step("1 down, check 2"); t = trig_for(5)
    check("fires on 2nd check: warning, value 1", t and t.severity == "warning" and t.metric_value == "1" and len(cards("ALERT")) == 1)
    await step("still 1 down"); check("no repeat card", not cards("ALERT"))
    WK["http://w3:8083"] = "slow"; await step("2 down")
    check("escalates to critical with a card", t.severity == "critical" and t.metric_value == "2"
          and any("2 of 3 Connect worker(s) down: w2:8083, w3:8083 (critical, threshold 2) -- ESCALATED from warning" in a
                  and "action: Check the Connect worker nodes with the Kafka team." in a for a in cards("ALERT")))
    WK.update(up3); await step("all up")
    check("resolves with one resolve card", t.resolved_at is not None and len(cards("RESOLVE")) == 1)

    print("\n== lifecycle (snapshot): resolve when the failure clears ==")
    cw_world([rule(1, FC, 8, {"warning": 1})]); world.connect_urls = {8: "http://w:8083"}; snap(8, 1, C8)
    await twice("failed connector"); t = trig_for(1)
    world.conn_snap.clear(); snap(8, 3, C8); snap(8, 1, [(n, "RUNNING", 0) for n, _, _ in C8])
    await step("connector restarted")
    check("fired then resolved with a resolve card", t and t.resolved_at is not None and len(cards("RESOLVE")) == 1)

    print("\n== three types + an older type on one cluster in one run ==")
    cw_world([rule(1, FC, 10, {"warning": 1}), rule(2, FT, 10, {"warning": 5}), rule(3, WD, 10, {"warning": 1}),
              rule(4, "cluster.urp_total", 10, {"warning": 1})])
    world.connect_urls = {10: "http://x1:8083,http://x2:8083"}; WK.update({"http://x1:8083": 200, "http://x2:8083": "refused"})
    snap(10, 1, C10 + [("dead-conn", "FAILED", 0)]); world.fresh = {10}; world.urp = {10: 4}
    await twice("cluster 10: all four types")
    check("all four fire independently on one cluster", len(world.triggers) == 4 and len(cards("ALERT")) == 4)
    check("values: failed connectors 1, failed tasks 6, workers down 1, urp 4",
          [trig_for(i).metric_value for i in (1, 2, 3, 4)] == ["1", "6", "1", "4"])
    check("structure-job query only for the URP rule's cluster",
          [p["job_ids"] for s, p in world.sql if "kafka_job_runs" in s] == [["kafka-topic-structure-10"]])
    check("one kafka_clusters URL query per run",
          sum("kafka_connect_url" in s for s, _ in world.sql) == 1)

    print("\n== query failure ==")
    cw_world([rule(1, FC, 8, {"warning": 1}), rule(5, WD, 8, {"warning": 1})]); world.connect_urls = {8: "http://w:8083"}
    world.fail_query = "kafka_connector_snapshots"
    await step("snapshot query fails")
    check("did not raise, no probes, no cards", not CALLS and not cards("ALERT"))

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
