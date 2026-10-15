"""teams_alerts._evaluate_cluster_metrics for Schema Registry nodes down and soft-deleted subjects (cluster.sr_*, HTTP faked).

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
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_schema_registry.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_schema_registry.py"""
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
        self.next_id = 500; self.dist = {}; self.snap = []; self.bm = []; self.zk_urls = {}; self.session_open = 0; self.connect_urls = {}; self.conn_snap = []; self.sql_sessions = []; self.sr_urls = {}; self.sr_sql_sessions = []
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
            if "schema_registry_url" in sql:
                world.sr_sql_sessions.append(world.session_open)
                return Res([NS(id=c, schema_registry_url=world.sr_urls[c]) for c in params["cids"] if c in world.sr_urls])
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

import json as _json
# ---- Fake httpx client: GET behaviour from HTTP[url], streamed GET from STREAM[url] ----
HTTP, STREAM, CALLS, INFLIGHT = {}, {}, [], {"now": 0, "max": 0}
class FakeResp:
    def __init__(self, code, chunks=(), headers=None): self.status_code, self._chunks, self.headers = code, chunks, headers or {}
    async def aiter_bytes(self):
        for c in self._chunks:
            STREAMED[0] += len(c); yield c
STREAMED = [0]
class FakeStream:
    def __init__(self, url): self.url = url
    async def __aenter__(self):
        CALLS.append(("STREAM", self.url, world.session_open, CLIENT_TIMEOUT[-1]))
        b = STREAM.get(self.url, "refused")
        if b == "slow": await asyncio.sleep(30)
        if b == "refused": raise ConnectionRefusedError("[Errno 111] Connection refused")
        return b
    async def __aexit__(self, *a): return False
CLIENT_TIMEOUT = [None]
class FakeClient:
    def __init__(self, timeout=None, **kw): self.timeout = timeout; FakeClient.made.append((timeout, kw)); CLIENT_TIMEOUT.append(timeout)
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    def stream(self, method, url): assert method == "GET"; return FakeStream(url)
    async def get(self, url):
        CALLS.append(("GET", url, world.session_open, self.timeout))
        INFLIGHT["now"] += 1; INFLIGHT["max"] = max(INFLIGHT["max"], INFLIGHT["now"])
        try:
            b = HTTP.get(url, "refused")
            if b == "slow": await asyncio.sleep(30)
            if b == "busy": await asyncio.sleep(0.2); return FakeResp(200)
            if b == "refused": raise ConnectionRefusedError("[Errno 111] Connection refused")
            return FakeResp(b)
        finally:
            INFLIGHT["now"] -= 1
FakeClient.made = []
ta.httpx = NS(AsyncClient=FakeClient)

ND, SD = "cluster.sr_nodes_down", "cluster.sr_soft_deleted"
def body(obj, chunk=65536):
    raw = _json.dumps(obj).encode() if not isinstance(obj, bytes) else obj
    return [raw[i:i + chunk] for i in range(0, len(raw), chunk)] or [b""]
def subjects(node, live, deleted_extra, live_code=200, del_code=200):
    STREAM[f"{node}/subjects"] = FakeResp(live_code, body(live))
    STREAM[f"{node}/subjects?deleted=true"] = FakeResp(del_code, body(live + deleted_extra))
def sr_world(configs):
    fresh_world(configs); HTTP.clear(); STREAM.clear(); CALLS.clear(); FakeClient.made.clear(); INFLIGHT.update(now=0, max=0); STREAMED[0] = 0
def log_has(prefix, text): return any(prefix in l and text in l for l in LOGS)
def trig_for(cfg_id): return next((t for t in world.triggers.values() if t.alert_config_id == cfg_id), None)
async def twice(label): await step(label + ", check 1"); await step(label + ", check 2")
FORBIDDEN = ("TOC", "engineering", "Engineering", "applications", "Applications", "escalat")

LIVE = [f"topic-{i:05d}-value" for i in range(21893)]
DELETED = [f"old-{n}-value" for n in ("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
                                       "india", "juliet", "kilo", "lima", "mike", "november", "oscar")]

async def main():
    print("== nodes down: values ==")
    async def ncase(label, url, behaviours, expect_value, expect_text=None):
        sr_world([rule(1, ND, 4, {"info": 1})]); world.sr_urls = {4: url}; HTTP.update(behaviours)
        await twice(label)
        t = trig_for(1); al = cards("ALERT")
        ok = (t is None and not al and ta._threshold_breach_counts.get((1, 4, "cluster")) == 0) if expect_value == 0 else \
             (t is not None and t.metric_value == str(expect_value) and al and expect_text in al[0])
        check(f"{label}: value {expect_value}" + (f", card '{expect_text}'" if expect_text else ""), ok)
        if al: print("    card:", al[0])
        return al
    N3 = "http://sr1:8081,http://sr2:8081,http://sr3:8081"
    up3 = {"http://sr1:8081/": 200, "http://sr2:8081/": 200, "http://sr3:8081/": 200}
    await ncase("all up", N3, up3, 0)
    await ncase("one timeout", N3, {**up3, "http://sr2:8081/": "slow"}, 1, "1 of 3 Schema Registry node(s) down: sr2:8081 (info, threshold 1)")
    await ncase("one HTTP 503", N3, {**up3, "http://sr3:8081/": 503}, 1, "1 of 3 Schema Registry node(s) down: sr3:8081 (info, threshold 1)")
    await ncase("one HTTP 500", N3, {**up3, "http://sr1:8081/": 500}, 1, "down: sr1:8081")
    await ncase("one connection refused", N3, {**up3, "http://sr1:8081/": "refused"}, 1, "down: sr1:8081")
    await ncase("401, 403, 422 count as up", N3, {"http://sr1:8081/": 401, "http://sr2:8081/": 403, "http://sr3:8081/": 422}, 0)
    await ncase("404 and 302 count as up", "http://sr1:8081,http://sr2:8081", {"http://sr1:8081/": 404, "http://sr2:8081/": 302}, 0)
    al = await ncase("all down", N3, {}, 3,
                     "3 of 3 Schema Registry node(s) down: sr1:8081, sr2:8081, sr3:8081; no Schema Registry node reachable from the agent: check the network path first (info, threshold 1)")
    al = await ncase("credentials, no scheme, spaces, trailing slash, duplicate",
                     " user:pw@srx:8081/ , https://admin:s3cret@sry:8443/, srx:8081 ,", {"https://sry:8443/": 500, "http://srx:8081/": 200}, 1,
                     "1 of 2 Schema Registry node(s) down: sry:8443 (info")
    check("  ... probed http://srx:8081/ and https://sry:8443/ (scheme added, credentials dropped), once each per run",
          sorted(c[1] for c in CALLS) == sorted(["http://srx:8081/", "https://sry:8443/"] * 2))
    check("  ... no credentials in any request URL or card", not any("pw" in c[1] or "s3cret" in c[1] or "@" in c[1] for c in CALLS)
          and not any("pw" in a or "s3cret" in a or "user" in a or "admin" in a for a in al))
    await ncase("12 nodes down: names capped at 10", ",".join(f"n{i:02d}:8081" for i in range(1, 13)), {}, 12,
                "12 of 12 Schema Registry node(s) down: " + ", ".join(f"n{i:02d}:8081" for i in range(1, 11)) + " and 2 more; no Schema Registry node reachable")

    print("\n== nodes down: concurrency, timeout, sharing, guards ==")
    sr_world([rule(1, ND, 4, {"info": 1})]); world.sr_urls = {4: ",".join(f"http://b{i:02d}:8081" for i in range(25))}
    HTTP.update({f"http://b{i:02d}:8081/": "busy" for i in range(25)})
    await step("25 nodes, 0.2 s each")
    check(f"concurrency cap: max in flight = {INFLIGHT['max']} (10)", INFLIGHT["max"] == 10 and len(CALLS) == 25)
    check("probe client timeout=3, no credentials passed", [m for m in FakeClient.made] == [(3, {})])
    sr_world([rule(1, ND, 4, {"info": 1})]); world.sr_urls = {4: ",".join(f"http://s{i:02d}:8081" for i in range(12))}
    HTTP.update({f"http://s{i:02d}:8081/": "slow" for i in range(12)})
    t0 = time.monotonic(); await step("12 slow nodes"); dt = time.monotonic() - t0
    check(f"each probe bounded by 3 s, semaphore wait not counted: {dt:.1f}s (~6), all down", 5.8 <= dt < 7.0 and ta._threshold_breach_counts[(1, 4, "cluster")] == 1)
    sr_world([rule(1, ND, 4, {"info": 1}), rule(2, ND, 8, {"info": 1})])
    world.sr_urls = {4: "http://shared:8081,http://only4:8081", 8: "shared:8081/"}
    HTTP.update({"http://shared:8081/": 500, "http://only4:8081/": 200})
    await twice("two clusters sharing a node")
    check("shared node probed once per run, both fire", len(CALLS) == 4 and sum(c[1] == "http://shared:8081/" for c in CALLS) == 2 and trig_for(1) and trig_for(2))
    check("no DB session open during probes; URL read in the read session", all(c[2] == 0 for c in CALLS) and world.sr_sql_sessions == [1, 1])
    for label, url, expect in [("empty URL", "", "4: schema_registry_url is empty"), ("NULL URL", None, "4: schema_registry_url is empty"),
                               ("commas only", " , ", "4: schema_registry_url is empty"), ("cluster row missing", "MISSING", "4: cluster not found in kafka_clusters")]:
        sr_world([rule(1, ND, 4, {"info": 1}), rule(2, SD, 4, {"info": 1})])
        if url != "MISSING": world.sr_urls[4] = url
        for i, cfg in ((91, 1), (92, 2)):
            world.triggers[i] = NS(id=i, alert_config_id=cfg, cluster_id=4, subject="cluster", severity="warning", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="1")
            ta._threshold_breach_counts[(cfg, 4, "cluster")] = 1
        await twice(label)
        check(f"{label}: both types skipped, no request, no resolve, counters unchanged, both INFO lines '{expect}'",
              not CALLS and not cards("RESOLVE") and world.triggers[91].resolved_at is None and world.triggers[92].resolved_at is None
              and ta._threshold_breach_counts[(1, 4, "cluster")] == 1 and ta._threshold_breach_counts[(2, 4, "cluster")] == 1
              and log_has("Schema Registry nodes ignored", expect) and log_has("Schema Registry soft-deleted subjects ignored", expect))

    print("\n== soft-deleted: values ==")
    sr_world([rule(2, SD, 9, {"warning": 1})]); world.sr_urls = {9: "http://sr9a:8081,http://sr9b:8081"}
    HTTP.update({"http://sr9a:8081/": "refused", "http://sr9b:8081/": 200}); subjects("http://sr9b:8081", LIVE, DELETED)
    await twice("21,893 live, 21,908 with deleted")
    al = cards("ALERT"); t = trig_for(2)
    print("    card:", al[0] if al else None)
    check("value 15; card names 5 sorted and 'and 10 more'", t is not None and t.metric_value == "15" and al and
          "15 soft-deleted subject(s) in Schema Registry: old-alpha-value, old-bravo-value, old-charlie-value, old-delta-value, old-echo-value and 10 more (warning, threshold 1)" in al[0])
    check("action 'Check the soft-deleted subjects in Schema Registry.'", "action: Check the soft-deleted subjects in Schema Registry." in al[0])
    st = [c for c in CALLS if c[0] == "STREAM"]
    check("subjects read from the first answering node (sr9b), both lists, client timeout 10, no credentials",
          sorted(c[1] for c in st) == sorted(["http://sr9b:8081/subjects", "http://sr9b:8081/subjects?deleted=true"] * 2)
          and all(c[3] == 10 for c in st) and all(kw == {} for _, kw in FakeClient.made))
    check("probe run although no nodes-down rule exists", sum(c[0] == "GET" for c in CALLS) == 4)
    check("no DB session open during subject requests", all(c[2] == 0 for c in st))

    async def scase(label, setup, expect_value=None, expect_log=None):
        sr_world([rule(2, SD, 4, {"info": 1})]); world.sr_urls = {4: "http://sr4:8081"}; HTTP["http://sr4:8081/"] = 200; setup()
        world.triggers[93] = NS(id=93, alert_config_id=2, cluster_id=4, subject="cluster", severity="info", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="3")
        ta._threshold_breach_counts[(2, 4, "cluster")] = 1
        await step(label)
        if expect_log:
            check(f"{label}: ignored (no fire/resolve, counter unchanged), INFO '{expect_log}'",
                  not cards("ALERT") and not cards("RESOLVE") and world.triggers[93].resolved_at is None
                  and ta._threshold_breach_counts[(2, 4, "cluster")] == 1 and log_has("Schema Registry soft-deleted subjects ignored", expect_log))
        else:
            check(f"{label}: value {expect_value}, open trigger resolved with a card",
                  world.triggers[93].resolved_at is not None and len(cards("RESOLVE")) == 1 and ta._threshold_breach_counts[(2, 4, "cluster")] == 0)
    await scase("equal lists", lambda: subjects("http://sr4:8081", LIVE[:100], []), 0)
    await scase("deleted list shorter than live (floor 0)", lambda: (STREAM.update({"http://sr4:8081/subjects": FakeResp(200, body(LIVE[:100])),
                "http://sr4:8081/subjects?deleted=true": FakeResp(200, body(LIVE[:90]))})), 0)
    await scase("422 on /subjects (credentials needed)", lambda: subjects("http://sr4:8081", [], [], live_code=422), expect_log="4: GET /subjects on sr4:8081 returned HTTP 422")
    await scase("401 on ?deleted=true", lambda: subjects("http://sr4:8081", LIVE[:5], [], del_code=401), expect_log="4: GET /subjects?deleted=true on sr4:8081 returned HTTP 401")
    await scase("500 on /subjects", lambda: subjects("http://sr4:8081", [], [], live_code=500), expect_log="returned HTTP 500")
    await scase("non-list JSON", lambda: STREAM.update({"http://sr4:8081/subjects": FakeResp(200, body({"error_code": 40101}))}),
                expect_log="4: GET /subjects on sr4:8081 did not return a JSON list of subjects")
    await scase("list of non-strings", lambda: STREAM.update({"http://sr4:8081/subjects": FakeResp(200, body([1, 2]))}), expect_log="did not return a JSON list of subjects")
    await scase("not JSON", lambda: STREAM.update({"http://sr4:8081/subjects": FakeResp(200, [b"<html>login</html>"])}),
                expect_log="4: GET /subjects on sr4:8081 did not return JSON")
    await scase("subjects request refused", lambda: None, expect_log="4: GET /subjects on sr4:8081 failed (ConnectionRefusedError)")
    big = [b"[" + b'"x",' * 262144] * 21   # 21 chunks of ~1 MB, no Content-Length
    await scase("oversized body (streamed, no Content-Length)", lambda: STREAM.update({"http://sr4:8081/subjects": FakeResp(200, big)}),
                expect_log="4: GET /subjects on sr4:8081 response larger than 20 MB")
    check(f"  ... stopped reading just past 20 MB ({STREAMED[0]} bytes read)", 20 * 1024 * 1024 < STREAMED[0] <= 21 * 1024 * 1024 + 1024)
    await scase("oversized Content-Length header", lambda: STREAM.update({"http://sr4:8081/subjects": FakeResp(200, big, {"content-length": str(30 * 1024 * 1024)})}),
                expect_log="response larger than 20 MB")
    check("  ... nothing read", STREAMED[0] == 0)
    sr_world([rule(2, SD, 4, {"info": 1})]); world.sr_urls = {4: "http://sr4:8081"}; HTTP["http://sr4:8081/"] = 200
    STREAM.update({"http://sr4:8081/subjects": "slow"})
    t0 = time.monotonic(); await step("subjects request hangs"); dt = time.monotonic() - t0
    check(f"bounded by 10 s ({dt:.1f}s), ignored with INFO", 9.9 <= dt < 11 and log_has("soft-deleted subjects ignored", "4: GET /subjects on sr4:8081 no answer within 10 s"))
    sr_world([rule(2, SD, 4, {"info": 1})]); world.sr_urls = {4: "http://sr4a:8081,http://sr4b:8081"}
    await step("no node answers")
    check("no node answered: ignored, no subject request, INFO", not [c for c in CALLS if c[0] == "STREAM"]
          and log_has("soft-deleted subjects ignored", "4: no Schema Registry node answered"))

    print("\n== lifecycle: fire on 2nd check, escalate, resolve card (both types) ==")
    sr_world([rule(1, ND, 8, {"warning": 1, "critical": 2}), rule(2, SD, 8, {"warning": 10, "critical": 20})])
    world.sr_urls = {8: N3}; HTTP.update({**up3, "http://sr2:8081/": 503}); subjects("http://sr1:8081", LIVE[:200], DELETED)
    await step("check 1"); check("no card after 1 check", not cards("ALERT") and not world.triggers)
    await step("check 2"); n, s = trig_for(1), trig_for(2)
    check("both fire on the 2nd check at warning", n and s and n.severity == s.severity == "warning" and n.metric_value == "1" and s.metric_value == "15" and len(cards("ALERT")) == 2)
    all_cards = list(cards("ALERT"))
    await step("still the same"); check("no repeat cards", not cards("ALERT"))
    HTTP["http://sr3:8081/"] = "slow"; subjects("http://sr1:8081", LIVE[:200], DELETED + [f"more-{i}" for i in range(10)])
    await step("2 nodes down, 25 soft-deleted")
    all_cards += cards("ALERT")
    check("both escalate to critical with cards", n.severity == s.severity == "critical" and len(cards("ALERT")) == 2
          and any("2 of 3 Schema Registry node(s) down: sr2:8081, sr3:8081 (critical, threshold 2) -- ESCALATED from warning" in a and "action: Check the Schema Registry nodes." in a for a in cards("ALERT"))
          and any("25 soft-deleted subject(s)" in a and "ESCALATED from warning" in a for a in cards("ALERT")))
    HTTP.update(up3); subjects("http://sr1:8081", LIVE[:200], [])
    await step("all clear")
    check("both resolve with resolve cards", n.resolved_at and s.resolved_at and len(cards("RESOLVE")) == 2)
    for a in all_cards: print("    card:", a)
    check("card texts contain no TOC / engineering / applications / escalation wording (other than the standard ESCALATED marker)",
          all(not any(w in a.replace("-- ESCALATED from", "") for w in FORBIDDEN) for a in all_cards))

    print("\n== both SR types + the three Connect types on one cluster in one run ==")
    sr_world([rule(1, ND, 10, {"warning": 1}), rule(2, SD, 10, {"warning": 1}),
              rule(3, "cluster.connect_failed_connectors", 10, {"warning": 1}), rule(4, "cluster.connect_failed_tasks", 10, {"warning": 1}),
              rule(5, "cluster.connect_workers_down", 10, {"warning": 1})])
    world.sr_urls = {10: "sr10a:8081,sr10b:8081"}; HTTP.update({"http://sr10a:8081/": 500, "http://sr10b:8081/": 401})
    subjects("http://sr10b:8081", LIVE[:50], DELETED[:3])
    world.connect_urls = {10: "http://cw1:8083,http://cw2:8083"}; HTTP.update({"http://cw1:8083/": 200, "http://cw2:8083/": "refused"})
    t = NOW() - timedelta(minutes=1)
    for nme, st_, ft in [("dbz-a", "RUNNING", 3), ("dbz-b", "RUNNING", 2), ("es", "RUNNING", 1), ("dead", "FAILED", 0)]: world.conn_snap.append((10, t, nme, st_, ft))
    await twice("cluster 10: five types")
    check("all five fire independently", len(world.triggers) == 5 and len(cards("ALERT")) == 5)
    check("values: sr nodes 1, soft-deleted 3, failed connectors 1, failed tasks 6, workers 1",
          [trig_for(i).metric_value for i in range(1, 6)] == ["1", "3", "1", "6", "1"])
    check("401 node used for subjects (answered the probe)", any(c[1] == "http://sr10b:8081/subjects" for c in CALLS))
    check("no structure-job query", not any("kafka_job_runs" in s_ for s_, _ in world.sql))

    print("\n== URL query failure ==")
    sr_world([rule(1, ND, 4, {"info": 1})]); world.sr_urls = {4: "http://sr4:8081"}; world.fail_query = "schema_registry_url"
    await step("schema_registry_url query fails")
    check("did not raise, no requests, no cards", not CALLS and not cards("ALERT"))

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
