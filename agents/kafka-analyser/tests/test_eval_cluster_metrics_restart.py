"""teams_alerts._evaluate_cluster_metrics for broker restarts (cluster.broker_restarted: rows of kafka_broker_restarts within the window).

Stub test: the real teams_alerts runs against an in-memory fake DB session
(database.SessionLocal), fake routes_settings, and fake Teams card builders
and sender; no database, network or Teams is touched. Prints one [PASS]/[FAIL]
line per check and "N/N checks passed"; exits 1 if any check fails.
Outbox recording is switched off here (outbox.enabled -> False); the outbox
path of this type is covered by tests/test_rule_notice_golden.py and
tests/test_outbox_cluster_metrics.py. The real-rows version is
tests/test_broker_restart_alert_db.py.
Run from the repo root with Python 3.10+ and the agent's requirements
(sqlalchemy), e.g. inside the agent image:
  docker run --rm -v $PWD/agents/kafka-analyser:/app:ro -v $PWD/shared:/opt/shared:ro \
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_eval_cluster_metrics_restart.py
or locally: python -B agents/kafka-analyser/tests/test_eval_cluster_metrics_restart.py"""
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
        self.restarts, self.fail_restarts, self.restart_queries = [], False, []
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
            if "FROM kafka_broker_restarts" in sql:
                world.restart_queries.append(params)
                if world.fail_restarts: raise RuntimeError('relation "kafka_broker_restarts" does not exist')
                return Res([r for r in world.restarts if r.cluster_id in params["cids"] and r.detected_at >= params["since"]])
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
    RESULTS.append(bool(cond)); print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
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

T = {"info": 1, "warning": 3, "critical": 5}
RT = "cluster.broker_restarted"
def r_rule(id, cl, tiers=T, **kw): return rule(id, RT, cl, tiers, **kw)
def healthy(cl, age_min=1): brokers(cl, [10.0, 10.0, 10.0], age_min=age_min)
def restart(cl, broker, minutes_ago, uptime_min=13 * 1440 + 125):
    detected = NOW() - timedelta(minutes=minutes_ago); started = detected - timedelta(seconds=45)
    ev = NS(cluster_id=cl, broker_id=broker, started_at=started, previous_started_at=started - timedelta(minutes=uptime_min), detected_at=detected)
    world.restarts.append(ev); return ev
def r_world(configs, window=None):
    fresh_world(configs)
    rs._config.pop("broker_restart_window_minutes", None)
    if window is not None: rs._config["broker_restart_window_minutes"] = window
def trig(): return next(iter(world.triggers.values()), None)
def when(ev): return ev.started_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
def ignored_log(): return [l for l in LOGS if "broker restarts ignored" in l]
ACTION = "action: Check with the Kafka team why the broker restarted and confirm it is back in sync."

async def main():
    # ---- Uptime text ----
    check("uptime: 13 d 2 h", ta._uptime_text(NOW() - timedelta(minutes=13 * 1440 + 125), NOW()) == "13 d 2 h")
    check("uptime: 5 h 12 min", ta._uptime_text(NOW() - timedelta(minutes=312), NOW()) == "5 h 12 min")
    check("uptime: 7 min", ta._uptime_text(NOW() - timedelta(minutes=7), NOW()) == "7 min")
    check("uptime: unknown or negative is None", ta._uptime_text(None, NOW()) is None and ta._uptime_text(NOW() + timedelta(hours=1), NOW()) is None)
    check("window setting: default, in range, out of range, bool, string",
          [ta._broker_restart_window_minutes(v) for v in (None, 15, 240, 14, 241, True, "60", 45)] == [30, 15, 240, 30, 30, 30, 30, 45])

    # ---- Below the lowest tier: no restarts in the window, no fire ----
    r_world([r_rule(1, 4)]); healthy(4); restart(4, 2, 45)   # an old restart, outside the 30 min window
    await step("0 restarts in the window, check 1"); await step("check 2")
    check("below the lowest tier: no trigger, no card", trig() is None and not cards("ALERT"))
    check("restart read bound to the cluster and the window (detected_at >= :since)",
          len(world.restart_queries) == 2 and all(q["cids"] == [4] for q in world.restart_queries)
          and abs((NOW() - world.restart_queries[0]["since"]).total_seconds() - 1800) < 5
          and any("detected_at >= :since" in s for s, _ in world.sql))

    # ---- At the tier: fires on the 2nd check with the description and action ----
    r_world([r_rule(1, 4)]); healthy(4); e1 = restart(4, 2, 10)
    await step("1 restart, check 1")
    check("no card after 1 check", not cards("ALERT") and not world.triggers)
    await step("1 restart, check 2")
    t = trig()
    want = f"Broker 2 restarted at {when(e1)} UTC after 13 d 2 h. 1 broker restart in the last 30 min (info, threshold 1)"
    check("fires on the 2nd check: info, value '1', description + action + subject cluster",
          t is not None and t.severity == "info" and t.metric_value == "1" and t.subject == "cluster"
          and any(want in c and ACTION in c for c in cards("ALERT")))
    print("    card:", cards("ALERT")[0] if cards("ALERT") else None)
    await step("still 1"); check("no repeat card while open", not cards("ALERT") and len(world.triggers) == 1)

    # ---- Escalate 1 -> 3 -> 5, de-escalate silently ----
    restart(4, 3, 8); e3 = restart(4, 1, 5)
    await step("3 restarts")
    check("escalates to warning with the most recent restart named, value '3'",
          t.severity == "warning" and t.metric_value == "3"
          and any(f"Broker 1 restarted at {when(e3)} UTC after 13 d 2 h. 3 broker restarts in the last 30 min (warning, threshold 3) -- ESCALATED from info" in c
                  for c in cards("ALERT")))
    restart(4, 2, 4); e5 = restart(4, 3, 1)
    await step("5 restarts")
    check("escalates to critical, value '5'", t.severity == "critical" and t.metric_value == "5"
          and any(f"Broker 3 restarted at {when(e5)} UTC" in c and "(critical, threshold 5) -- ESCALATED from warning" in c for c in cards("ALERT")))
    world.restarts = [r for r in world.restarts if r.detected_at > NOW() - timedelta(minutes=9)]   # the 10 min old one goes: 4 left
    await step("4 restarts left")
    check("silent de-escalation to warning when the count drops to 4", len(world.restarts) == 4 and not cards("ALERT") and t.severity == "warning" and t.metric_value == "4")

    # ---- Resolve when the events age out of the window ----
    for r in world.restarts: r.detected_at = NOW() - timedelta(minutes=31)
    await step("all restarts older than the window")
    check("resolves with one resolve card", t.resolved_at is not None and len(cards("RESOLVE")) == 1 and not cards("ALERT"))
    check("resolve card carries the rule and subject", any("rule=rule1" in c and "subject=cluster" in c for c in cards("RESOLVE")))

    # ---- Window boundary and the setting ----
    r_world([r_rule(1, 4, {"info": 1})]); healthy(4); restart(4, 2, 29)
    await step("event 29 min old, default window 30"); await step("again")
    check("29 min old counts with N=30 (fires on the 2nd check)", trig() is not None and trig().metric_value == "1")
    r_world([r_rule(1, 4, {"info": 1})]); healthy(4); restart(4, 2, 31)
    await step("event 31 min old, default window 30"); await step("again")
    check("31 min old does not count with N=30", trig() is None)
    r_world([r_rule(1, 4, {"info": 1})], window=60); healthy(4); restart(4, 2, 31)
    await step("event 31 min old, window 60"); await step("again")
    t = trig()
    check("setting 60: the 31 min old event counts; text says 60 min; read at evaluation time",
          t is not None and "in the last 60 min" in t.message_sent and abs((NOW() - world.restart_queries[-1]["since"]).total_seconds() - 3600) < 5)
    rs._config["broker_restart_window_minutes"] = 15
    await step("window changed to 15 while open")
    check("window lowered to 15: the 31 min old event ages out, trigger resolves", t.resolved_at is not None)
    for bad in (999, "x", 5, True):
        r_world([r_rule(1, 4, {"info": 1})], window=bad); healthy(4); restart(4, 2, 20)
        await step(f"setting {bad!r}")
        check(f"invalid setting {bad!r}: default 30 used, warning logged, event 20 min old counted",
              world.restart_queries and abs((NOW() - world.restart_queries[-1]["since"]).total_seconds() - 1800) < 5
              and any("broker_restart_window_minutes" in l for l in LOGS))

    # ---- Description names the latest restart (by detected_at), not the first listed ----
    r_world([r_rule(1, 4, {"info": 1})]); healthy(4)
    restart(4, 1, 20); late = restart(4, 3, 2); restart(4, 2, 12)
    await step("3 events, latest broker 3"); await step("again")
    check("description names the most recent restart: broker 3, count 3",
          trig() is not None and f"Broker 3 restarted at {when(late)} UTC" in trig().message_sent and ". 3 broker restarts in the last 30 min" in trig().message_sent)
    r_world([r_rule(1, 4, {"info": 1})]); healthy(4)
    ev = restart(4, 2, 3); ev.previous_started_at = None
    await step("no previous start"); await step("again")
    check("unknown previous start: no 'after ...' text", trig() is not None and f"Broker 2 restarted at {when(ev)} UTC. 1 broker restart in" in trig().message_sent)

    # ---- Stale guard: no fire, no resolve, no counter change, no restart query ----
    for label, setup in (("stale broker metrics (10 min)", lambda: healthy(4, age_min=10)), ("no broker metrics rows", lambda: None)):
        r_world([r_rule(1, 4)]); setup(); restart(4, 2, 3); restart(4, 2, 2); restart(4, 1, 1)
        world.triggers[91] = NS(id=91, alert_config_id=1, cluster_id=4, subject="cluster", severity="warning", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="3")
        ta._threshold_breach_counts[(1, 4, "cluster")] = 1
        await step(label); await step(label + " (again)")
        check(f"{label}: skipped (no read, no fire/resolve, counter unchanged), INFO names it",
              not world.restart_queries and not cards("ALERT") and not cards("RESOLVE") and world.triggers[91].resolved_at is None
              and ta._threshold_breach_counts[(1, 4, "cluster")] == 1 and ignored_log())
        print("    log:", ignored_log())
    r_world([r_rule(1, 4, {"info": 1})]); brokers(4, [10, 10, 10], overrides={1: {"time": NOW() - timedelta(minutes=30)}}); restart(4, 2, 3)
    await step("one broker row stale, others fresh"); await step("again")
    check("a stale broker row alone (the restarted one may be down) does not hide the cluster", trig() is not None)
    r_world([r_rule(1, 4, {"info": 1}), r_rule(2, 8, {"info": 1})]); healthy(4); healthy(8, age_min=10); restart(4, 1, 3); restart(8, 1, 3)
    await step("two clusters, one stale"); await step("again")
    check("the fresh cluster fires, the stale one is skipped", [t.cluster_id for t in world.triggers.values()] == [4] and world.restart_queries and all(q["cids"] == [4] for q in world.restart_queries))

    # ---- Missing table / failing query: only this type is skipped ----
    r_world([r_rule(1, 4, {"info": 1}), rule(2, "cluster.urp_total", 4, {"warning": 1})]); healthy(4); restart(4, 1, 3)
    world.fresh = {4}; world.urp = {4: 7}; world.fail_restarts = True
    world.triggers[91] = NS(id=91, alert_config_id=1, cluster_id=4, subject="cluster", severity="info", triggered_at=NOW(), resolved_at=None, teams_post_success=True, metric_value="1")
    ta._threshold_breach_counts[(1, 4, "cluster")] = 1
    await step("restart table missing, check 1"); await step("restart table missing, check 2")
    check("urp rule on the same cluster still fires on its 2nd check", any(t.alert_config_id == 2 for t in world.triggers.values()) and len(cards("ALERT")) == 1)
    check("restart rule skipped: open trigger not resolved, counter unchanged, warning logged",
          world.triggers[91].resolved_at is None and ta._threshold_breach_counts[(1, 4, "cluster")] == 1
          and any("broker restart read failed" in l and RT in l for l in LOGS) and not [l for l in LOGS if "setup failed" in l])
    world.fail_restarts = False
    await step("table back"); await step("table back, check 2")
    check("evaluated again once the read works (counter reaches the confirm checks, open trigger kept)", world.triggers[91].resolved_at is None and world.triggers[91].metric_value == "1")
    # a query failure in another type must still behave as before (setup failure returns, nothing raises)
    r_world([r_rule(1, 4, {"info": 1})]); healthy(4); restart(4, 1, 3); world.fail_query = "FROM kafka_broker_metrics"
    await step("broker metrics query fails"); check("guard query failure: did not raise, no trigger", trig() is None)

    # ---- Tier config: cluster required, whole numbers, direction above ----
    import routes_alerts as RA
    check("registered: threshold, whole-number, label, evaluator type, action",
          RT in RA._THRESHOLD_ALERT_TYPES and RT in RA._WHOLE_TIER_ALERT_TYPES and RA._TYPE_LABELS[RT] == "Broker restarts"
          and RT in ta.CLUSTER_METRIC_TYPES and RT in ta._CLUSTER_METRIC_ACTIONS and RT not in RA._BELOW_ALERT_TYPES and RT not in RA._GENERAL_ALERT_TYPES)
    check("tiers 1/3/5 validate; 2.5 and descending are rejected",
          RA._threshold_config(T, False, True, False) == {"mode": "simple", **T}
          and all(_raises(lambda b=b: RA._threshold_config(b, False, True, False)) for b in ({"info": 2.5}, {"info": 5, "warning": 3}, {"warning": 0})))

    # ---- Per-rule channels ----
    EMAILS = []
    real_queue = ta._queue_rule_emails
    ta._queue_rule_emails = lambda emails: EMAILS.extend(e for e in emails if e)
    def chan(r, teams, email): r.enabled, r.email_enabled = teams, email; return r
    def mails(kind): return [e for e in EMAILS if e["kind"] == kind]
    def ch_world(teams, email):
        r_world([chan(r_rule(1, 4, {"info": 1}), teams, email)]); healthy(4); restart(4, 2, 5); EMAILS.clear()
    ch_world(False, True)
    await step("email-only: check 1"); await step("email-only: check 2")
    t = trig()
    check("email-only: trigger saved, no Teams card, detail 'Teams off for this rule', fire email with the description and action",
          t is not None and t.error_detail == "Teams off for this rule" and not cards("ALERT") and len(mails("fire")) == 1
          and "Broker 2 restarted at" in mails("fire")[0]["description"] and mails("fire")[0]["recommended_action"] == ta._CLUSTER_METRIC_ACTIONS[RT])
    world.restarts.clear(); EMAILS.clear()
    await step("email-only: events aged out")
    check("email-only: resolved by email, no resolve card", t.resolved_at is not None and len(mails("resolve")) == 1 and not cards("RESOLVE"))
    ch_world(True, False)
    await step("Teams-only: check 1"); await step("Teams-only: check 2")
    check("Teams-only: card posted, no email", len(cards("ALERT")) == 1 and trig().teams_post_success is True and not EMAILS)
    ch_world(False, False)
    await step("both off"); await step("both off, again")
    check("both toggles off: not evaluated (no restart read, no trigger)", not world.restart_queries and not world.triggers and not EMAILS)
    ta._queue_rule_emails = real_queue

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed" + ("" if all(RESULTS) else "  <-- FAILURES"))

def _raises(fn):
    try: fn()
    except Exception: return True
    return False
asyncio.run(main())
sys.exit(0 if RESULTS and all(RESULTS) else 1)
