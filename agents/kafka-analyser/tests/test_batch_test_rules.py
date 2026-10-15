"""Safety tests for scripts/batch_test_rules.py against a SCRATCH Postgres with
the live schema and a copy of the live kafka_alert_configs rows (65 rules,
only rule 5 enabled, no webhooks). The real evaluator (teams_alerts) runs
in-process as the "live agent", with a fake Teams (send_to_teams replaced by
a recorder), the repo's FakeSMTP for email, and the settings router served
in-process (httpx ASGITransport) so GET/POST /settings behave as on the agent.
Broker reachability (rule 5) is replaced by a no-op: no network is used.

Covers: the refusals (ids 3 and 5, a name without the prefix, another rule
with email on, a webhook set, recipients not empty, bad --to, a missing
webhook file, the test webhook being the agent-level one); dry run changing
nothing (tables compared before and after); a full run (fire, resolve, the
outbox rows, the "rule email sent" lines, the operator prompts, YES for lag,
--escalate); --channels email with global Teams off (no card, emails sent) and the
settings checks per --channels; TIMEOUT; the finally restore after an injected exception, a
KeyboardInterrupt and a real SIGINT (a second SIGINT during cleanup is
ignored); the final verification failing when a real rule changes; --cleanup
and --restore-settings; the backup's mode and content; and that the test
webhook never appears in stdout, stderr, the log, any file or the database.
Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates and
reseeds the tables it uses):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \
    -e BATCH_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_batch_test_rules.py
"""
import asyncio
import contextlib
import io
import json
import logging
import os
import re
import signal
import stat
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, HERE)

assert "scratch" in os.environ.get("BATCH_TEST_CONFIRM", ""), \
    "set BATCH_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"
assert not os.environ.get("ENCRYPTION_KEY"), "run without ENCRYPTION_KEY (settings are seeded in plain text)"

import httpx  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from sqlalchemy import text  # noqa: E402

import batch_test_rules as B  # noqa: E402
import database  # noqa: E402
import outbox  # noqa: E402
import routes_settings  # noqa: E402
import teams_alerts as TA  # noqa: E402
from _fakes import FakeSMTP  # noqa: E402
from storage import init_storage  # noqa: E402

MARK = "HOOKSECRET-7f3a91"
HOOK = f"https://teamstest.example.com/webhookb2/{MARK}/IncomingWebhook/abc/def"
AGENT_HOOK = "https://agentlevel.example.com/webhookb2/REAL-CHANNEL"
TO = "karthikeyan.gopalan@sintecmedia.com"
TMP = tempfile.mkdtemp(prefix="batchtest-")
LOG_FILE = os.path.join(TMP, "agent.log")
HOOK_FILE = os.path.join(TMP, "hook", "batch_webhook")
BACKUP_DIR = os.path.join(TMP, "backups")
os.environ["BATCH_TEST_WEBHOOK_FILE"] = HOOK_FILE
os.environ["BATCH_TEST_BACKUP_DIR"] = BACKUP_DIR
CLUSTERS = {3: ("DevQA Kafka Internal", False), 4: ("External Staging Kafka", True), 8: ("Internal Staging Kafka", True),
            9: ("Internal Production Kafka", True), 10: ("Internal Support Kafka", True)}
TABLES = ("kafka_alert_configs", "kafka_alert_triggers", "agent_config", "kafka_clusters", "kafka_job_schedules",
          "kafka_job_runs", "kafka_notification_outbox", "kafka_notification_attempts", "kafka_broker_metrics",
          "kafka_topic_metrics", "kafka_broker_distribution", "kafka_topic_message_rate_snapshots",
          "kafka_slo_compliance", "kafka_connector_snapshots", "kafka_consumer_group_lag")

PASSED = []
SMTP = FakeSMTP()
CARDS: list = []          # (webhook_url, card) posted to "Teams"
LIVE_RULES: list = []     # the copy of the live rules loaded into the scratch DB

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(file_handler)
logging.getLogger().setLevel(logging.INFO)
for noisy in ("httpx", "httpcore", "asyncio"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}", flush=True)


async def sql(statement, **params):
    async with database.SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


# ── Fakes: Teams, the evaluator tick, time, the operator ─────────────────────

async def fake_send_to_teams(webhook_url, card, report=None, **_kw):
    CARDS.append((webhook_url, card))
    return True


async def no_reachability():
    return None


TA.send_to_teams = fake_send_to_teams
TA._evaluate_broker_reachability = no_reachability


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t


class FakeTime:
    """Stands in for the script's time module: monotonic() follows the fake clock."""

    def __init__(self, clock):
        self._clock = clock

    def monotonic(self):
        return self._clock.monotonic()


class Agent:
    """The 'live agent' for one run: a fake clock, the evaluator run by the
    script's sleeps, scripted operator answers, optional hooks."""

    def __init__(self, answers=None, evaluate=True, on_tick=None, on_ask=None):
        self.clock = FakeClock()
        self.answers = list(answers or [])
        self.prompts = []
        self.evaluate = evaluate
        self.on_tick = on_tick
        self.on_ask = on_ask
        self.ticks = 0
        B.time = FakeTime(self.clock)

    async def sleep(self, seconds):
        self.clock.t += seconds
        self.ticks += 1
        if self.on_tick:
            await self.on_tick(self)
        if self.evaluate:
            await refresh_metrics()
            await TA.evaluate_alerts()
        await settle()

    async def ask(self, prompt):
        self.prompts.append(prompt)
        if self.on_ask:
            await self.on_ask(self, prompt)
        if not self.answers:
            raise AssertionError(f"unexpected prompt: {prompt}")
        return self.answers.pop(0)


async def settle():
    for _ in range(3):
        tasks = list(TA._rule_email_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await outbox.drain(10)
        await asyncio.sleep(0)


def settings_client():
    app = FastAPI()
    app.include_router(routes_settings.router)
    return B.AgentSettings("http://agent", transport=httpx.ASGITransport(app=app))


# ── Scratch data ─────────────────────────────────────────────────────────────

async def load_live_rules():
    rows = (await sql("SELECT row_to_json(c)::text FROM kafka_alert_configs c ORDER BY id")).all()
    LIVE_RULES[:] = [json.loads(r[0]) for r in rows]
    ids = {r["id"] for r in LIVE_RULES}
    assert len(LIVE_RULES) == 65 and {3, 5} <= ids, "load the live kafka_alert_configs rows into the scratch DB first"
    assert [r["id"] for r in LIVE_RULES if r["enabled"]] == [5]
    assert not any(r["webhook_url"] or r["email_enabled"] for r in LIVE_RULES)


async def put_setting(key, value):
    await routes_settings._upsert(key, value)


async def reset(**settings_over):
    await settle()
    await sql("TRUNCATE " + ", ".join(TABLES) + " RESTART IDENTITY CASCADE")
    await sql("INSERT INTO kafka_alert_configs SELECT * FROM json_populate_recordset(NULL::kafka_alert_configs, CAST(:j AS json))",
              j=json.dumps(LIVE_RULES))
    await sql("SELECT setval('kafka_alert_configs_id_seq', (SELECT MAX(id) FROM kafka_alert_configs))")
    for cid, (name, enabled) in CLUSTERS.items():
        await sql("INSERT INTO kafka_clusters (id, agent_slug, name, environment, source_type, auth_type, sasl_mechanism, "
                  "tls_enabled, enabled, status, created_at) VALUES (:id, 'kafka-analyser', :n, 'prod', 'real', 'none', "
                  "'PLAIN', false, :e, 'ok', now())", id=cid, n=name, e=enabled)
    await sql("INSERT INTO kafka_job_schedules (job_id, cron_expression, enabled, timeout_secs, created_at) "
              "VALUES ('kafka-alert-evaluator', '*/5 * * * *', true, 60, now())")
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, started_at, ended_at, logs, created_at) "
              "VALUES ('kafka-alert-evaluator', 'schedule', 'success', now(), now(), '', now())")
    for i, (cpu, heap, gb) in enumerate(((60, 50, 100.0), (40, 45, 90.0), (20, 40, 80.0)), 1):
        await sql("INSERT INTO kafka_broker_metrics (time, cluster_id, broker_id, heap_pct, gc_pause_ms, "
                  "request_handler_idle_pct, urp_count, messages_in_per_sec, cpu_pct, disk_pct, data_gb_true) "
                  "VALUES (now(), 8, :b, :h, 0, 90, 0, 1, :c, 10, :g)", b=str(i), h=heap, c=cpu, g=gb)
        await sql("INSERT INTO kafka_broker_distribution (cluster_id, broker_id, leader_partition_count, "
                  "replica_partition_count, data_gb, updated_at) VALUES (8, :b, :l, 100, 1, now())",
                  b=str(i), l=(120, 60, 60)[i - 1])
    for topic, rf in (("orders", 3), ("payments", 3), ("audit", 2), ("__consumer_offsets", 1)):
        await sql("INSERT INTO kafka_topic_metrics (time, cluster_id, topic, partition_count, replication_factor, "
                  "messages_in_per_sec, bytes_in_per_sec, bytes_out_per_sec, total_messages, size_bytes, retention_bytes, "
                  "retention_pct, urp_count) VALUES (now(), 8, :t, 6, :rf, 1, 1, 1, 1, 1, 1, 1, 0)", t=topic, rf=rf)
    for group, lag in (("g-big", 5000), ("g-small", 3)):
        await sql("INSERT INTO kafka_consumer_group_lag (cluster_id, group_id, state, total_lag, topic_count, "
                  "committed_offsets, updated_at) VALUES (8, :g, 'Stable', :l, 1, 1, now())", g=group, l=lag)
    await refresh_metrics()
    values = {
        "teams_enabled": True, "teams_webhook_url": AGENT_HOOK, "email_enabled": False, "email_recipients": "",
        "smtp_host": "127.0.0.1", "smtp_port": SMTP.port, "smtp_username": "", "smtp_password": "",
        "smtp_from_address": "kafka-alerts@sintecmedia.com", "notification_outbox_mode": "on",
        "health_email_recipients": "", **settings_over,
    }
    routes_settings._config.clear()
    routes_settings._config.update(routes_settings._DEFAULTS)
    for k, v in values.items():
        await put_setting(k, v)
    await routes_settings.load_config_from_db()
    SMTP.messages.clear()
    CARDS.clear()
    TA._threshold_breach_counts.clear()
    for path in (BACKUP_DIR,):
        if os.path.isdir(path):
            for f in os.listdir(path):
                os.remove(os.path.join(path, f))
    write_hook()
    with open(LOG_FILE, "w"):
        pass


async def refresh_metrics():
    """Keeps the seeded data fresh and adds a new inflow run (10 msgs/sec)."""
    await sql("UPDATE kafka_broker_metrics SET time = now()")
    await sql("UPDATE kafka_broker_distribution SET updated_at = now()")
    await sql("UPDATE kafka_consumer_group_lag SET updated_at = now()")
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, started_at, ended_at, logs, created_at) "
              "VALUES ('kafka-topic-structure-8', 'schedule', 'success', now(), now(), '', now())")
    await sql("INSERT INTO kafka_topic_message_rate_snapshots (cluster_id, topic, inflow, outflow, interval_seconds, collected_at) "
              "VALUES (8, 'orders', 2000, NULL, 300, clock_timestamp()), (8, 'payments', 1000, NULL, 300, clock_timestamp())")
    # one collected_at per run: align the second row on the first
    await sql("UPDATE kafka_topic_message_rate_snapshots s SET collected_at = m.c FROM "
              "(SELECT MAX(collected_at) c FROM kafka_topic_message_rate_snapshots) m "
              "WHERE s.collected_at > m.c - interval '1 second'")


def write_hook(content=HOOK):
    os.makedirs(os.path.dirname(HOOK_FILE), mode=0o700, exist_ok=True)
    with open(HOOK_FILE, "w") as fh:
        fh.write(content + "\n")


async def dump_tables() -> dict:
    out = {}
    for t in TABLES:
        out[t] = (await sql(f"SELECT coalesce(string_agg(x::text, E'\\n' ORDER BY x::text), '') FROM {t} x")).scalar()
    return out


async def run_script(argv, agent: Agent, settings=None):
    out, err = io.StringIO(), io.StringIO()
    deps = B.Deps(settings=settings or settings_client(), ask=agent.ask, sleep=agent.sleep, interactive_check=False)
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = await asyncio.create_task(B.run(B.parse_args(argv), deps))
    await settle()
    return code, out.getvalue() + err.getvalue()


async def assert_no_leak(output: str, check_db: bool = True):
    """The test webhook nowhere: output, log, files under TMP, database."""
    assert MARK not in output, "webhook in the output"
    file_handler.flush()
    for root, _dirs, files in os.walk(TMP):
        for f in files:
            path = os.path.join(root, f)
            if path == HOOK_FILE:
                continue
            with open(path, encoding="utf-8", errors="replace") as fh:
                assert MARK not in fh.read(), f"webhook in {path}"
    dump = await dump_tables() if check_db else {}
    for t, content in dump.items():
        assert MARK not in content, f"webhook in table {t}"


async def assert_restored(saved_recipients=""):
    rules = [json.loads(r[0]) for r in (await sql("SELECT row_to_json(c)::text FROM kafka_alert_configs c ORDER BY id")).all()]
    assert rules == LIVE_RULES, "the real rules changed"
    assert (await sql("SELECT COUNT(*) FROM kafka_alert_configs WHERE name LIKE 'BATCHTEST - %'")).scalar() == 0
    assert routes_settings._config["email_enabled"] is False, routes_settings._config["email_enabled"]
    assert routes_settings._config["email_recipients"] == saved_recipients
    db = {r.key: json.loads(r.value) for r in (await sql(
        "SELECT key, value FROM agent_config WHERE key IN ('email_enabled', 'email_recipients', 'teams_enabled')")).all()}
    assert db == {"email_enabled": False, "email_recipients": saved_recipients, "teams_enabled": True}, db


def answers_all(n=60, first=None):
    return ([first] if first else []) + ["y"] * n


# ── Unit-level refusals ──────────────────────────────────────────────────────

async def test_guard_refusals():
    await reset()
    for rule_id in (3, 5):
        for fn in (B.guard_rule, B.remove_rule):
            try:
                await fn(rule_id)
            except B.SafetyError as exc:
                assert "never touched" in str(exc)
            else:
                raise AssertionError(f"{fn.__name__}({rule_id}) not refused")
    # rule 3 renamed with the prefix is still refused (id check first)
    await sql("UPDATE kafka_alert_configs SET name = 'BATCHTEST - x' WHERE id = 3")
    try:
        await B.update_rule(3, {"enabled": True})
    except B.SafetyError:
        pass
    else:
        raise AssertionError("renamed rule 3 not refused")
    await sql("UPDATE kafka_alert_configs SET name = :n WHERE id = 3", n=next(r["name"] for r in LIVE_RULES if r["id"] == 3))
    for rule_id in (20, 87):
        for call in (B.update_rule(rule_id, {"enabled": True}), B.remove_rule(rule_id)):
            try:
                await call
            except B.SafetyError as exc:
                assert "does not start with" in str(exc)
            else:
                raise AssertionError(f"rule {rule_id} not refused")
    for bad in ("x", 5.0, True, None):
        try:
            await B.guard_rule(bad)
        except B.SafetyError:
            pass
        else:
            raise AssertionError(f"guard_rule({bad!r}) not refused")
    try:
        await B.create_rule({"name": "Broker CPU % - copy", "alert_type": "broker.cpu_pct", "cluster_id": 8,
                             "tiers": {"critical": 1}})
    except B.SafetyError as exc:
        assert "must start with" in str(exc)
    else:
        raise AssertionError("create without the prefix not refused")
    try:
        await B.update_rule(999, {"name": "BATCHTEST - y"})
    except B.SafetyError as exc:
        assert "not found" in str(exc)
    else:
        raise AssertionError("missing rule not refused")
    await assert_restored()
    ok("guard refuses ids 3 and 5 (even renamed), names without the prefix, bad ids; create needs the prefix")


async def test_check_to():
    for good in ("a.b@sintecmedia.com", "x@operative.com", "X.Y@Operative.com"):
        assert B.check_to(good) == good
    for bad in (None, "", "x@gmail.com", "x@operative.com.evil.com", "x@sub.operative.com", "x@evil-operative.com",
                "a@operative.com,b@operative.com", "a@operative.com b@operative.com", "a@b@operative.com",
                "a@operative.com;b@operative.com", "<a@operative.com>"):
        try:
            B.check_to(bad)
        except B.SafetyError:
            pass
        else:
            raise AssertionError(f"--to {bad!r} accepted")
    ok("--to: one address at @operative.com or @sintecmedia.com only")


async def test_masking():
    B.MASK.add(HOOK)
    record = logging.LogRecord("x", logging.WARNING, __file__, 1, "posting to %s failed", (HOOK,), None)
    B._MaskFilter().filter(record)
    assert MARK not in record.getMessage() and B.WEBHOOK_MASK in record.getMessage()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        B.say("url", HOOK)
    assert MARK not in out.getvalue() and B.WEBHOOK_MASK in out.getvalue()
    ok("say() and the logging filter mask the webhook")


# ── Refusals of a whole run ──────────────────────────────────────────────────

async def refused(argv, expect, agent=None, hook=HOOK, check_db=True):
    if hook is None:
        if os.path.exists(HOOK_FILE):
            os.remove(HOOK_FILE)
    else:
        write_hook(hook)
    before = await dump_tables()
    code, output = await run_script(argv, agent or Agent())
    assert code == 2, (code, output)
    assert "REFUSED" in output and expect in output, output
    await assert_no_leak(output, check_db)
    return before, output


async def test_run_refusals():
    run = ["--run", "--to", TO]

    await reset()
    await sql("UPDATE kafka_alert_configs SET email_enabled = true WHERE id = 20")
    await refused(run, "rule 20 (Under-replicated partitions - External Staging Kafka) has email_enabled on")
    assert not CARDS and not SMTP.messages

    await reset()
    await sql("UPDATE kafka_alert_configs SET webhook_url = 'https://other.example.com/hook' WHERE id = 28")
    _b, output = await refused(run, "rule 28 (Broker CPU % - External Staging Kafka) has a webhook_url set")
    assert "other.example.com" not in output  # the URL itself is never printed

    await reset(email_recipients="someone@operative.com")
    await refused(run, "email_recipients is not empty")
    assert routes_settings._config["email_recipients"] == "someone@operative.com"
    assert routes_settings._config["email_enabled"] is False

    await reset(teams_webhook_url=HOOK)
    # (this case seeds the hook as the agent-level URL itself, so the DB scan is skipped)
    await refused(run, "the test webhook is the agent-level teams_webhook_url", check_db=False)

    await reset()
    await refused(["--run", "--to", "me@gmail.com"], "--to must be at")
    await refused(["--run", "--to", "a@operative.com,b@operative.com"], "exactly one email address")
    await refused(["--run"], "--to is required")

    await reset()
    await refused(run, "is missing", hook=None)
    await refused(run, "does not hold a single https:// URL", hook="http://not-https.example.com/x")
    await refused(run, "is empty", hook="")

    await reset()
    await refused(["--run", "--to", TO, "--cluster", "3"], "is not an enabled cluster")
    await refused(["--run", "--to", TO, "--only", "cluster.nope"], "unknown type")

    # no change in any refusal: rules, settings, no BATCHTEST, no cards or emails
    await reset()
    await refused(["--run", "--to", "x@yahoo.com"], "--to must be at")
    await assert_restored()
    assert not CARDS and not SMTP.messages
    ok("--run refuses: other rule with email on or a webhook (named), recipients set, agent-level webhook, "
       "bad --to, missing/empty/non-https webhook file, disabled cluster, unknown type")


async def test_refusal_leaves_state():
    """A refusal after the backup: the state is unchanged, the backup exists
    (600), leftovers removed, verification printed and passing."""
    await reset(email_recipients="someone@operative.com")
    code, output = await run_script(["--run", "--to", TO], Agent())
    assert code == 2 and "VERIFICATION PASSED" in output, output
    files = os.listdir(BACKUP_DIR)
    assert len(files) == 1 and stat.S_IMODE(os.stat(os.path.join(BACKUP_DIR, files[0])).st_mode) == 0o600
    assert routes_settings._config["email_recipients"] == "someone@operative.com"
    assert routes_settings._config["email_enabled"] is False
    ok("refusal after the backup: settings untouched, verification passes, backup 600")


# ── Dry run ──────────────────────────────────────────────────────────────────

async def add_leftover():
    await sql("INSERT INTO kafka_alert_configs (name, alert_type, cluster_id, severity, config, webhook_url, cooldown_minutes, "
              "enabled, email_enabled, created_at, updated_at) VALUES ('BATCHTEST - broker.cpu_pct - Internal Staging Kafka', "
              "'broker.cpu_pct', 8, 'warning', '{\"mode\": \"simple\", \"critical\": 1}', :h, 0, true, true, now(), now())", h=HOOK)
    return (await sql("SELECT MAX(id) FROM kafka_alert_configs")).scalar()


async def test_dry_run_changes_nothing():
    await reset()
    leftover = await add_leftover()
    before = await dump_tables()
    agent = Agent()
    code, output = await run_script(["--to", TO], agent)
    after = await dump_tables()
    assert code == 0, output
    assert before == after, [t for t in TABLES if before[t] != after[t]]
    assert f"would remove leftover BATCHTEST rule {leftover}" in output
    assert "DRY RUN: nothing was changed" in output and "PLAN" in output
    assert not os.path.isdir(BACKUP_DIR) or not os.listdir(BACKUP_DIR), "dry run wrote a backup"
    assert os.path.exists(HOOK_FILE), "dry run removed the webhook file"
    assert agent.ticks == 0 and not agent.prompts and not CARDS and not SMTP.messages
    # the leftover still holds the webhook (it is a BATCHTEST copy); nothing printed it
    assert MARK not in output
    # with problems, dry run reports what --run would refuse, still changing nothing
    await sql("UPDATE kafka_alert_configs SET email_enabled = true WHERE id = 21")
    before = await dump_tables()
    code, output = await run_script(["--to", TO], Agent())
    assert code == 0 and "--run would REFUSE: rule 21" in output, output
    assert before == await dump_tables()
    ok("dry run: all 15 tables identical before and after, no backup, no prompts, webhook file kept, refusals reported")
    return output


# ── Full runs ────────────────────────────────────────────────────────────────

RESULT_COLS = ("type", "cluster", "fire", "fire_teams", "fire_email", "fire_log", "fire_ok", "escalate",
               "resolve", "resolve_teams", "resolve_email", "resolve_log", "resolve_ok", "secs")


def result_row(output, alert_type) -> dict:
    """One row of the RESULTS table as {column: value}."""
    table = output.split("\nRESULTS\n", 1)[1]
    line = next(l for l in table.splitlines() if l.strip().startswith(alert_type + " "))
    return dict(zip(RESULT_COLS, re.split(r"\s{2,}", line.strip())))


async def test_full_run():
    await reset()
    leftover = await add_leftover()
    agent = Agent(answers=answers_all())
    code, output = await run_script(["--run", "--to", TO, "--log-file", LOG_FILE], agent)
    assert code == 0, output
    assert f"removed leftover BATCHTEST rule {leftover}" in output
    for t in ("broker.cpu_pct", "broker.heap_pct", "cluster.rf_below_min", "cluster.leader_skew",
              "cluster.data_gb_max", "cluster.data_spread_pct", "cluster.msg_rate_in"):
        row = result_row(output, t)
        assert row["cluster"] == "8 Internal Staging Kafka" and row["fire"].startswith("critical x"), row
        assert row["resolve"].startswith("resolved x") and row["escalate"] == "-", row
        assert (row["fire_teams"], row["fire_email"], row["fire_log"], row["fire_ok"]) == ("ok", "sent", "sent", "y"), row
        assert (row["resolve_teams"], row["resolve_email"], row["resolve_log"], row["resolve_ok"]) == ("sent", "sent", "sent", "y"), row
    for t, why in (("cluster.urp_total", "value 0"), ("cluster.zk_ensemble", "zookeeper_url empty"),
                   ("cluster.connect_failed_connectors", "kafka_connect_url is empty"),
                   ("cluster.sr_nodes_down", "schema_registry_url is empty")):
        assert t in output and why in output, (t, why)
    assert "VERIFICATION PASSED" in output, output
    await assert_restored()
    # Teams: every card to the test webhook, none to the agent-level one; resolve cards included
    assert CARDS and all(url == HOOK for url, _c in CARDS), {u for u, _c in CARDS}
    # email: only --to, one fire and one resolve per tested rule (cpu/heap: per broker)
    assert SMTP.messages and all(rcpts == [TO] for rcpts, _m in SMTP.messages)
    subjects = SMTP.subjects()
    assert sum((s or "").startswith("[RESOLVED]") for s in subjects) >= 7, subjects
    # outbox rows were found for each fire and resolve
    assert output.count("outbox row") >= 14, output
    # the backup: whole table + settings, leftover's webhook excluded, mode 600
    files = os.listdir(BACKUP_DIR)
    assert len(files) == 1 and files[0].startswith("rules-before-batch-") and files[0].endswith("Z.json"), files
    path = os.path.join(BACKUP_DIR, files[0])
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    doc = json.load(open(path))
    assert doc["settings"] == {"email_enabled": False, "email_recipients": "", "teams_enabled": True}, doc["settings"]
    rows = {r["id"]: r for r in doc["kafka_alert_configs"]}
    assert len(rows) == 66 and rows[leftover]["webhook_url"] is None and rows[leftover]["webhook_url_excluded"] is True
    assert [rows[i] for i in sorted(rows) if i != leftover] == LIVE_RULES
    assert not os.path.exists(HOOK_FILE), "webhook file not removed after the run"
    await assert_no_leak(output)
    ok("full run: 7 types fire and resolve on cluster 8 (cards to the test webhook only, emails to --to only, "
       "outbox rows and log lines found, operator y), 7 skipped with reasons; leftover removed; backup 600 with "
       "the leftover's webhook excluded; settings restored; verification passed; no webhook anywhere")
    return output


def test_settings_checks_channels():
    s = {"email_recipients": "", "teams_enabled": False, "email_enabled": True}
    assert B.settings_problems(s, "email") == [], B.settings_problems(s, "email")
    assert B.settings_problems(s, "both") == B.settings_problems(s, "teams") == [
        "setting teams_enabled is off: no Teams card would be posted (use --channels email)"], B.settings_problems(s, "both")
    assert B.settings_problems({**s, "email_enabled": False}, "email") == []  # the run turns email_enabled on
    assert B.settings_problems({**s, "teams_enabled": True, "email_enabled": False}) == []
    assert B.parse_args(["--to", TO]).channels == "both" and B.parse_args(["--channels", "email"]).channels == "email"
    ok("settings checks: --channels email runs with global Teams off, both/teams need it; --channels default both")


async def test_channels_email_only():
    await reset(teams_enabled=False)
    agent = Agent(answers=answers_all())
    code, output = await run_script(["--run", "--to", TO, "--log-file", LOG_FILE, "--channels", "email",
                                     "--only", "broker.cpu_pct"], agent)
    assert code == 0, output
    assert "(Teams and Email off; webhook" in output and "(Teams off, Email on)" in output, output
    row = result_row(output, "broker.cpu_pct")
    assert row["fire"].startswith("critical x") and row["resolve"].startswith("resolved x"), row
    assert (row["fire_teams"], row["fire_email"], row["fire_log"], row["fire_ok"]) == ("off", "sent", "sent", "y"), row
    assert (row["resolve_teams"], row["resolve_email"], row["resolve_log"], row["resolve_ok"]) == ("skipped", "sent", "sent", "y"), row
    assert "note:" not in output.split("\nRESULTS\n", 1)[1], output
    assert "error=Teams off for this rule" in output and "teams_post_success=True" not in output, output
    assert not CARDS, CARDS
    subjects = SMTP.subjects()
    assert any((s or "").startswith("[RESOLVED]") for s in subjects) and all(rcpts == [TO] for rcpts, _m in SMTP.messages), subjects
    assert agent.prompts and all("NO card in the TEST channel" in p for p in agent.prompts), agent.prompts
    assert "VERIFICATION PASSED" in output, output
    rules = [json.loads(r[0]) for r in (await sql("SELECT row_to_json(c)::text FROM kafka_alert_configs c ORDER BY id")).all()]
    assert rules == LIVE_RULES, "the real rules changed"
    assert (routes_settings._config["email_enabled"], routes_settings._config["email_recipients"],
            routes_settings._config["teams_enabled"]) == (False, "", False)
    ok("--channels email with global Teams off: the copy runs, no card (triggers saved as 'Teams off for this rule', "
       "outbox Teams skipped on the resolve), fire and resolve emails sent, prompts ask for no card")


async def test_settle_outbox():
    await reset()
    from types import SimpleNamespace as NS
    out = io.StringIO()
    t = (await sql("INSERT INTO kafka_alert_triggers (alert_config_id, triggered_at, teams_post_success, error_detail, "
                   "is_recurrence) VALUES (20, now(), false, 'queued (outbox row 1)', false) RETURNING id")).scalar()
    found = [NS(id=t)]
    sleeps = []

    clock = FakeClock()
    B.time = FakeTime(clock)

    async def sleep(secs):
        sleeps.append(secs)
        clock.t += secs
        if len(sleeps) == 2:  # the worker settles the row and writes the trigger back
            await sql("UPDATE kafka_notification_outbox SET email_status = 'sent', teams_status = 'sent'")
            await sql("UPDATE kafka_alert_triggers SET teams_post_success = true, error_detail = NULL WHERE id = :t", t=t)
    ctx = NS(saved_settings={"_rule_outbox_evaluators": []}, deps=NS(sleep=sleep))
    with contextlib.redirect_stdout(out):
        same = await B.settle_outbox(ctx, "broker.cpu_pct", "rule_fire", 20, found)
    assert same is found and not sleeps and out.getvalue() == "", out.getvalue()
    await sql("INSERT INTO kafka_notification_outbox (kind, source, routing, origin, dedupe_key, alert_config_id, "
              "trigger_ids, payload, email_status, teams_status, expires_at, state) VALUES ('rule_fire', 't', 'rule', "
              "'outbox', 'k1', 20, ARRAY[:t]::integer[], '{}', 'pending', 'pending', now() + interval '1 day', 'pending')", t=t)
    ctx.saved_settings["_rule_outbox_evaluators"] = ["broker_thresholds"]
    with contextlib.redirect_stdout(out):
        got = await B.settle_outbox(ctx, "broker.cpu_pct", "rule_fire", 20, found)
    assert sleeps == [10, 10] and got[0].teams_post_success is True and got[0].error_detail is None, (sleeps, got)
    assert "outbox (rule_fire): settled after 20 s: row 1 email sent, teams sent" in out.getvalue(), out.getvalue()
    assert [B.evaluator_of(a) for a in ("close_wait_spike", "broker_unreachable", "broker.heap_pct",
                                        "cluster.consumer_lag", "cluster.urp_total")] == [
        "close_wait", "reachability", "broker_thresholds", "lag", "cluster_metrics"]
    ok("settle_outbox: with rule_outbox_evaluators empty nothing is read, waited or printed; with the type's "
       "evaluator listed it waits for both channels to settle, prints the wait and returns the written-back triggers")


async def test_lag_needs_yes_and_escalation():
    await reset()
    agent = Agent(answers=["no"])
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.consumer_lag"], agent)
    assert code == 2 and "not confirmed with YES" in output and "fire for REAL lag" in output, output
    await assert_restored()
    assert not CARDS

    await reset()
    agent = Agent(answers=["YES"] + ["y"] * 10)
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.consumer_lag,cluster.leader_skew",
                                      "--escalate", "--log-file", LOG_FILE], agent)
    assert code == 0, output
    assert "custom row for 'g-big' only" in output
    for t in ("cluster.consumer_lag", "cluster.leader_skew"):
        row = result_row(output, t)
        assert row["fire"] == "warning x1" and row["escalate"] == "critical (y)", row
        assert row["resolve"] == "resolved x1" and row["resolve_ok"] == "y", row
    # g-small (lag 3) never fired: the generic tiers are out of reach
    assert not any("g-small" in json.dumps(card) for _u, card in CARDS)
    assert "VERIFICATION PASSED" in output
    await assert_restored()
    await assert_no_leak(output)
    ok("lag needs YES (refused otherwise); lag copy fires for one name only; --escalate: warning -> critical -> resolve")


async def test_timeout_moves_on():
    await reset()
    agent = Agent(answers=answers_all(), evaluate=False)
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.leader_skew,cluster.data_gb_max"], agent)
    assert code == 0, output
    assert output.count("TIMEOUT") >= 2 and "no critical trigger within 20 min" in output, output
    assert output.count("disabled and deleted") == 2
    assert "VERIFICATION PASSED" in output
    await assert_restored()
    ok("TIMEOUT after 20 min (fake clock): reported, copy deleted, next rule runs")


async def test_skip_answer_deletes_copy():
    await reset()
    agent = Agent(answers=["s", "y", "y"])
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.leader_skew,cluster.data_gb_max"], agent)
    assert code == 0, output
    assert "operator skipped after the fire" in output
    # the skipped copy is gone before the next rule is created
    first_delete = output.index("disabled and deleted")
    assert first_delete < output.index("=== cluster.data_gb_max")
    assert "VERIFICATION PASSED" in output
    await assert_restored()
    ok("operator 's' skips the rest of the rule; its copy is deleted before the next rule")


# ── finally: exception, KeyboardInterrupt, SIGINT ────────────────────────────

async def test_finally_after_exception():
    await reset()

    async def boom(agent, prompt):
        raise RuntimeError("injected failure")

    agent = Agent(answers=["y"], on_ask=boom)
    # RuntimeError inside a rule is recorded and the run moves on; make it hit the run itself:
    settings = settings_client()
    real_write = settings.write
    calls = {"n": 0}

    async def write(values):
        calls["n"] += 1
        await real_write(values)
        if calls["n"] == 1:
            raise RuntimeError(f"injected failure after the settings write (url {HOOK})")

    settings.write = write
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.leader_skew"], agent, settings)
    assert code == 1 and "injected failure" in output and "VERIFICATION PASSED" in output, output
    assert calls["n"] == 2, calls  # the restore ran
    await assert_restored()
    assert not os.path.exists(HOOK_FILE)
    await assert_no_leak(output)

    # an exception in the middle of a rule (operator prompt): rule error noted, cleanup and restore still happen
    await reset()
    agent = Agent(answers=["y"], on_ask=boom)
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.leader_skew"], agent)
    assert "ERROR RuntimeError: injected failure" in output and "VERIFICATION PASSED" in output, output
    await assert_restored()
    await assert_no_leak(output)
    ok("finally after an injected exception (run-level and rule-level): rules removed, settings restored, verified, "
       "webhook masked in the error text")


async def test_finally_after_keyboard_interrupt():
    await reset()

    async def interrupt(agent, prompt):
        raise KeyboardInterrupt

    agent = Agent(answers=["y"], on_ask=interrupt)
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.leader_skew,cluster.data_gb_max"], agent)
    assert code == 130 and "STOPPED by Ctrl+C" in output, output
    assert "removed BATCHTEST rule" in output and "VERIFICATION PASSED" in output, output
    await assert_restored()
    assert not os.path.exists(HOOK_FILE)
    await assert_no_leak(output)
    ok("finally after KeyboardInterrupt at a prompt: the open copy removed, settings restored, verified")


async def test_finally_after_sigint():
    await reset()

    async def sigint_on_third_tick(agent):
        if agent.ticks == 3:
            os.kill(os.getpid(), signal.SIGINT)
            await asyncio.sleep(0.05)

    settings = settings_client()
    real_write = settings.write

    async def write(values):
        if values.get("email_enabled") is False:  # the restore: a second Ctrl+C now must be ignored
            os.kill(os.getpid(), signal.SIGINT)
            await asyncio.sleep(0.05)
        await real_write(values)

    settings.write = write
    agent = Agent(answers=answers_all(), on_tick=sigint_on_third_tick)
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.leader_skew"], agent, settings)
    assert code == 130 and "SIGINT received" in output and "SIGINT ignored: cleanup in progress" in output, output
    assert "VERIFICATION PASSED" in output
    await assert_restored()
    await assert_no_leak(output)
    # the handlers were removed: SIGINT is Python's default again
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    ok("real SIGINT mid-wait: stops, cleans up, restores, verifies; a second SIGINT during cleanup is ignored")


async def test_verification_fails_on_change():
    await reset()

    async def tamper(agent):
        if agent.ticks == 1:
            await sql("UPDATE kafka_alert_configs SET enabled = true WHERE id = 20")

    agent = Agent(answers=answers_all(), on_tick=tamper)
    code, output = await run_script(["--run", "--to", TO, "--only", "cluster.leader_skew"], agent)
    assert code == 1 and "FAIL enabled rules: [5, 20] (saved [5])" in output and "VERIFICATION FAILED" in output, output
    assert "FAIL non-BATCHTEST rows unchanged" in output
    await sql("UPDATE kafka_alert_configs SET enabled = false WHERE id = 20")
    ok("final verification fails (exit 1) when a real rule changes during the run")


async def test_cleanup_mode():
    await reset()
    leftover = await add_leftover()
    code, output = await run_script(["--cleanup"], Agent())
    assert code == 0 and f"would remove leftover BATCHTEST rule {leftover}" in output, output
    assert (await sql("SELECT COUNT(*) FROM kafka_alert_configs WHERE id = :i", i=leftover)).scalar() == 1
    # a crashed run left settings changed: --cleanup --restore-settings --run puts them back from the backup
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with open(os.path.join(BACKUP_DIR, "rules-before-batch-20261013T000000Z.json"), "w") as fh:
        json.dump({"settings": {"email_enabled": False, "email_recipients": "", "teams_enabled": True}}, fh)
    await put_setting("email_enabled", True)
    await put_setting("email_recipients", TO)
    await routes_settings.load_config_from_db()
    code, output = await run_script(["--cleanup", "--restore-settings", "--run"], Agent())
    assert code == 0 and f"removed leftover BATCHTEST rule {leftover}" in output and "VERIFICATION PASSED" in output, output
    await assert_restored()
    assert os.path.exists(HOOK_FILE), "--cleanup does not need or remove the webhook file"
    await assert_no_leak(output)
    ok("--cleanup: dry by default; with --run removes leftovers; --restore-settings restores from the newest backup")


async def main():
    init_storage("kafka-analyser")
    await load_live_rules()
    try:
        await test_check_to()
        await test_masking()
        await test_guard_refusals()
        await test_run_refusals()
        await test_refusal_leaves_state()
        dry = await test_dry_run_changes_nothing()
        await test_full_run()
        test_settings_checks_channels()
        await test_settle_outbox()
        await test_channels_email_only()
        await test_lag_needs_yes_and_escalation()
        await test_timeout_moves_on()
        await test_skip_answer_deletes_copy()
        await test_finally_after_exception()
        await test_finally_after_keyboard_interrupt()
        await test_finally_after_sigint()
        await test_verification_fails_on_change()
        await test_cleanup_mode()
    finally:
        await reset()
    print(f"\n{len(PASSED)} passed")


if __name__ == "__main__":
    asyncio.run(main())
