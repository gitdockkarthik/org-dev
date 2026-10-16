"""Tests for scripts/apply_rule_plan.py against a SCRATCH Postgres with the
live schema and a copy of the live kafka_alert_configs rows (65 rules, only
rule 5 enabled, no webhooks). The settings router is served in-process
(httpx ASGITransport), so GET/POST /settings behave as on the agent. The
CSV is the app's own export (routes_alert_export.build_csv) of the scratch
rows, then edited the way Excel and the Kafka head would edit it.

Covers: parse and diff (TRUE/false in any case, 3 vs 3.0, blank cells,
rule 5 and rule 3, the tier cells of the general types, id / alert_type /
cluster_id mismatches, missing and unknown ids, bad numbers and booleans);
the plan (rule 3 never, rule 5 Teams on, lag only through --only, --skip);
invalid tiers refusing the whole run; a dry run changing nothing; a real run
changing exactly the intended columns and passing verification, the backup
(mode 600, every column, the two settings); a run whose verification fails;
--revert (dry, then byte for byte); the refusals (rule_outbox_evaluators,
BATCHTEST rules, --recipients); --list-lag-offenders; and that no webhook
value reaches any output. Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates and
reseeds the tables it uses):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \
    -e APPLY_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_apply_rule_plan.py
"""
import asyncio
import contextlib
import csv
import io
import json
import os
import stat
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

assert "scratch" in os.environ.get("APPLY_TEST_CONFIRM", ""), \
    "set APPLY_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"
assert not os.environ.get("ENCRYPTION_KEY"), "run without ENCRYPTION_KEY (settings are seeded in plain text)"

import httpx  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from sqlalchemy import select, text  # noqa: E402

import apply_rule_plan as A  # noqa: E402
import batch_test_rules as B  # noqa: E402
import database  # noqa: E402
import routes_alerts as RA  # noqa: E402
import routes_settings  # noqa: E402
from models import KafkaAlertConfig  # noqa: E402
from routes_alert_export import EXPORT_COLUMNS, build_csv  # noqa: E402
from storage import init_storage  # noqa: E402

MARK = "HOOKSECRET-c41d"
RULE_HOOK = f"https://rulehook.example.com/webhookb2/{MARK}/rule"
AGENT_HOOK = f"https://agentlevel.example.com/webhookb2/{MARK}/agent"
TO = "Prod_Kafka_alerts@operative.com"
TMP = tempfile.mkdtemp(prefix="applyplan-")
BACKUP_DIR = os.path.join(TMP, "backups")
os.environ["APPLY_RULES_BACKUP_DIR"] = BACKUP_DIR
CLUSTERS = {3: ("DevQA Kafka Internal", False), 4: ("External Staging Kafka", True), 8: ("Internal Staging Kafka", True),
            9: ("Internal Production Kafka", True), 10: ("Internal Support Kafka", True)}
TABLES = ("kafka_alert_configs", "kafka_alert_triggers", "agent_config", "kafka_clusters", "kafka_job_schedules",
          "kafka_job_runs", "kafka_notification_outbox", "kafka_broker_metrics", "kafka_topic_metrics",
          "kafka_broker_distribution", "kafka_topic_message_rate_snapshots", "kafka_slo_compliance",
          "kafka_connector_snapshots", "kafka_consumer_group_lag")
LAG_TYPES = ("cluster.consumer_lag", "cluster.connector_lag")

PASSED = []
LIVE_RULES: list = []
OUTPUTS: list = []


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}", flush=True)


async def sql(statement, **params):
    async with database.SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


def settings_client():
    app = FastAPI()
    app.include_router(routes_settings.router)
    return B.AgentSettings("http://agent", transport=httpx.ASGITransport(app=app))


async def load_live_rules():
    rows = (await sql("SELECT row_to_json(c)::text FROM kafka_alert_configs c ORDER BY id")).all()
    LIVE_RULES[:] = [json.loads(r[0]) for r in rows]
    assert len(LIVE_RULES) == 65, "load the live kafka_alert_configs rows into the scratch DB first"
    assert [r["id"] for r in LIVE_RULES if r["enabled"]] == [5]
    assert not any(r["webhook_url"] or r["email_enabled"] for r in LIVE_RULES)


async def rules_now() -> list[dict]:
    return [json.loads(r[0]) for r in (await sql("SELECT row_to_json(c)::text FROM kafka_alert_configs c ORDER BY id")).all()]


async def reset(evaluators=("broker_thresholds", "cluster_metrics")):
    await sql("TRUNCATE " + ", ".join(TABLES) + " RESTART IDENTITY CASCADE")
    await sql("INSERT INTO kafka_alert_configs SELECT * FROM json_populate_recordset(NULL::kafka_alert_configs, CAST(:j AS json))",
              j=json.dumps(LIVE_RULES))
    await sql("SELECT setval('kafka_alert_configs_id_seq', (SELECT MAX(id) FROM kafka_alert_configs))")
    for cid, (name, enabled) in CLUSTERS.items():
        await sql("INSERT INTO kafka_clusters (id, agent_slug, name, environment, source_type, auth_type, sasl_mechanism, "
                  "tls_enabled, enabled, status, created_at) VALUES (:id, 'kafka-analyser', :n, 'prod', 'real', 'none', "
                  "'PLAIN', false, :e, 'ok', now())", id=cid, n=name, e=enabled)
    # cluster 8: CPU 96 / 40 / 20 (critical under 90/95), heap 50, data 100/90/80 GB
    for i, (cpu, heap, gb) in enumerate(((96, 50, 100.0), (40, 45, 90.0), (20, 40, 80.0)), 1):
        await sql("INSERT INTO kafka_broker_metrics (time, cluster_id, broker_id, heap_pct, gc_pause_ms, "
                  "request_handler_idle_pct, urp_count, messages_in_per_sec, cpu_pct, disk_pct, data_gb_true) "
                  "VALUES (now(), 8, :b, :h, 0, 90, 0, 1, :c, 10, :g)", b=str(i), h=heap, c=cpu, g=gb)
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, started_at, ended_at, logs, created_at) "
              "VALUES ('kafka-topic-structure-8', 'schedule', 'success', now(), now(), '', now())")
    for topic, rf in (("orders", 3), ("payments", 2), ("audit", 2), ("__consumer_offsets", 1)):
        await sql("INSERT INTO kafka_topic_metrics (time, cluster_id, topic, partition_count, replication_factor, "
                  "messages_in_per_sec, bytes_in_per_sec, bytes_out_per_sec, total_messages, size_bytes, retention_bytes, "
                  "retention_pct, urp_count) VALUES (now(), 8, :t, 6, :rf, 1, 1, 1, 1, 1, 1, 1, 0)", t=topic, rf=rf)
    await sql("INSERT INTO kafka_slo_compliance (cluster_id, hour_bucket, overall_compliance_pct) "
              "VALUES (8, date_trunc('hour', now()), 82.0)")
    lag = (("g-huge", 60000, 0), ("g-big", 12000, 0), ("g-edge", 10000, 0), ("g-small", 3, 0), ("g-old", 90000, 30),
           ("connect-sink-a", 40000, 0), ("connect-sink-b", 5, 0))
    for group, value, age_min in lag:
        await sql("INSERT INTO kafka_consumer_group_lag (cluster_id, group_id, state, total_lag, topic_count, "
                  "committed_offsets, updated_at) VALUES (8, :g, 'Stable', :l, 1, 1, now() - make_interval(mins => :a))",
                  g=group, l=value, a=age_min)
    await sql("INSERT INTO kafka_connector_snapshots (cluster_id, connector_name, state, collected_at) "
              "VALUES (8, 'sink-a', 'RUNNING', now()), (8, 'sink-b', 'RUNNING', now())")  # one run
    values = {"teams_enabled": True, "teams_webhook_url": AGENT_HOOK, "email_enabled": False, "email_recipients": "",
              "rule_outbox_evaluators": list(evaluators), "rule_teams_fallback": True}
    routes_settings._config.clear()
    routes_settings._config.update(routes_settings._DEFAULTS)
    for k, v in values.items():
        await routes_settings._upsert(k, v)
    await routes_settings.load_config_from_db()
    if os.path.isdir(BACKUP_DIR):
        for f in os.listdir(BACKUP_DIR):
            os.remove(os.path.join(BACKUP_DIR, f))


async def dump_tables() -> dict:
    return {t: (await sql(f"SELECT coalesce(string_agg(x::text, E'\\n' ORDER BY x::text), '') FROM {t} x")).scalar()
            for t in TABLES}


# ── The file ─────────────────────────────────────────────────────────────────

async def export_rows() -> list[dict]:
    async with database.SessionLocal() as s:
        configs = (await s.execute(select(KafkaAlertConfig).order_by(KafkaAlertConfig.id))).scalars().all()
    names = {cid: n for cid, (n, _e) in CLUSTERS.items()}
    return list(csv.DictReader(io.StringIO(build_csv(configs, names))))


def excelify(rows: list[dict]) -> list[dict]:
    """What Excel does on save: TRUE/FALSE, and whole numbers that look like 30.0 in places."""
    out = []
    for r in rows:
        r = dict(r)
        for c in ("enabled", "email_enabled", "send_resolve_card", "has_custom_webhook"):
            r[c] = r[c].upper() if r[c] else r[c]
        if r["cooldown_minutes"]:
            r["cooldown_minutes"] += ".0"
        out.append(r)
    return out


def write_csv(rows: list[dict], name="his.csv") -> str:
    path = os.path.join(TMP, name)
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(EXPORT_COLUMNS), lineterminator="\r\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in EXPORT_COLUMNS})
    return path


def by_id(rows):
    return {int(r["id"]): r for r in rows}


def rows_of(rules):
    return [r for r in rules if r["alert_type"]]


def his_edits(rows: list[dict]) -> list[dict]:
    """The rollout file's kind of edits, valid ones only (CPU 90/95, heap 80/90,
    SLO 90/80, lag 10000/50000, rule 5 cooldown 15, rule 3 edited too)."""
    ids = by_id(rows)
    for r in rows:
        t = r["alert_type"]
        if t == "broker.cpu_pct":
            r["warning"], r["critical"] = "90.0", "95"
        elif t == "broker.heap_pct":
            r["warning"], r["critical"] = "80", "90"
        elif t == "cluster.slo_compliance":
            r["warning"], r["critical"] = "90", "80"
        elif t == "cluster.consumer_lag":
            r["warning"], r["critical"] = "10000", "50000"
        elif t == "cluster.data_gb_max":
            r["warning"], r["critical"] = "", ""  # blank in his file: no change
    ids[5]["cooldown_minutes"] = "15"
    ids[5]["warning"] = "7"        # tier cells of a general type: ignored
    ids[3]["cooldown_minutes"] = "5"   # rule 3 is never planned
    return rows


async def run_script(argv, settings=None):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        code = await A.run(A.parse_args(argv), A.Deps(settings=settings or settings_client()))
    OUTPUTS.append(out.getvalue())
    return code, out.getvalue()


def assert_no_leak(output: str):
    assert MARK not in output, "a webhook value in the output"


# ── Tests ────────────────────────────────────────────────────────────────────

async def test_parse_and_diff():
    await reset()
    rules = await rules_now()
    plain = await export_rows()
    # unchanged export, Excel-saved: no change anywhere
    diffs = A.diff(rules, A.read_csv(write_csv(excelify(plain))))
    assert len(diffs) == 65 and not any(d.changes for d in diffs), [(d.rule["id"], d.changes) for d in diffs if d.changes]
    # his edits
    diffs = {d.rule["id"]: d for d in A.diff(rules, A.read_csv(write_csv(his_edits(excelify(await export_rows())))))}
    assert diffs[30].changes == {"warning": (70, 90), "critical": (90, 95)}, diffs[30].changes
    assert isinstance(diffs[30].changes["warning"][1], int)  # "90.0" -> 90
    assert diffs[31].changes == {"warning": (70, 80), "critical": (85, 90)}
    assert diffs[88].changes == {"warning": (75, 90), "critical": (60, 80)}
    assert diffs[50].changes == {} and diffs[37].changes == {}  # blanks; leader skew 3.0 vs "3.0"
    assert diffs[5].changes == {"cooldown_minutes": (30, 15)}, diffs[5].changes  # tier cell ignored
    assert diffs[3].changes == {"cooldown_minutes": (15, 5)}
    # number format and case: 3 vs 3.0 vs 3.00, booleans in any case
    rows = await export_rows()
    ids = by_id(rows)
    ids[22]["critical"], ids[22]["enabled"], ids[22]["email_enabled"] = "5.00", "False", "fAlSe"
    ids[42]["critical"] = "0.0"
    assert not any(d.changes for d in A.diff(rules, A.read_csv(write_csv(rows))))

    async def refused(mutate, expect):
        rows = await export_rows()
        mutate(rows)
        try:
            A.diff(rules, A.read_csv(write_csv(rows)))
        except A.SafetyError as exc:
            assert expect in str(exc), (expect, str(exc))
        else:
            raise AssertionError(f"not refused: {expect}")

    await refused(lambda rs: by_id(rs)[30].update(alert_type="broker.heap_pct"), "rule 30 (line 14): alert_type 'broker.heap_pct' in the file")
    await refused(lambda rs: by_id(rs)[30].update(cluster_id="9"), "rule 30 (line 14): cluster_id 9 in the file, 8 live")
    await refused(lambda rs: by_id(rs)[5].update(cluster_id="8"), "rule 5 (line 3): cluster_id 8 in the file, None live")
    await refused(lambda rs: by_id(rs)[30].update(id="999"), "not in the live rules: [999]")
    await refused(lambda rs: rs.pop(), "missing from the file: [90]")
    await refused(lambda rs: by_id(rs)[31].update(id="30"), "rule 30 appears more than once")
    await refused(lambda rs: by_id(rs)[30].update(warning="1,000"), "rule 30 (line 14) warning: '1,000' is not a number")
    await refused(lambda rs: by_id(rs)[30].update(enabled="yes"), "'yes' is not TRUE or FALSE")
    await refused(lambda rs: by_id(rs)[30].update(min_rf="3"), "min_rf does not apply to broker.cpu_pct")
    await refused(lambda rs: by_id(rs)[22].update(reminder_hours="3"), "reminder_hours does not apply")
    # the whole-file checks
    for content, expect in (("id,name\n1,x\n", "missing column(s)"), ("", "missing column(s)")):
        path = os.path.join(TMP, "bad.csv")
        with open(path, "w") as fh:
            fh.write(content)
        try:
            A.read_csv(path)
        except A.SafetyError as exc:
            assert expect in str(exc), str(exc)
        else:
            raise AssertionError(expect)
    ok("parse and diff: Excel booleans/numbers, blanks, rule 5 tier cells ignored, rule 3, every mismatch refused")


async def test_plan():
    await reset()
    rules = await rules_now()
    diffs = A.diff(rules, A.read_csv(write_csv(his_edits(excelify(await export_rows())))))
    plan, excluded = A.build_plan(diffs, [], [])
    planned = {i.rule["id"]: i for i in plan}
    lag_ids = {r["id"] for r in rules if r["alert_type"] in LAG_TYPES}
    assert 3 not in planned and not (lag_ids & set(planned)) and len(plan) == 65 - 1 - len(lag_ids), len(plan)
    assert planned[5].body == {"enabled": True, "email_enabled": True, "cooldown_minutes": 15}, planned[5].body
    assert planned[30].body == {"enabled": False, "email_enabled": True, "tiers": {"warning": 90, "critical": 95}}
    assert planned[87].body["tiers"] == {"warning": 90, "critical": 80}
    assert planned[50].body == {"enabled": False, "email_enabled": True}
    assert all(A.validate(i) is None for i in plan), [(i.rule["id"], A.validate(i)) for i in plan if A.validate(i)]
    # --only: lag types come in only this way; rule 3 never, even named
    plan, _ = A.build_plan(diffs, ["cluster.consumer_lag", "close_wait_spike"], [])
    assert sorted(i.rule["id"] for i in plan) == [80, 81, 82, 83], [i.rule["id"] for i in plan]
    assert plan[0].body["tiers"] == {"warning": 10000, "critical": 50000}
    # --skip
    plan, excluded = A.build_plan(diffs, [], ["broker.cpu_pct", "broker_unreachable"])
    assert not {28, 30, 32, 34, 5} & {i.rule["id"] for i in plan}
    assert {d.rule["id"]: why for d, why in excluded}[30] == "--skip"
    ok("plan: rule 3 never, rule 5 Email+Teams, others Email only, lag only via --only, --skip; all rows validate")


async def test_invalid_tiers_refused():
    for edit, expect in (
        (lambda r: r["alert_type"] == "cluster.rf_below_min" and r.update(warning="1", critical="1"),
         "tiers.warning must be less than tiers.critical"),
        (lambda r: r["alert_type"] == "cluster.slo_compliance" and r.update(warning="101"),
         "tiers.warning must be a number from 0 to 100"),
        (lambda r: r["alert_type"] == "broker.heap_pct" and r.update(warning="95", critical="90"),
         "tiers.warning must be less than tiers.critical"),
        (lambda r: r["alert_type"] == "cluster.connect_workers_down" and r.update(critical="2.5"),
         "must be a whole number greater than 0"),
        (lambda r: r["alert_type"] == "cluster.rf_below_min" and r.update(min_rf="1"), "min_rf must be a whole number of at least 2"),
        (lambda r: r["id"] == "5" and r.update(cooldown_minutes="-1"), "cooldown_minutes"),
    ):
        await reset()
        rows = await export_rows()
        for r in rows:
            edit(r)
        before = await dump_tables()
        for argv in ([], ["--run"]):
            code, output = await run_script(["--csv", write_csv(rows), "--recipients", TO] + argv)
            assert code == 2 and "REFUSED: the API would refuse these rows" in output and expect in output, output
            assert "rule " in output and "PLAN:" not in output
        assert await dump_tables() == before
        assert not os.path.isdir(BACKUP_DIR) or not os.listdir(BACKUP_DIR)
    ok("invalid tiers (equal, out of range, wrong order, not whole, min_rf, cooldown) refuse the whole run; nothing written")


async def test_dry_run_changes_nothing():
    await reset()
    await sql("UPDATE kafka_alert_configs SET webhook_url = :h WHERE id = 22", h=RULE_HOOK)
    before = await dump_tables()
    code, output = await run_script(["--csv", write_csv(his_edits(excelify(await export_rows()))), "--recipients", TO])
    assert code == 0, output
    assert await dump_tables() == before
    assert routes_settings._config["email_enabled"] is False and routes_settings._config["email_recipients"] == ""
    assert "DRY RUN: nothing was changed" in output and "safety checks: OK" in output
    assert "PLAN: 57 rules" in output and "NOT PLANNED: 8 rules" in output, output
    line30 = next(ln for ln in output.splitlines() if ln.startswith("  rule  30 "))
    assert "Teams off->off  Email off->on  would fire now: critical (value 96.0)" in line30, line30
    assert "would fire now: warning (value 82.0)" in next(ln for ln in output.splitlines() if ln.startswith("  rule  88 "))
    assert "would fire now: no (value 50.0)" in next(ln for ln in output.splitlines() if ln.startswith("  rule  31 "))
    assert "would fire now: warning (value 2 topics below RF 3)" in next(ln for ln in output.splitlines() if ln.startswith("  rule  23 "))
    assert "would fire now: n/a" in next(ln for ln in output.splitlines() if ln.startswith("  rule   5 "))
    assert "Teams on->on" in next(ln for ln in output.splitlines() if ln.startswith("  rule   5 "))
    assert "rule   3 close_wait_spike" in output and "rule 3 stays off; file differs: cooldown_minutes 15 -> 5" in output
    assert "lag type: only through --only" in output and "a fire needs 2 consecutive breaching checks" in output
    assert "SUMMARY" in output and "Email off -> on: 57" in output and "Teams on -> off: 0" in output
    assert not os.path.isdir(BACKUP_DIR) or not os.listdir(BACKUP_DIR)
    assert_no_leak(output)
    # --only lag in a dry run: per-name forecast
    code, output = await run_script(["--csv", write_csv(his_edits(excelify(await export_rows()))), "--recipients", TO,
                                     "--only", ",".join(LAG_TYPES)])
    assert code == 0 and "PLAN: 7 rules" in output, output
    assert "would fire now: critical (4 of 6 names; largest g-huge 60000)" in output, output
    assert "would fire now: warning (1 of 2 names; largest sink-a 40000)" in output, output
    assert await dump_tables() == before
    ok("dry run: plan, would-fire per rule (above, below strict, min_rf, lag, n/a), summary; nothing changed")
    return output


async def test_real_run_and_revert():
    await reset()
    await sql("UPDATE kafka_alert_configs SET webhook_url = :h WHERE id = 22", h=RULE_HOOK)
    start = await rules_now()
    path = write_csv(his_edits(excelify(await export_rows())))
    code, output = await run_script(["--csv", path, "--recipients", TO, "--run"])
    assert code == 0 and "VERIFICATION PASSED" in output, output
    assert_no_leak(output)
    after = {r["id"]: r for r in await rules_now()}
    lag_ids = {r["id"] for r in start if r["alert_type"] in LAG_TYPES}
    for old in start:
        new = after[old["id"]]
        changed = {k for k in old if old[k] != new[k]}
        if old["id"] == 3 or old["id"] in lag_ids:
            assert not changed, (old["id"], changed)
            continue
        assert new["email_enabled"] is True and new["enabled"] is (old["id"] == 5), old["id"]
        allowed = {"enabled", "email_enabled", "updated_at", "config", "cooldown_minutes"}
        assert changed <= allowed, (old["id"], changed)
        if old["alert_type"] == "broker.cpu_pct":
            assert new["config"] == {**old["config"], "warning": 90, "critical": 95}, new["config"]
        elif old["alert_type"] == "cluster.slo_compliance":
            assert new["config"] == {**old["config"], "warning": 90, "critical": 80}, new["config"]
        elif old["id"] == 5:
            assert new["cooldown_minutes"] == 15 and new["config"] == old["config"]
        elif old["alert_type"] not in ("broker.heap_pct",):
            assert new["config"] == old["config"] and new["cooldown_minutes"] == old["cooldown_minutes"], old["id"]
    assert after[22]["webhook_url"] == RULE_HOOK
    assert routes_settings._config["email_enabled"] is True and routes_settings._config["email_recipients"] == TO
    db = {r.key: json.loads(r.value) for r in (await sql(
        "SELECT key, value FROM agent_config WHERE key IN ('email_enabled', 'email_recipients')")).all()}
    assert db == {"email_enabled": True, "email_recipients": TO}, db
    # the backup
    files = os.listdir(BACKUP_DIR)
    assert len(files) == 1 and files[0].startswith("rules-before-apply-") and files[0].endswith("Z.json"), files
    backup = os.path.join(BACKUP_DIR, files[0])
    assert stat.S_IMODE(os.stat(backup).st_mode) == 0o600
    with open(backup) as fh:
        doc = json.load(fh)
    assert doc["kafka_alert_configs"] == start and doc["settings"] == {"email_enabled": False, "email_recipients": ""}
    ok("real run: exactly the intended columns, other rules untouched, settings set, backup 600 with every column")

    # revert: dry, then for real, byte for byte
    mid = await dump_tables()
    code, output = await run_script(["--revert", backup])
    assert code == 0 and "rules to restore: 57 of 65" in output and "DRY RUN" in output, output
    assert await dump_tables() == mid
    assert_no_leak(output)
    code, output = await run_script(["--revert", backup, "--run"])
    assert code == 0 and "VERIFICATION PASSED" in output, output
    assert_no_leak(output)
    assert await rules_now() == start, "rules not restored byte for byte"
    assert routes_settings._config["email_enabled"] is False and routes_settings._config["email_recipients"] == ""
    db = {r.key: json.loads(r.value) for r in (await sql(
        "SELECT key, value FROM agent_config WHERE key IN ('email_enabled', 'email_recipients')")).all()}
    assert db == {"email_enabled": False, "email_recipients": ""}, db
    # a second revert has nothing to do
    code, output = await run_script(["--revert", backup, "--run"])
    assert code == 0 and "rules to restore: 0 of 65" in output and "VERIFICATION PASSED" in output, output
    # revert refuses when something it does not manage changed
    await sql("UPDATE kafka_alert_configs SET name = 'renamed' WHERE id = 30")
    code, output = await run_script(["--revert", backup, "--run"])
    assert code == 2 and "rule 30: name changed since the backup" in output, output
    ok("--revert: dry run changes nothing; --run restores every rule byte for byte and both settings; refuses foreign changes")


async def test_verification_fails():
    await reset()
    original = RA.update_alert_config

    async def tampering(config_id, payload):
        result = await original(config_id, payload)
        if config_id == 30:
            await sql("UPDATE kafka_alert_configs SET cooldown_minutes = 99 WHERE id = 31")  # an "other" change
            await sql("UPDATE kafka_alert_configs SET enabled = true WHERE id = 20")
        return result

    RA.update_alert_config = tampering
    try:
        code, output = await run_script(["--csv", write_csv(his_edits(await export_rows())), "--recipients", TO, "--run",
                                         "--skip", "broker.heap_pct"])
    finally:
        RA.update_alert_config = original
    assert code == 1 and "VERIFICATION FAILED" in output, output
    assert "FAIL" in output and "20: enabled True" in output and "31 (cooldown_minutes)" in output, output
    ok("verification fails (exit 1) when a planned rule ends wrong or another rule changes")


async def test_refusals():
    good = ["--recipients", TO]
    await reset(evaluators=("broker_thresholds",))
    path = write_csv(his_edits(await export_rows()))
    before = await dump_tables()
    code, output = await run_script(["--csv", path] + good)
    assert code == 0 and "--run would REFUSE: rule_outbox_evaluators is ['broker_thresholds']" in output, output
    code, output = await run_script(["--csv", path, "--run"] + good)
    assert code == 2 and "REFUSED: rule_outbox_evaluators" in output and "missing cluster_metrics" in output, output
    assert await dump_tables() == before
    assert not os.path.isdir(BACKUP_DIR) or not os.listdir(BACKUP_DIR)

    await reset()
    await sql("INSERT INTO kafka_alert_configs (name, alert_type, cluster_id, severity, config, cooldown_minutes, enabled, "
              "email_enabled, created_at, updated_at) VALUES ('BATCHTEST - broker.cpu_pct - x', 'broker.cpu_pct', 8, "
              "'warning', '{\"mode\": \"simple\", \"critical\": 1}', 0, false, false, now(), now())")
    rows = await export_rows()
    path = write_csv(rows)
    before = await dump_tables()
    code, output = await run_script(["--csv", path, "--run"] + good)
    assert code == 2 and "BATCHTEST rule(s) exist: [91]" in output, output
    assert await dump_tables() == before

    await reset()
    path = write_csv(await export_rows())
    for bad in ("x@gmail.com", "a@operative.com,b@operative.com", "a@operative.com.evil.com", ""):
        code, output = await run_script(["--csv", path, "--recipients", bad, "--run"])
        assert code == 2 and "REFUSED: --recipients" in output, (bad, output)
    assert A.check_recipients("Prod_Kafka_alerts@operative.com") == "Prod_Kafka_alerts@operative.com"  # case kept
    code, output = await run_script(["--csv", path, "--recipients", TO, "--only", "cluster.nope"])
    assert code == 2 and "--only: unknown type(s) cluster.nope" in output
    code, output = await run_script(["--csv", os.path.join(TMP, "absent.csv"), "--recipients", TO])
    assert code == 2 and "file not found" in output
    for argv in (["--list-lag-offenders", "--run"], ["--revert", "x", "--csv", path], ["--run"]):
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                A.parse_args(argv)
        except SystemExit:
            pass
        else:
            raise AssertionError(argv)
    await sql("UPDATE kafka_alert_configs SET alert_type = 'cluster.urp_total', cluster_id = 3 WHERE id = 5")
    code, output = await run_script(["--csv", path, "--recipients", TO, "--run"])
    assert code == 2 and "rule 5 (line 3): alert_type 'broker_unreachable' in the file, 'cluster.urp_total' live" in output, output
    before = await dump_tables()
    code, output = await run_script(["--csv", write_csv(await export_rows()), "--recipients", TO, "--run"])
    assert code == 2 and "REFUSED: rule 5 is 'cluster.urp_total', expected 'broker_unreachable'" in output, output
    assert await dump_tables() == before
    ok("refusals: rule_outbox_evaluators, BATCHTEST rules, --recipients, unknown types, missing file, bad options, rule 5 type")


async def test_list_lag_offenders():
    await reset()
    await sql("UPDATE kafka_alert_configs SET config = config || '{\"blacklist\": [\"g-big\"]}' WHERE id = 81")
    before = await dump_tables()
    code, output = await run_script(["--list-lag-offenders"])
    assert code == 0, output
    assert await dump_tables() == before
    lines = output.splitlines()
    i = lines.index(next(ln for ln in lines if ln.startswith("cluster 8 Internal Staging Kafka -- cluster.consumer_lag (rule 81)")))
    assert "warning tier 10000; 4 of 6 consumer groups at or above it, 1 already blacklisted or custom, 3 to consider" in lines[i], lines[i]
    assert [ln.split() for ln in lines[i + 1:i + 5]] == [["60000", "g-huge"], ["40000", "connect-sink-a"],
                                                          ["12000", "g-big", "(blacklisted)"], ["10000", "g-edge"]], lines[i + 1:i + 5]
    assert lines[i + 5:i + 10] == ["  -- paste into the blacklist box (one name per line) --", "g-huge", "connect-sink-a",
                                   "g-edge", "  -- end --"], lines[i + 5:i + 10]
    assert "g-old" not in output and "g-small" not in output  # stale row; below the tier
    j = lines.index(next(ln for ln in lines if ln.startswith("cluster 8 Internal Staging Kafka -- cluster.connector_lag")))
    assert "1 of 2 connectors at or above it" in lines[j] and lines[j + 3] == "sink-a", lines[j:j + 5]
    assert "cluster 9 Internal Production Kafka -- cluster.consumer_lag (rule 82): no data" in output
    assert "TOTAL to consider: 1 connectors, 3 consumer groups" in output, output
    ok("--list-lag-offenders: read-only, per cluster, sorted by lag, blacklisted marked, paste block of names only")


async def main():
    init_storage("kafka-analyser")
    await load_live_rules()
    try:
        await test_parse_and_diff()
        await test_plan()
        await test_invalid_tiers_refused()
        await test_dry_run_changes_nothing()
        await test_real_run_and_revert()
        await test_verification_fails()
        await test_refusals()
        await test_list_lag_offenders()
        for out in OUTPUTS:
            assert_no_leak(out)
        ok(f"no webhook value in any of the {len(OUTPUTS)} outputs")
    finally:
        await reset()
    print(f"\n{len(PASSED)} passed")


if __name__ == "__main__":
    asyncio.run(main())
