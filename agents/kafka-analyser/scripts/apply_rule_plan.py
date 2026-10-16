#!/usr/bin/env python3
"""Apply the Kafka head's alert thresholds (an edited copy of the Teams tab's
"Export CSV") to the live rules, switch every planned rule to Email on and
Teams off (rule 5: both on), and set the global email settings. See
README_apply_rule_plan.md beside this file.

Runs inside the agent container, like batch_test_rules.py (whose readers,
probes and settings client it reuses): init_storage, the routes_alerts
validation and update function for every rule change, and the LIVE agent's
GET/POST /settings for email_enabled and email_recipients (the evaluator
reads its own in-memory copy of them).

  dry run (the default; reads only, prints the plan and "would fire now"):
    docker exec -it <container> python scripts/apply_rule_plan.py --csv FILE --recipients LIST@operative.com
  real run:
    docker exec -it <container> python scripts/apply_rule_plan.py --csv FILE --recipients LIST@operative.com --run
  undo (dry run, then for real):
    docker exec -it <container> python scripts/apply_rule_plan.py --revert BACKUP.json [--run]
  lag offenders, per cluster, for the blacklist boxes (read-only):
    docker exec -it <container> python scripts/apply_rule_plan.py --list-lag-offenders

Safety (covered by tests/test_apply_rule_plan.py): nothing changes without
--run; any invalid or mismatched row refuses the whole run before anything
is written; rule 3 is never planned, the lag types only through --only; a
backup of every kafka_alert_configs row and the two settings is written
(mode 600) before the first change; each change is verified afterwards.
Webhook URLs are never printed."""
import argparse
import asyncio
import csv
import json
import math
import os
import re
import sys
import traceback
from dataclasses import dataclass, field
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)

import batch_test_rules as B  # noqa: E402  (readers, probes, settings client, masking)
from batch_test_rules import SafetyError, say  # noqa: E402

DEFAULT_BACKUP_DIR = "/home/backups/manual"
DEFAULT_AGENT_URL = "http://localhost:8003"
SETTINGS_KEYS = ("email_enabled", "email_recipients")
REQUIRED_OUTBOX_EVALUATORS = ("broker_thresholds", "cluster_metrics")
EXCLUDED_IDS = frozenset({3})      # CLOSE_WAIT: stays off
TEAMS_ON_IDS = frozenset({5})      # reachability: Email and Teams on
EXPECTED_TYPES = {3: "close_wait_spike", 5: "broker_unreachable"}
TIERS = ("info", "warning", "critical")
COMPARED = TIERS + ("min_rf", "cooldown_minutes", "reminder_hours")
# Columns that may differ in the file but are never applied: reported only.
NOT_APPLIED = ("name", "severity", "send_resolve_card", "threshold")
LAG_FRESH_MINUTES = 20  # teams_alerts._LAG_MAX_AGE
FIRE_NOTE = ("a fire needs 2 consecutive breaching checks (SLO: 2 hourly readings; message rate: 2 new "
             "inflow runs), so 'would fire' is a forecast, not an immediate card or email")
_FORMULA = re.compile(r"'*(?:[ \t]*[=+\-@]|[\t\r])")  # routes_alert_export._FORMULA


def _types():
    import routes_alerts as RA
    return RA


def known_types() -> tuple:
    RA = _types()
    return RA._THRESHOLD_ALERT_TYPES + RA._LAG_ALERT_TYPES + RA._GENERAL_ALERT_TYPES


def parse_types(value: str | None, option: str) -> list[str]:
    if not value:
        return []
    types = [t.strip() for t in value.split(",") if t.strip()]
    unknown = [t for t in types if t not in known_types()]
    if unknown:
        raise SafetyError(f"{option}: unknown type(s) {', '.join(unknown)}; known: {', '.join(known_types())}")
    return list(dict.fromkeys(types))


def check_recipients(value: str | None) -> str:
    """--recipients: one address at @operative.com or @sintecmedia.com, case kept."""
    if not value:
        raise SafetyError("--recipients is required: one address at @operative.com or @sintecmedia.com")
    try:
        return B.check_to(value)
    except SafetyError as exc:
        raise SafetyError(str(exc).replace("--to", "--recipients"))


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", help="the edited export (23 columns)")
    p.add_argument("--recipients", help="the one address for email_recipients")
    p.add_argument("--only", help="comma-separated alert types to plan (needed for the two lag types)")
    p.add_argument("--skip", help="comma-separated alert types to leave out")
    p.add_argument("--run", action="store_true", help="required to change anything")
    p.add_argument("--revert", metavar="BACKUP", help="restore rules and settings from a backup this script wrote")
    p.add_argument("--list-lag-offenders", action="store_true", help="read-only: names at or above each lag rule's warning tier")
    p.add_argument("--agent-url", default=os.environ.get("APPLY_RULES_AGENT_URL", DEFAULT_AGENT_URL))
    args = p.parse_args(argv)
    modes = [m for m in ("revert", "list_lag_offenders") if getattr(args, m)]
    if len(modes) > 1:
        p.error("--revert and --list-lag-offenders together")
    if modes and (args.csv or args.recipients or args.only or args.skip):
        p.error("--csv, --recipients, --only and --skip belong to the apply mode")
    if args.list_lag_offenders and args.run:
        p.error("--list-lag-offenders is read-only")
    if not modes and not args.csv:
        p.error("--csv is required (or use --revert / --list-lag-offenders)")
    return args


# ── The file ─────────────────────────────────────────────────────────────────

def _cell(row: dict, col: str) -> str:
    v = (row.get(col) or "").strip()
    return v[1:] if v.startswith("'") and _FORMULA.match(v[1:]) else v


def parse_number(text: str, where: str) -> float | None:
    """A blank cell is None; 3, 3.0 and 3,0-free decimals are numbers."""
    if text == "":
        return None
    try:
        value = float(text)
    except ValueError:
        raise SafetyError(f"{where}: {text!r} is not a number")
    if not math.isfinite(value):
        raise SafetyError(f"{where}: {text!r} is not a finite number")
    return value


def parse_bool(text: str, where: str) -> bool | None:
    if text == "":
        return None
    low = text.lower()
    if low in ("true", "false"):
        return low == "true"
    raise SafetyError(f"{where}: {text!r} is not TRUE or FALSE")


def norm(value: float):
    """3.0 -> 3 (the API takes whole tiers only as integers); 2.5 stays."""
    return int(value) if float(value).is_integer() else float(value)


def read_csv(path: str) -> dict[int, dict]:
    from routes_alert_export import EXPORT_COLUMNS
    try:
        with open(path, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            header = [h.strip() for h in (reader.fieldnames or [])]
            missing = [c for c in EXPORT_COLUMNS if c not in header]
            if missing:
                raise SafetyError(f"{path}: missing column(s) {', '.join(missing)} (expected the {len(EXPORT_COLUMNS)}-column export)")
            reader.fieldnames = header
            rows = {}
            for n, row in enumerate(reader, 2):
                if not any((v or "").strip() for v in row.values() if isinstance(v, str)):
                    continue  # an empty line Excel left at the end
                ident = parse_number(_cell(row, "id"), f"line {n} id")
                if ident is None or not ident.is_integer():
                    raise SafetyError(f"line {n}: id {_cell(row, 'id')!r} is not a rule id")
                ident = int(ident)
                if ident in rows:
                    raise SafetyError(f"line {n}: rule {ident} appears more than once")
                row["_line"] = n
                rows[ident] = row
    except FileNotFoundError:
        raise SafetyError(f"{path}: file not found")
    except UnicodeDecodeError:
        raise SafetyError(f"{path}: not UTF-8 (save it from Excel as 'CSV UTF-8')")
    except csv.Error as exc:
        raise SafetyError(f"{path}: not a readable CSV ({exc})")
    if not rows:
        raise SafetyError(f"{path}: no rule rows")
    return rows


# ── Diff and plan ────────────────────────────────────────────────────────────

def live_value(rule: dict, col: str):
    if col == "cooldown_minutes":
        return rule.get("cooldown_minutes")
    return (rule.get("config") or {}).get(col)


def applies(rule: dict, col: str) -> bool:
    RA = _types()
    t = rule["alert_type"]
    if col in TIERS:
        return t not in RA._GENERAL_ALERT_TYPES
    if col == "min_rf":
        return t == RA._MIN_RF_ALERT_TYPE
    if col == "reminder_hours":
        return t in RA._LAG_ALERT_TYPES
    return True


@dataclass
class RuleDiff:
    rule: dict
    changes: dict = field(default_factory=dict)   # col -> (old, new)
    ignored: list = field(default_factory=list)   # "col: live x, file y (not applied)"


def diff(rules: list[dict], rows: dict[int, dict]) -> list[RuleDiff]:
    """One RuleDiff per live rule, in id order. Refuses on any mismatch."""
    by_id = {r["id"]: r for r in rules}
    problems = []
    unknown = sorted(set(rows) - set(by_id))
    if unknown:
        problems.append(f"the file has rule id(s) not in the live rules: {unknown}")
    absent = sorted(set(by_id) - set(rows))
    if absent:
        problems.append(f"live rule id(s) missing from the file: {absent} (use a fresh export)")
    out = []
    for rid in sorted(set(rows) & set(by_id)):
        rule, row = by_id[rid], rows[rid]
        where = f"rule {rid} (line {row['_line']})"
        try:
            if _cell(row, "alert_type").lower() != rule["alert_type"].lower():
                problems.append(f"{where}: alert_type {_cell(row, 'alert_type')!r} in the file, {rule['alert_type']!r} live")
            file_cluster = parse_number(_cell(row, "cluster_id"), f"{where} cluster_id")
            if (None if file_cluster is None else norm(file_cluster)) != rule.get("cluster_id"):
                problems.append(f"{where}: cluster_id {_cell(row, 'cluster_id') or 'blank'} in the file, {rule.get('cluster_id')} live")
            d = RuleDiff(rule)
            for col in COMPARED:
                text = _cell(row, col)
                if not applies(rule, col):
                    if text and col not in TIERS:
                        problems.append(f"{where}: {col} {text!r} is set, but {col} does not apply to {rule['alert_type']}")
                    continue  # tier cells of the two general types are ignored
                new = parse_number(text, f"{where} {col}")
                if new is None:
                    continue  # blank: no change, never a cleared tier
                old = live_value(rule, col)
                if old is None or float(old) != new:
                    d.changes[col] = (old, norm(new))
            for col in NOT_APPLIED:
                text = _cell(row, col)
                if col == "send_resolve_card":
                    file_v = parse_bool(text, f"{where} {col}")
                    live_v = (rule.get("config") or {}).get("send_resolve_card", True) is not False
                    if file_v is not None and file_v != live_v:
                        d.ignored.append(f"send_resolve_card: live {live_v}, file {file_v}")
                elif col == "threshold":
                    file_v = parse_number(text, f"{where} threshold")
                    live_v = (rule.get("config") or {}).get("threshold")
                    if file_v is not None and (live_v is None or float(live_v) != file_v):
                        d.ignored.append(f"threshold: live {live_v}, file {norm(file_v)}")
                elif text and text != str(rule.get(col) or ""):
                    d.ignored.append(f"{col}: live {rule.get(col)!r}, file {text!r}")
            for col in ("enabled", "email_enabled"):
                parse_bool(_cell(row, col), f"{where} {col}")  # must be readable; the plan decides the value
            out.append(d)
        except SafetyError as exc:
            problems.append(str(exc))
    if problems:
        raise SafetyError("the file does not match the live rules:\n  - " + "\n  - ".join(problems))
    return out


@dataclass
class PlanItem:
    diff: RuleDiff
    enabled: bool
    email_enabled: bool
    body: dict = field(default_factory=dict)       # the PUT body
    expect_config: dict = field(default_factory=dict)  # changed config keys -> value
    fire: str = "-"

    @property
    def rule(self) -> dict:
        return self.diff.rule


def exclusion(rule: dict, only: list[str], skip: list[str]) -> str | None:
    RA = _types()
    t = rule["alert_type"]
    if rule["id"] in EXCLUDED_IDS:
        return f"rule {rule['id']} stays off"
    if t in skip:
        return "--skip"
    if only:
        return None if t in only else "not in --only"
    if t in RA._LAG_ALERT_TYPES:
        return "lag type: only through --only (fill the blacklists first)"
    return None


def build_plan(diffs: list[RuleDiff], only: list[str], skip: list[str]) -> tuple[list[PlanItem], list[tuple]]:
    plan, excluded = [], []
    for d in diffs:
        why = exclusion(d.rule, only, skip)
        if why:
            excluded.append((d, why))
            continue
        teams = d.rule["id"] in TEAMS_ON_IDS
        item = PlanItem(d, enabled=teams, email_enabled=True)
        item.body = {"enabled": teams, "email_enabled": True}
        tier_changes = {c: v[1] for c, v in d.changes.items() if c in TIERS}
        if tier_changes:
            config = d.rule.get("config") or {}
            item.body["tiers"] = {t: norm(float(config[t])) if t not in tier_changes else tier_changes[t]
                                  for t in TIERS if t in config or t in tier_changes}
        for col in ("min_rf", "cooldown_minutes", "reminder_hours"):
            if col in d.changes:
                item.body[col] = d.changes[col][1]
        item.expect_config = {c: v[1] for c, v in d.changes.items() if c != "cooldown_minutes"}
        plan.append(item)
    return plan, excluded


def validate(item: PlanItem) -> str | None:
    """The routes_alerts validation of the PUT body, without writing: None or why not."""
    RA = _types()
    from fastapi import HTTPException
    from pydantic import ValidationError
    rule, body = item.rule, item.body
    t = rule["alert_type"]
    try:
        RA.AlertConfigUpdatePayload(**body)
        previous = rule.get("config") or {}
        if t in RA._LAG_ALERT_TYPES:
            RA._lag_config(previous, body.get("tiers"), None, None, body.get("reminder_hours"))
        elif "tiers" in body:
            RA._threshold_config(body["tiers"], t in RA._BELOW_ALERT_TYPES, t in RA._WHOLE_TIER_ALERT_TYPES,
                                 t in RA._PCT_TIER_ALERT_TYPES)
        if t == RA._MIN_RF_ALERT_TYPE:
            RA._min_rf(body.get("min_rf", previous.get("min_rf")))
    except HTTPException as exc:
        return str(exc.detail)
    except ValidationError as exc:
        return "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
    return None


# ── Would it fire now ────────────────────────────────────────────────────────

async def lag_values(alert_type: str, cid: int) -> tuple[dict[str, int] | None, str | None]:
    """{name: lag} as _evaluate_cluster_lag builds it, or (None, why ignored)."""
    import teams_alerts as TA
    now = B._now()
    since = now - TA._LAG_MAX_AGE
    async with B._session() as session:
        groups = {r.group_id: int(r.total_lag or 0)
                  for r in (await session.execute(TA._LAG_FRESH_SQL, {"cids": [cid], "since": since})).all()}
        if alert_type == TA._CONSUMER_LAG_ALERT_TYPE:
            return (groups, None) if groups else (None, f"no consumer group rows updated in {LAG_FRESH_MINUTES} min")
        snap = (await session.execute(TA._CONNECT_SNAPSHOT_SQL, {"cids": [cid], "since": since})).all()
    if not snap:
        return None, f"no connector snapshot in {LAG_FRESH_MINUTES} min"
    if snap[0].previous_rows is not None and len(snap) * 2 < snap[0].previous_rows:
        return None, "latest connector snapshot looks partial"
    names = {r.connector_name: groups[f"connect-{r.connector_name}"] for r in snap if f"connect-{r.connector_name}" in groups}
    return (names, None) if names else (None, f"no connector with a group row updated in {LAG_FRESH_MINUTES} min")


def _new_config(item: PlanItem) -> dict:
    config = dict(item.rule.get("config") or {})
    config.update(item.expect_config)
    return config


async def would_fire(item: PlanItem) -> str:
    """The tier the live value breaches under the NEW tiers, "no", or "n/a"."""
    import teams_alerts as TA
    RA = _types()
    rule = item.rule
    t, cid = rule["alert_type"], rule.get("cluster_id")
    config = _new_config(item)
    if t in RA._GENERAL_ALERT_TYPES or cid is None:
        return "n/a"
    try:
        if t in RA._LAG_ALERT_TYPES:
            names, ignored = await lag_values(t, cid)
            if names is None:
                return f"no (ignored: {ignored})"
            generic, custom, blacklist = TA._lag_rule_parts(SimpleNamespace(config=config))
            hits = []
            for name, lag in names.items():
                tiers = custom.get(name) if name in custom else (None if name in blacklist else generic)
                tier = TA._threshold_tier(tiers, lag) if tiers else None
                if tier:
                    hits.append((TA._TIER_RANK[tier], lag, name, tier))
            if not hits:
                return f"no ({len(names)} name(s), largest {max(names.values())})"
            worst = max(hits)
            return f"{worst[3]} ({len(hits)} of {len(names)} names; largest {worst[2]} {worst[1]})"
        if t == RA._MIN_RF_ALERT_TYPE:
            async with B._session() as session:
                fresh = (await session.execute(TA._CLUSTER_STRUCTURE_FRESH_SQL, {
                    "job_ids": [f"{TA._CLUSTER_STRUCTURE_JOB_PREFIX}{cid}"], "since": B._now() - TA._CLUSTER_STRUCTURE_MAX_AGE,
                })).all()
                if not fresh:
                    return "no (ignored: no successful topic-structure run in 15 min)"
                count = int((await session.execute(TA._RF_BELOW_MIN_SQL, {"cid": cid, "min_rf": int(config["min_rf"])})).all()[0][0])
            tier = TA._threshold_tier(config, count)
            return f"{tier or 'no'} (value {count} topics below RF {config['min_rf']})"
        reading = await B.read_value(t, cid)
    except SafetyError:
        return "n/a"
    except Exception as exc:  # noqa: BLE001 -- a failed probe is reported, not fatal
        return f"read failed ({type(exc).__name__})"
    if reading.ignored:
        return f"no (ignored: {reading.ignored})"
    if t == TA._SLO_COMPLIANCE_ALERT_TYPE:
        tier = TA._threshold_tier_below(config, reading.value, strict=True)
    elif t == TA._MSG_RATE_IN_ALERT_TYPE:
        tier = TA._threshold_tier_below(config, reading.value)
    else:
        tier = TA._threshold_tier(config, reading.value)
    return f"{tier or 'no'} (value {reading.value})"


# ── Settings, backup, verification ───────────────────────────────────────────

@dataclass
class Deps:
    settings: B.AgentSettings


def settings_problems(saved: dict) -> list[str]:
    have = saved.get("_rule_outbox_evaluators") or []
    missing = [e for e in REQUIRED_OUTBOX_EVALUATORS if e not in have]
    if missing:
        return [f"rule_outbox_evaluators is {have}: it must list {', '.join(REQUIRED_OUTBOX_EVALUATORS)} "
                f"(missing {', '.join(missing)}), so a failed rule email is retried and can fall back to Teams"]
    return []


def rule_problems(rules: list[dict]) -> list[str]:
    problems = []
    batch = [r["id"] for r in rules if B.is_batch(r)]
    if batch:
        problems.append(f"BATCHTEST rule(s) exist: {batch} (finish or clean up the batch test first)")
    by_id = {r["id"]: r for r in rules}
    for rid, t in EXPECTED_TYPES.items():
        if rid in by_id and by_id[rid]["alert_type"] != t:
            problems.append(f"rule {rid} is {by_id[rid]['alert_type']!r}, expected {t!r}")
    return problems


def backup_dir() -> str:
    return os.environ.get("APPLY_RULES_BACKUP_DIR", DEFAULT_BACKUP_DIR)


def write_backup(rules: list[dict], saved: dict) -> str:
    """Every kafka_alert_configs row (all columns) and the two settings, mode 600."""
    os.makedirs(backup_dir(), mode=0o700, exist_ok=True)
    path = os.path.join(backup_dir(), f"rules-before-apply-{B._now().strftime('%Y%m%dT%H%M%SZ')}.json")
    doc = {
        "created_at": B._now().isoformat(),
        "purpose": "state before scripts/apply_rule_plan.py; restore with --revert <this file> --run",
        "settings": {k: saved.get(k) for k in SETTINGS_KEYS},
        "kafka_alert_configs": rules,
    }
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, json.dumps(doc, indent=1, sort_keys=True).encode("utf-8"))
    finally:
        os.close(fd)
    return path


async def snapshot() -> list[dict]:
    rules = await B.snapshot_rules()
    for r in rules:
        B.MASK.add(str(r.get("webhook_url") or "").strip())
    return rules


class Checker:
    def __init__(self) -> None:
        self.ok = True

    def line(self, good: bool, label: str) -> None:
        self.ok = self.ok and good
        say(f"  {'OK  ' if good else 'FAIL'} {label}")


def _changed_columns(before: dict, after: dict, skip=()) -> list[str]:
    return sorted(k for k in set(before) | set(after) if k not in skip and before.get(k) != after.get(k))


async def verify_apply(deps: Deps, before: list[dict], plan: list[PlanItem], recipients: str) -> bool:
    say("")
    say("VERIFICATION")
    c = Checker()
    after = {r["id"]: r for r in await snapshot()}
    planned = {i.rule["id"]: i for i in plan}
    c.line(set(after) == {r["id"] for r in before}, f"rule ids unchanged ({len(after)} rules)")
    wrong_planned, wrong_other = [], []
    for old in before:
        new = after.get(old["id"])
        if new is None:
            continue
        item = planned.get(old["id"])
        if item is None:
            if new != old:
                wrong_other.append(f"{old['id']} ({', '.join(_changed_columns(old, new))})")
            continue
        problems = []
        if new["enabled"] is not item.enabled:
            problems.append(f"enabled {new['enabled']}")
        if new["email_enabled"] is not True:
            problems.append(f"email_enabled {new['email_enabled']}")
        want_cooldown = item.body.get("cooldown_minutes", old["cooldown_minutes"])
        if new["cooldown_minutes"] != want_cooldown:
            problems.append(f"cooldown_minutes {new['cooldown_minutes']} (want {want_cooldown})")
        want_config = _new_config(item)
        for key in sorted(set(want_config) | set(new.get("config") or {})):
            got, want = (new.get("config") or {}).get(key), want_config.get(key)
            if got != want:
                problems.append(f"config.{key} {got!r} (want {want!r})")
        others = _changed_columns(old, new, skip=("enabled", "email_enabled", "cooldown_minutes", "config", "updated_at"))
        if others:
            problems.append(f"also changed: {', '.join(others)}")
        if problems:
            wrong_planned.append(f"{old['id']}: {'; '.join(problems)}")
    c.line(not wrong_planned, f"{len(plan)} planned rules have the intended enabled, email_enabled and tiers"
           + ("" if not wrong_planned else ": " + " | ".join(wrong_planned)))
    c.line(not wrong_other, f"{len(before) - len(plan)} other rules unchanged in every column"
           + ("" if not wrong_other else ": changed " + ", ".join(wrong_other)))
    try:
        now = await deps.settings.read()
        c.line(now.get("email_enabled") is True, f"setting email_enabled {now.get('email_enabled')!r} (want True)")
        c.line(now.get("email_recipients") == recipients, f"setting email_recipients {now.get('email_recipients')!r} (want {recipients!r})")
    except Exception as exc:  # noqa: BLE001
        c.line(False, f"settings readable from the agent ({type(exc).__name__})")
    say(f"  {'VERIFICATION PASSED' if c.ok else 'VERIFICATION FAILED: see the FAIL lines; undo with --revert <backup> --run'}")
    return c.ok


# ── Apply mode ───────────────────────────────────────────────────────────────

def _fmt(v) -> str:
    return "-" if v is None else str(v)


def print_plan(plan: list[PlanItem], excluded: list[tuple]) -> None:
    say("")
    say(f"PLAN: {len(plan)} rules (Teams/Email before -> after; tier changes from the file; would fire now under the NEW tiers)")
    for item in plan:
        r = item.rule
        say(f"  rule {r['id']:>3} {r['alert_type']:<34} cluster {_fmt(r.get('cluster_id')):>4}  "
            f"Teams {'on' if r['enabled'] else 'off'}->{'on' if item.enabled else 'off'}  "
            f"Email {'on' if r['email_enabled'] else 'off'}->on  would fire now: {item.fire}")
        for col, (old, new) in item.diff.changes.items():
            say(f"      {col}: {_fmt(old)} -> {new}")
        for note in item.diff.ignored:
            say(f"      not applied: {note}")
    if excluded:
        say("")
        say(f"NOT PLANNED: {len(excluded)} rules (left exactly as they are)")
        for d, why in excluded:
            r = d.rule
            changes = "; ".join(f"{c} {_fmt(o)} -> {n}" for c, (o, n) in d.changes.items())
            say(f"  rule {r['id']:>3} {r['alert_type']:<34} cluster {_fmt(r.get('cluster_id')):>4}  {why}"
                + (f"; file differs: {changes}" if changes else ""))
    say("")
    say(f"NOTE: {FIRE_NOTE}.")


def print_summary(plan: list[PlanItem], excluded: list[tuple]) -> None:
    fire = {}
    for item in plan:
        key = item.fire.split(" ")[0]
        fire[key] = fire.get(key, 0) + 1
    say("")
    say("SUMMARY")
    say(f"  planned rules: {len(plan)}; not planned: {len(excluded)}")
    say(f"  with tier/min_rf/cooldown/reminder changes: {sum(1 for i in plan if i.diff.changes)}")
    say(f"  Teams on -> off: {sum(1 for i in plan if i.rule['enabled'] and not i.enabled)}; "
        f"Email off -> on: {sum(1 for i in plan if not i.rule['email_enabled'])}")
    say("  would fire now: " + ", ".join(f"{k} {v}" for k, v in sorted(fire.items())))


async def apply_mode(args, deps: Deps) -> int:
    recipients = check_recipients(args.recipients)
    only, skip = parse_types(args.only, "--only"), parse_types(args.skip, "--skip")
    say(f"APPLY RULE PLAN -- {'RUN (changes rules and settings)' if args.run else 'DRY RUN (reads only; nothing is changed)'}")
    say(f"  started {B._now().isoformat(timespec='seconds')}; file {args.csv}")
    rows = read_csv(args.csv)
    rules = await snapshot()
    saved = await deps.settings.read()
    say(f"  agent {args.agent_url}: email_enabled={saved.get('email_enabled')!r} email_recipients={saved.get('email_recipients')!r} "
        f"-> email_enabled=True email_recipients={recipients!r}; rule_outbox_evaluators={saved.get('_rule_outbox_evaluators')}")
    problems = settings_problems(saved) + rule_problems(rules)
    diffs = diff(rules, rows)
    plan, excluded = build_plan(diffs, only, skip)
    invalid = [f"rule {i.rule['id']} ({i.rule['alert_type']}, cluster {i.rule.get('cluster_id')}) {json.dumps(i.body)}: {why}"
               for i in plan if (why := validate(i))]
    if invalid:
        raise SafetyError("the API would refuse these rows, so nothing is applied:\n  - " + "\n  - ".join(invalid))
    for item in plan:
        item.fire = await would_fire(item)
    print_plan(plan, excluded)
    print_summary(plan, excluded)
    if problems and args.run:
        raise SafetyError("; ".join(problems))
    if not args.run:
        say("  --run would REFUSE: " + "; ".join(problems) if problems else "  safety checks: OK")
        say("")
        say(f"DRY RUN: nothing was changed. --run would write the backup to {backup_dir()}/rules-before-apply-<UTC>.json, "
            "set the two settings, then apply the rules above one at a time.")
        return 0

    path = write_backup(rules, saved)
    say("")
    say(f"backup written: {path} (mode 600)")
    import routes_alerts as RA
    from fastapi import HTTPException
    error = None
    try:
        await deps.settings.write({"email_enabled": True, "email_recipients": recipients})
        say(f"  settings: email_enabled=True email_recipients={recipients!r}")
        for item in plan:
            try:
                await RA.update_alert_config(item.rule["id"], RA.AlertConfigUpdatePayload(**item.body))
            except HTTPException as exc:
                raise RuntimeError(f"rule {item.rule['id']} refused by the API ({exc.status_code}): {exc.detail}")
            say(f"  rule {item.rule['id']} applied")
    except Exception as exc:  # noqa: BLE001 -- verify whatever happened
        error = exc
        say(f"ERROR: {type(exc).__name__}: {exc}; stopped here")
    ok = await verify_apply(deps, rules, plan, recipients)
    say(f"  undo: python scripts/apply_rule_plan.py --revert {path} [--run]")
    return 0 if ok and error is None else 1


# ── Revert mode ──────────────────────────────────────────────────────────────

REVERT_COLUMNS = ("enabled", "email_enabled", "cooldown_minutes", "config", "updated_at")


def load_backup(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError:
        raise SafetyError(f"{path}: backup not found")
    except (OSError, ValueError) as exc:
        raise SafetyError(f"{path}: not a readable backup ({type(exc).__name__})")
    rules = doc.get("kafka_alert_configs") if isinstance(doc, dict) else None
    settings = doc.get("settings") if isinstance(doc, dict) else None
    if not isinstance(rules, list) or not isinstance(settings, dict) or set(settings) != set(SETTINGS_KEYS) \
            or not all(isinstance(r, dict) and isinstance(r.get("id"), int) for r in rules):
        raise SafetyError(f"{path}: not a backup written by apply_rule_plan.py")
    return doc


async def revert_mode(args, deps: Deps) -> int:
    from sqlalchemy import text
    doc = load_backup(args.revert)
    say(f"REVERT FROM {args.revert} -- {'RUN' if args.run else 'DRY RUN (reads only; nothing is changed)'}")
    saved_rules = {r["id"]: r for r in doc["kafka_alert_configs"]}
    for r in saved_rules.values():
        B.MASK.add(str(r.get("webhook_url") or "").strip())
    now_rules = {r["id"]: r for r in await snapshot()}
    missing = sorted(set(saved_rules) - set(now_rules))
    if missing:
        raise SafetyError(f"rule(s) in the backup no longer exist: {missing}; nothing restored")
    extra = sorted(set(now_rules) - set(saved_rules))
    to_restore = []
    for rid, old in sorted(saved_rules.items()):
        cols = _changed_columns(old, now_rules[rid])
        if not cols:
            continue
        foreign = [c for c in cols if c not in REVERT_COLUMNS]
        if foreign:
            raise SafetyError(f"rule {rid}: {', '.join(foreign)} changed since the backup (not something this script "
                              "changes); nothing restored")
        to_restore.append((rid, [c for c in cols if c != "updated_at"]))
    current = await deps.settings.read()
    want = doc["settings"]
    say(f"  rules to restore: {len(to_restore)} of {len(saved_rules)}" + (f"; rules newer than the backup (left alone): {extra}" if extra else ""))
    for rid, cols in to_restore:
        say(f"    rule {rid}: {', '.join(cols) or 'updated_at only'}")
    say(f"  settings now email_enabled={current.get('email_enabled')!r} email_recipients={current.get('email_recipients')!r} "
        f"-> email_enabled={want['email_enabled']!r} email_recipients={want['email_recipients']!r}")
    if not args.run:
        say("")
        say("DRY RUN: nothing was changed. Add --run to restore.")
        return 0
    error = None
    try:
        # Rules first, so no rule is left emailing once the settings change back.
        for rid, _cols in to_restore:
            old = saved_rules[rid]
            async with B._session() as session:
                await session.execute(text(
                    "UPDATE kafka_alert_configs SET enabled = :enabled, email_enabled = :email_enabled, "
                    "cooldown_minutes = :cooldown_minutes, config = CAST(CAST(:config AS text) AS jsonb), "
                    "updated_at = CAST(CAST(:updated_at AS text) AS timestamptz) WHERE id = :id"),
                    {"id": rid, "enabled": old["enabled"], "email_enabled": old["email_enabled"],
                     "cooldown_minutes": old["cooldown_minutes"],
                     "config": None if old["config"] is None else json.dumps(old["config"]),
                     "updated_at": old["updated_at"]})
                await session.commit()
            say(f"  rule {rid} restored")
        await deps.settings.write({k: want[k] for k in SETTINGS_KEYS})
        say("  settings restored")
    except Exception as exc:  # noqa: BLE001
        error = exc
        say(f"ERROR: {type(exc).__name__}: {exc}; stopped here")
    say("")
    say("VERIFICATION")
    c = Checker()
    after = {r["id"]: r for r in await snapshot()}
    diffs = [f"{rid} ({', '.join(_changed_columns(old, after.get(rid) or {}))})"
             for rid, old in sorted(saved_rules.items()) if after.get(rid) != old]
    c.line(not diffs, f"{len(saved_rules)} rules identical to the backup in every column" + (f": differ {', '.join(diffs)}" if diffs else ""))
    try:
        now = await deps.settings.read()
        for k in SETTINGS_KEYS:
            c.line(now.get(k) == want[k], f"setting {k} {now.get(k)!r} (want {want[k]!r})")
    except Exception as exc:  # noqa: BLE001
        c.line(False, f"settings readable from the agent ({type(exc).__name__})")
    say(f"  {'VERIFICATION PASSED' if c.ok else 'VERIFICATION FAILED: see the FAIL lines'}")
    return 0 if c.ok and error is None else 1


# ── Lag offenders ────────────────────────────────────────────────────────────

async def lag_offenders_mode(args, deps: Deps) -> int:
    RA = _types()
    import teams_alerts as TA
    say(f"LAG OFFENDERS (read-only): names whose lag (rows updated in the last {LAG_FRESH_MINUTES} min) is at or above "
        "the cluster's lag rule warning tier")
    names = await B.cluster_names(enabled_only=False)
    rules = [r for r in await snapshot() if r["alert_type"] in RA._LAG_ALERT_TYPES and r.get("cluster_id") is not None]
    totals = {}
    for rule in sorted(rules, key=lambda r: (r["cluster_id"], r["alert_type"])):
        cid, t = rule["cluster_id"], rule["alert_type"]
        generic, custom, blacklist = TA._lag_rule_parts(SimpleNamespace(config=rule.get("config") or {}))
        noun = TA._LAG_NOUNS[t]
        say("")
        head = f"cluster {cid} {names.get(cid, '?')} -- {t} (rule {rule['id']})"
        tier = generic.get("warning")
        tier_note = "warning"
        if tier is None and generic:
            tier_note, tier = min(generic.items(), key=lambda kv: kv[1])
            tier_note += " (no warning tier set)"
        if tier is None:
            say(f"{head}: no generic tiers; skipped")
            continue
        values, ignored = await lag_values(t, cid)
        if values is None:
            say(f"{head}: no data ({ignored})")
            continue
        hits = sorted(((lag, name) for name, lag in values.items() if lag >= tier), key=lambda x: (-x[0], x[1]))
        new = [(lag, n) for lag, n in hits if n not in blacklist and n not in custom]
        say(f"{head}: {tier_note} tier {tier}; {len(hits)} of {len(values)} {noun}s at or above it, "
            f"{len(hits) - len(new)} already blacklisted or custom, {len(new)} to consider")
        for lag, n in hits:
            mark = " (blacklisted)" if n in blacklist else (" (custom tiers)" if n in custom else "")
            say(f"  {lag:>14}  {n}{mark}")
        if new:
            say("  -- paste into the blacklist box (one name per line) --")
            for _lag, n in new:
                say(n)
            say("  -- end --")
        totals[noun] = totals.get(noun, 0) + len(new)
    say("")
    say("TOTAL to consider: " + (", ".join(f"{v} {k}s" for k, v in sorted(totals.items())) or "none"))
    return 0


# ── Entry ────────────────────────────────────────────────────────────────────

async def run(args, deps: Deps) -> int:
    code = 0
    try:
        from config import settings as agent_settings
        from storage import init_storage
        init_storage(agent_settings.agent_slug)
        if args.revert:
            code = await revert_mode(args, deps)
        elif args.list_lag_offenders:
            code = await lag_offenders_mode(args, deps)
        else:
            code = await apply_mode(args, deps)
    except SafetyError as exc:
        say(f"REFUSED: {exc}")
        code = 2
    except Exception:  # noqa: BLE001
        say("ERROR: " + traceback.format_exc())
        code = 1
    finally:
        await deps.settings.close()
    return code


def main(argv=None) -> int:
    B._setup_logging()
    sys.excepthook = B._masked_excepthook
    args = parse_args(argv)
    return asyncio.run(run(args, Deps(settings=B.AgentSettings(args.agent_url))))


if __name__ == "__main__":
    sys.exit(main())
