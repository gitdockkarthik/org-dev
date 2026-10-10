"""Proves an owner notice's INFO lines reach the container's stderr with the
agent's real logging setup (main.py's basicConfig), naming the channel, the
email result or error and the attempt durations.

Host side (default): starts a throwaway container from the agent image with
the working tree mounted read-only and --network none, runs this file inside
it, and checks the container's stderr. Inside the container (OWNER_LOG_CHILD=1)
it imports main, starts a fake SMTP server and a fake Teams webhook on
127.0.0.1, and sends the health summary twice: SMTP up (email), then SMTP down
(Teams fallback). No database: DATABASE_URL is unset, so nothing is recorded.

Run from the repo root:  python3 agents/kafka-analyser/tests/test_owner_notice_log.py
Env: DOCKER (default "docker", e.g. "sudo -n docker"), KAFKA_IMAGE
(default org-dev-kafka-analyser:latest)."""
import os
import re
import shlex
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(AGENT))

EXPECTED = [
    # Scenario 1: SMTP up.
    r"^\S+ \S+ INFO shared\.escalation\.email_notifier — notify_owner: channel=email ok=True "
    r"email=sent in \d+ ms teams=skipped \(fallback not needed: email sent\)$",
    r"^\S+ \S+ INFO collectors — owner notice \(health summary\): channel=email ok=True "
    r"email=sent in \d+ ms teams=skipped \(fallback not needed: email sent\)$",
    # Scenario 2: SMTP down, Teams fallback.
    r"^\S+ \S+ INFO shared\.escalation\.email_notifier — notify_owner: channel=teams ok=True "
    r"email=failed in \d+ ms \(connection refused\) teams=sent in \d+ ms$",
    r"^\S+ \S+ INFO collectors — owner notice \(health summary\): channel=teams ok=True "
    r"email=failed in \d+ ms \(connection refused\) teams=sent in \d+ ms$",
]


def child():
    import asyncio
    from datetime import datetime, timezone

    sys.path.insert(0, HERE)
    import main  # noqa: F401 -- the agent's real logging configuration
    import collectors
    from _fakes import FakeSMTP, FakeTeams
    from routes_settings import _config

    smtp, teams = FakeSMTP(), FakeTeams()
    _config.update({
        "teams_enabled": True, "teams_webhook_url": teams.url,
        "smtp_host": "127.0.0.1", "smtp_port": smtp.port, "smtp_from_address": "alerts@sintecmedia.com",
        "health_email_recipients": "owner@operative.com",
    })

    async def gather():
        return datetime.now(timezone.utc), "alive", [{"name": "c1", "data_age_minutes": 1.0}]
    collectors._gather_health_summary = gather

    async def run():
        await collectors._send_health_summary(9)
        smtp.down()
        await collectors._send_health_summary(9)

    asyncio.run(run())
    assert len(smtp.messages) == 1 and len(teams.cards) == 1, (len(smtp.messages), len(teams.cards))
    print("CHILD DONE: 1 email, 1 Teams card")


def host():
    docker = shlex.split(os.environ.get("DOCKER", "docker"))
    image = os.environ.get("KAFKA_IMAGE", "org-dev-kafka-analyser:latest")
    cmd = docker + [
        "run", "--rm", "--network", "none", "-e", "OWNER_LOG_CHILD=1", "-e", "PYTHONDONTWRITEBYTECODE=1",
        "-e", "PYTHONPATH=/app:/opt", "-v", f"{AGENT}:/app:ro", "-v", f"{REPO}/shared:/opt/shared:ro",
        "-w", "/app", image, "python", "-B", "tests/test_owner_notice_log.py",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    stderr_lines = proc.stderr.splitlines()
    print(f"container exit={proc.returncode}; stdout: {proc.stdout.strip()}")
    assert proc.returncode == 0, proc.stderr[-3000:]
    assert "CHILD DONE" in proc.stdout
    for pattern in EXPECTED:
        hits = [line for line in stderr_lines if re.match(pattern, line)]
        assert len(hits) == 1, f"expected one stderr line matching {pattern!r}, got {hits}\n" + proc.stderr[-3000:]
        print(f"PASS stderr: {hits[0]}")
    assert not any("owner notice" in line for line in proc.stdout.splitlines()), "the line went to stdout"
    print(f"\n{len(EXPECTED)} checks passed")


if __name__ == "__main__":
    child() if os.environ.get("OWNER_LOG_CHILD") == "1" else host()
