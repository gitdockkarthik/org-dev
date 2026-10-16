"""Broker restart capture, step 1: the real collectors.collect_broker_health against a
SCRATCH Postgres (the live schema plus Alembic 0059): baseline, same value, a later
value (exactly one event), a repeat, a missing value, and the stored value kept when
the new one is None. Only the Kafka admin and the Prometheus scrape are replaced.
Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates
kafka_broker_metrics and kafka_broker_restarts):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \\
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \\
    -e OUTBOX_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_broker_restart_db.py
"""
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

from sqlalchemy import text  # noqa: E402

import broker_restarts  # noqa: E402
import collectors  # noqa: E402
import tools.prometheus_collector as pc  # noqa: E402
from database import SessionLocal, engine  # noqa: E402

PASSED = []
CID = 1
T0 = 1.792092709056e9
SCRAPE = {"1": T0, "2": T0 + 100}  # broker id -> process_start_time returned by the next scrape (None = missing)


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append((record.levelno, record.getMessage()))


CAP = Capture()
logging.getLogger("broker_restarts").addHandler(CAP)
logging.getLogger("broker_restarts").setLevel(logging.INFO)


def check(name, cond, detail=""):
    assert cond, f"FAIL {name} {detail}"
    PASSED.append(name)
    print(f"PASS {name}")


async def sql(statement, **params):
    async with SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


async def _clusters():
    return [{"id": CID, "name": "scratch-1", "enabled": True, "prometheus_port": 7071, "bootstrap_servers": "x:9092"}]


class FakeCollector:
    async def collect_brokers_only(self):
        return [{"broker_id": "1", "host": "b1"}, {"broker_id": "2", "host": "b2"}]

    async def collect_broker_log_dir_sizes(self):
        return {"broker_sizes_gb": {}}


async def _get_collector(c):
    return FakeCollector()


async def _scrape_all(brokers, port, cpu_cores=None, per_broker_timeout=60.0):
    return {b["broker_id"]: {"heap_pct": 10.0, "cpu_pct": 5.0, "process_start_time": SCRAPE[b["broker_id"]]} for b in brokers}


collectors._get_enabled_clusters = _clusters
collectors._get_collector = _get_collector
pc.scrape_all_brokers = _scrape_all


async def run(b1, b2="same"):
    SCRAPE["1"] = b1
    if b2 != "same":
        SCRAPE["2"] = b2
    CAP.records.clear()
    await collectors.collect_broker_health(str(CID))


async def state(bid="1"):
    row = (await sql("SELECT EXTRACT(EPOCH FROM process_start_time) AS e, heap_pct FROM kafka_broker_metrics "
                     "WHERE cluster_id = :c AND broker_id = :b", c=CID, b=bid)).first()
    return None if row is None else (None if row.e is None else float(row.e), row.heap_pct)


async def events():
    return (await sql("SELECT broker_id, EXTRACT(EPOCH FROM started_at) AS s, EXTRACT(EPOCH FROM previous_started_at) AS p "
                      "FROM kafka_broker_restarts WHERE cluster_id = :c ORDER BY id", c=CID)).all()


async def main():
    await sql("TRUNCATE kafka_broker_metrics, kafka_broker_restarts RESTART IDENTITY")

    await run(T0, T0 + 100)
    s = await state("1")
    check("baseline stored, no event", s is not None and abs(s[0] - T0) < 1e-3 and not await events(), s)
    check("metrics upsert still written", s[1] == 10.0)
    check("second broker baseline", abs((await state("2"))[0] - (T0 + 100)) < 1e-3)

    await run(T0)
    check("same value: no event, stored unchanged", not await events() and abs((await state("1"))[0] - T0) < 1e-3)

    await run(T0 + 3)
    check("3 s jitter: no event, stored unchanged", not await events() and abs((await state("1"))[0] - T0) < 1e-3)

    await run(T0 + 3600)
    ev = await events()
    check("later value: exactly one event", len(ev) == 1 and ev[0].broker_id == 1
          and abs(ev[0].s - (T0 + 3600)) < 1e-3 and abs(ev[0].p - T0) < 1e-3, ev)
    check("stored value moved", abs((await state("1"))[0] - (T0 + 3600)) < 1e-3)
    check("INFO log written", any(l == logging.INFO and "broker restart detected: cluster scratch-1, broker 1" in m
                                  for l, m in CAP.records), CAP.records)

    await run(T0 + 3600)
    check("repeat of that value: no second event", len(await events()) == 1)

    # The same event delivered again (a second collector racing, or a replay) is absorbed by the unique index.
    async with SessionLocal() as sess:
        n = await broker_restarts.record_broker_restarts(
            sess, CID, [{"broker_id": "1", "process_start_time": T0 + 3600}], "scratch-1")
        await sess.commit()
    check("replayed value records nothing", n == 0 and len(await events()) == 1)
    await sql("UPDATE kafka_broker_metrics SET process_start_time = to_timestamp(:v) WHERE cluster_id = :c AND broker_id = '1'",
              v=T0, c=CID)
    async with SessionLocal() as sess:
        n = await broker_restarts.record_broker_restarts(
            sess, CID, [{"broker_id": "1", "process_start_time": T0 + 3600}], "scratch-1")
        await sess.commit()
    check("same event after a stored rewind: ON CONFLICT DO NOTHING, no second row", n == 0 and len(await events()) == 1)
    await sql("UPDATE kafka_broker_metrics SET process_start_time = to_timestamp(:v) WHERE cluster_id = :c AND broker_id = '1'",
              v=T0 + 3600, c=CID)

    await run(None)
    s = await state("1")
    check("missing value: stored kept, no event", abs(s[0] - (T0 + 3600)) < 1e-3 and len(await events()) == 1, s)
    check("missing value: other metrics still upserted", s[1] == 10.0)

    # The metrics upsert itself keeps the stored start (it never names the column).
    SCRAPE["1"] = None
    await collectors.collect_broker_health(str(CID))
    check("upsert keeps the old value when the new is None", abs((await state("1"))[0] - (T0 + 3600)) < 1e-3)

    await run(T0 + 1)
    check("earlier start: stored kept, no event, WARNING logged",
          abs((await state("1"))[0] - (T0 + 3600)) < 1e-3 and len(await events()) == 1
          and any(l == logging.WARNING and "went backwards" in m for l, m in CAP.records), CAP.records)

    # A failing event insert must not fail the collection.
    await sql("ALTER TABLE kafka_broker_restarts RENAME TO kafka_broker_restarts_x")
    try:
        await run(T0 + 7200)
        s = await state("1")
        check("event insert failure: collection completed, metrics written", s is not None and s[1] == 10.0, s)
        check("event insert failure: WARNING logged, stored value not advanced",
              any(l == logging.WARNING and "capture failed" in m for l, m in CAP.records)
              and abs(s[0] - (T0 + 3600)) < 1e-3, CAP.records)
    finally:
        await sql("ALTER TABLE kafka_broker_restarts_x RENAME TO kafka_broker_restarts")

    await run(T0 + 7200)
    check("after the table is back, the later value is recorded", len(await events()) == 2)

    await engine.dispose()
    print(f"\n{len(PASSED)} checks passed")


asyncio.run(main())
