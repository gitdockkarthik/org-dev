"""Broker restart capture (step 1 of the "broker restarted" alert: recording only).

decide_restart compares the last stored broker JVM start with the one a scrape
just returned; record_broker_restarts applies it per broker inside collect_broker_health
and writes kafka_broker_metrics.process_start_time and kafka_broker_restarts
(Alembic 0059). Nothing here can fail a collection: every broker runs in its
own savepoint and every error is logged and swallowed, so the metrics upsert
and a database that has not had 0059 applied yet behave exactly as before.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Starts closer than this are the same process (clock and rounding jitter).
RESTART_TOLERANCE_SECS = 5.0


def decide_restart(stored: float | None, new: float | None, now: float | None = None):
    """(value_to_store, event or None, warning or None); times are epoch seconds.

    new None             -> keep stored, no event
    stored None          -> store new as the baseline, no event
    new later  by > 5 s  -> store new, event {started_at, previous_started_at}
    within 5 s either way-> keep stored
    new earlier by > 5 s -> keep stored, warning
    `now` is unused today (kept for the caller's clock and later steps)."""
    if new is None:
        return stored, None, None
    if stored is None:
        return new, None, None
    delta = new - stored
    if delta > RESTART_TOLERANCE_SECS:
        return new, {"started_at": new, "previous_started_at": stored}, None
    if delta < -RESTART_TOLERANCE_SECS:
        return stored, None, (
            f"scraped start {_iso(new)} is {-delta:.0f} s earlier than the stored start {_iso(stored)}; "
            "keeping the stored value"
        )
    return stored, None, None


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


_STORED_SQL = text(
    "SELECT EXTRACT(EPOCH FROM process_start_time) AS epoch FROM kafka_broker_metrics "
    "WHERE cluster_id = :cid AND broker_id = :bid"
)
_STORE_SQL = text(
    "UPDATE kafka_broker_metrics SET process_start_time = to_timestamp(:v) "
    "WHERE cluster_id = :cid AND broker_id = :bid"
)
_EVENT_SQL = text(
    "INSERT INTO kafka_broker_restarts (cluster_id, broker_id, started_at, previous_started_at) "
    "VALUES (:cid, :bid, to_timestamp(:started), to_timestamp(:previous)) "
    "ON CONFLICT (cluster_id, broker_id, started_at) DO NOTHING"
)


async def record_broker_restarts(sess, cluster_id: int, brokers: list[dict[str, Any]], cluster_name: str = "") -> int:
    """Apply decide_restart to every scraped broker; returns the events inserted.

    Call after the kafka_broker_metrics upsert, in the same session, so each
    broker's row exists (a brand-new broker gets its baseline here, no event).
    The stored value is read and written by its own statements, never by the
    metrics upsert: a missing process_start_time column only fails this
    broker's savepoint. A broker without a numeric id or without a start time
    is skipped. Never raises."""
    recorded = 0
    label = cluster_name or str(cluster_id)
    for broker in brokers:
        new = broker.get("process_start_time")
        bid = str(broker.get("broker_id") or broker.get("id") or "")
        if new is None or not bid.isdigit():
            continue
        try:
            async with sess.begin_nested():
                row = (await sess.execute(_STORED_SQL, {"cid": int(cluster_id), "bid": bid})).first()
                if row is None:
                    continue  # no metrics row for this broker (the upsert failed): nothing to compare
                stored = float(row.epoch) if row.epoch is not None else None
                value, event, warning = decide_restart(stored, new, None)
                if value is not None and value != stored:
                    await sess.execute(_STORE_SQL, {"v": value, "cid": int(cluster_id), "bid": bid})
                if event is not None:
                    result = await sess.execute(_EVENT_SQL, {
                        "cid": int(cluster_id), "bid": int(bid),
                        "started": event["started_at"], "previous": event["previous_started_at"],
                    })
                    if result.rowcount:
                        recorded += 1
                        logger.info(
                            "broker restart detected: cluster %s, broker %s, started %s, previous %s",
                            label, bid, _iso(event["started_at"]), _iso(event["previous_started_at"]),
                        )
            if warning:
                logger.warning("broker start time went backwards: cluster %s, broker %s: %s", label, bid, warning)
        except Exception as exc:
            logger.warning("broker restart capture failed for cluster %s, broker %s: %s", label, bid, exc)
    return recorded
