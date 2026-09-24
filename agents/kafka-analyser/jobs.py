"""Self-contained job management for Kafka Analyser.
Multiple jobs for different metric types with independent schedules and timeouts.
"""
import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select, desc, update

from database import SessionLocal
from models import KafkaJobRun, KafkaJobSchedule, KafkaClusterBreakerState

logger = logging.getLogger(__name__)

_scheduler = AsyncIOScheduler(timezone="UTC")
_jobs: dict = {}

_BREAKER_TRACKED_PREFIXES = (
    "kafka-broker-health-",
    "kafka-consumer-lag-",
    "kafka-topic-structure-",
    "kafka-msg-rate-",
    "kafka-topic-inflow-",
)

# All 9 per-cluster job-type prefixes -- broader than _BREAKER_TRACKED_PREFIXES.
# Used only to decide whether a job should be SKIPPED while its cluster is
# paused (every per-cluster job type stops, so nothing computes from
# increasingly stale data during an incident) -- NOT used for deciding
# whether a job's outcome counts toward tripping or resetting the breaker
# (that stays scoped to _BREAKER_TRACKED_PREFIXES, the 5 job types that
# actually make raw broker-protocol connections).
_ALL_PER_CLUSTER_PREFIXES = _BREAKER_TRACKED_PREFIXES + (
    "kafka-topic-sizes-",
    "kafka-connector-snapshots-",
    "kafka-sr-sync-",
    "kafka-slo-compliance-",
)

_BREAKER_TRIP_THRESHOLD = 5


def _parse_any_cluster_job_id(job_id: str) -> int | None:
    """Return the cluster_id if job_id matches ANY of the 9 per-cluster job
    types (not just the 5 breaker-tracked ones). Used only to decide whether
    a job should be skipped while its cluster is paused. Cluster-agnostic
    jobs (kafka-snapshot-rollup, kafka-vacuum-full,
    kafka-breaker-recovery-check) always return None here too, exactly as
    with _parse_breaker_cluster_id -- the recovery-check job must keep
    running even while clusters are paused, or nothing could ever resume."""
    for prefix in _ALL_PER_CLUSTER_PREFIXES:
        if job_id.startswith(prefix):
            suffix = job_id[len(prefix):]
            if suffix.isdigit():
                return int(suffix)
    return None


def _parse_breaker_cluster_id(job_id: str) -> int | None:
    """Return the cluster_id if job_id matches one of the 5 breaker-tracked
    job-type prefixes (broker-health, consumer-lag, topic-structure,
    msg-rate, topic-inflow), else None. Cluster-agnostic jobs
    (kafka-snapshot-rollup, kafka-vacuum-full, the future recovery-check
    job) and non-tracked per-cluster job types (connector-snapshots,
    sr-sync, slo-compliance) always return None and are never paused or
    counted."""
    for prefix in _BREAKER_TRACKED_PREFIXES:
        if job_id.startswith(prefix):
            suffix = job_id[len(prefix):]
            if suffix.isdigit():
                return int(suffix)
    return None


async def _get_breaker_state(cluster_id: int) -> KafkaClusterBreakerState | None:
    if SessionLocal is None:
        return None
    async with SessionLocal() as session:
        result = await session.execute(
            select(KafkaClusterBreakerState).where(
                KafkaClusterBreakerState.cluster_id == cluster_id
            )
        )
        return result.scalar_one_or_none()


async def _record_breaker_outcome(job_id: str, success: bool, error: str | None) -> None:
    """Update circuit-breaker state after a tracked job type's outcome. No-op
    for job_id values that aren't one of the 5 tracked prefixes, and a no-op
    (not a trip/reset) if the cluster is already paused -- once paused, only
    the future recovery-check job clears it, so outcomes while paused
    (which shouldn't normally occur, since trigger_job() intercepts them
    first) don't further mutate consecutive_failures."""
    cluster_id = _parse_breaker_cluster_id(job_id)
    if cluster_id is None or SessionLocal is None:
        return
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(KafkaClusterBreakerState).where(
                    KafkaClusterBreakerState.cluster_id == cluster_id
                )
            )
            state = result.scalar_one_or_none()
            now = datetime.now(timezone.utc)
            if state is None:
                state = KafkaClusterBreakerState(
                    cluster_id=cluster_id,
                    consecutive_failures=0,
                    paused=False,
                    updated_at=now,
                )
                session.add(state)
            if state.paused:
                # Already paused -- only the recovery-check job clears this.
                return
            if success:
                if state.consecutive_failures != 0:
                    state.consecutive_failures = 0
                    state.updated_at = now
            else:
                state.consecutive_failures += 1
                state.updated_at = now
                if state.consecutive_failures >= _BREAKER_TRIP_THRESHOLD:
                    state.paused = True
                    state.paused_at = now
                    state.paused_reason = (error or "unknown error")[:2000]
                    logger.warning(
                        "Circuit breaker TRIPPED for cluster %s after %d consecutive "
                        "broker-connection failures (last: %s) — pausing all job "
                        "dispatch for this cluster until connectivity recovers",
                        cluster_id, state.consecutive_failures, state.paused_reason,
                    )
            await session.commit()
    except Exception as exc:
        logger.warning("_record_breaker_outcome failed for job %s: %s", job_id, exc)


def register_job(job_id: str, name: str, description: str, handler, default_timeout_secs: int = 60) -> None:
    _jobs[job_id] = {
        "id": job_id,
        "name": name,
        "description": description,
        "handler": handler,
        "default_timeout_secs": default_timeout_secs,
    }
    logger.info("Job registered: %s (timeout=%ds)", job_id, default_timeout_secs)


async def _get_active_run(job_id: str) -> KafkaJobRun | None:
    if SessionLocal is None:
        return None
    async with SessionLocal() as session:
        result = await session.execute(
            select(KafkaJobRun).where(KafkaJobRun.job_id == job_id, KafkaJobRun.status == "running")
        )
        return result.scalar_one_or_none()


async def _create_run(job_id: str, triggered_by: str) -> KafkaJobRun:
    async with SessionLocal() as session:
        run = KafkaJobRun(
            job_id=job_id,
            triggered_by=triggered_by,
            status="pending",
            started_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )
        session.add(run)
        await session.commit()
        await session.refresh(run)
        return run


async def _update_run(run_id: int, **kwargs) -> None:
    if SessionLocal is None:
        return
    async with SessionLocal() as session:
        await session.execute(update(KafkaJobRun).where(KafkaJobRun.id == run_id).values(**kwargs))
        await session.commit()


async def trigger_job(job_id: str, triggered_by: str = "manual") -> dict:
    job = _jobs.get(job_id)
    if not job:
        return {"ok": False, "error": f"Job '{job_id}' not found"}
    _breaker_cid = _parse_any_cluster_job_id(job_id)
    if _breaker_cid is not None:
        _breaker_state = await _get_breaker_state(_breaker_cid)
        if _breaker_state is not None and _breaker_state.paused:
            _paused_since = _breaker_state.paused_at.isoformat() if _breaker_state.paused_at else "unknown time"
            _skip_reason = (
                f"Skipped — cluster {_breaker_cid} circuit breaker is paused "
                f"(paused since {_paused_since}, reason: {_breaker_state.paused_reason or 'unknown'}). "
                f"Will resume automatically once connectivity recovery checks succeed."
            )
            _skip_run = await _create_run(job_id, triggered_by)
            await _update_run(
                _skip_run.id, status="skipped",
                ended_at=datetime.now(timezone.utc), duration_seconds=0.0,
                logs=_skip_reason,
            )
            logger.info("Job %s skipped — cluster %s is paused", job_id, _breaker_cid)
            return {"ok": False, "error": _skip_reason, "run_id": _skip_run.id}
    active = await _get_active_run(job_id)
    if active:
        elapsed = (datetime.now(timezone.utc) - active.started_at).total_seconds()
        # Use the real, DB-configured timeout for staleness detection -- NOT the
        # hardcoded registration-time default, which can be badly out of date once
        # a job's timeout has been tuned via the schedule (matches the same lookup
        # pattern already used in _execute_job(), for consistency).
        timeout = job.get("default_timeout_secs", 60)
        try:
            if SessionLocal is not None:
                async with SessionLocal() as session:
                    result = await session.execute(
                        select(KafkaJobSchedule).where(
                            KafkaJobSchedule.job_id == job_id,
                            KafkaJobSchedule.enabled == True,
                        ).limit(1)
                    )
                    sched = result.scalar_one_or_none()
                    if sched and sched.timeout_secs:
                        timeout = sched.timeout_secs
        except Exception:
            pass
        if elapsed > timeout * 2:
            # Stale run — clear it and allow new run
            await _update_run(active.id, status="failed",
                              error_message=f"Cleared stale run (stuck for {elapsed:.0f}s)")
            logger.warning("Cleared stale run %d for job %s (stuck for %.0fs)", active.id, job_id, elapsed)
        else:
            return {"ok": False, "error": f"Job already running (started {elapsed:.0f}s ago)"}
    run = await _create_run(job_id, triggered_by)
    asyncio.create_task(_execute_job(job, run))
    return {"ok": True, "run_id": run.id}


async def _execute_job(job: dict, run: KafkaJobRun) -> None:
    await _update_run(run.id, status="running")
    started = datetime.now(timezone.utc)
    # Use timeout from DB schedule if available, otherwise use job default
    timeout = job.get("default_timeout_secs", 60)
    try:
        if SessionLocal is not None:
            async with SessionLocal() as session:
                result = await session.execute(
                    select(KafkaJobSchedule).where(
                        KafkaJobSchedule.job_id == job["id"],
                        KafkaJobSchedule.enabled == True,
                    ).limit(1)
                )
                sched = result.scalar_one_or_none()
                if sched and sched.timeout_secs:
                    timeout = sched.timeout_secs
    except Exception:
        pass
    try:
        logger.info("Job %s starting (run_id=%d, timeout=%ds)", job["id"], run.id, timeout)
        await asyncio.wait_for(job["handler"](), timeout=timeout)
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        log_msg = f"Completed in {duration:.1f}s"
        await _record_breaker_outcome(job["id"], success=True, error=None)
        await _update_run(run.id, status="success", ended_at=ended, duration_seconds=duration, logs=log_msg)
        logger.info("Job %s completed in %.1fs", job["id"], duration)
    except asyncio.TimeoutError:
        logger.warning("Job %s timed out after %ds — retrying once", job["id"], timeout)
        retry_started = datetime.now(timezone.utc)
        try:
            await asyncio.wait_for(job["handler"](), timeout=timeout)
            ended = datetime.now(timezone.utc)
            duration = (ended - started).total_seconds()
            retry_duration = (ended - retry_started).total_seconds()
            log_msg = f"Retry succeeded in {retry_duration:.1f}s (total since first attempt: {duration:.1f}s)"
            await _record_breaker_outcome(job["id"], success=True, error=None)
            await _update_run(run.id, status="success", ended_at=ended, duration_seconds=duration, logs=log_msg)
            logger.info("Job %s retry succeeded in %.1fs (total since first attempt: %.1fs)", job["id"], retry_duration, duration)
        except Exception as retry_exc:
            ended = datetime.now(timezone.utc)
            duration = (ended - started).total_seconds()
            await _record_breaker_outcome(job["id"], success=False, error=f"Timed out after {timeout}s (retry also failed: {retry_exc})")
            await _update_run(run.id, status="failed", ended_at=ended, duration_seconds=duration,
                             error_message=f"Timed out after {timeout}s (retry also failed: {retry_exc})",
                             logs=f"Job timed out after {timeout}s — retry failed")
            logger.error("Job %s timed out and retry failed: %s", job["id"], retry_exc)
    except Exception as e:
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        await _record_breaker_outcome(job["id"], success=False, error=str(e))
        await _update_run(run.id, status="failed", ended_at=ended, duration_seconds=duration,
                         error_message=str(e), logs=f"Failed after {duration:.1f}s: {e}")
        logger.error("Job %s failed: %s", job["id"], e)


async def load_schedules() -> int:
    if SessionLocal is None:
        return 0
    _scheduler.remove_all_jobs()
    async with SessionLocal() as session:
        result = await session.execute(select(KafkaJobSchedule).where(KafkaJobSchedule.enabled == True))
        schedules = result.scalars().all()
    count = 0
    for sched in schedules:
        try:
            _scheduler.add_job(
                _schedule_trigger,
                trigger=CronTrigger.from_crontab(sched.cron_expression, timezone="UTC"),
                args=[sched.job_id, sched.id],
                id=f"sched_{sched.id}",
                replace_existing=True,
            )
            count += 1
            logger.info("Scheduled job %s: %s", sched.job_id, sched.cron_expression)
        except Exception as e:
            logger.warning("Failed to schedule job %s: %s", sched.job_id, e)
    return count


async def _schedule_trigger(job_id: str, schedule_id: int) -> None:
    await trigger_job(job_id, triggered_by="schedule")


async def get_runs(job_id: str | None = None, limit: int = 50, offset: int = 0, status: str | None = None, job_type: str | None = None) -> list:
    if SessionLocal is None:
        return []
    async with SessionLocal() as session:
        q = select(KafkaJobRun).order_by(desc(KafkaJobRun.created_at)).limit(limit).offset(offset)
        if job_id:
            q = q.where(KafkaJobRun.job_id == job_id)
        if status:
            q = q.where(KafkaJobRun.status == status)
        if job_type:
            q = q.where(KafkaJobRun.job_id.like(f'kafka-{job_type}-%'))
        result = await session.execute(q)
        runs = result.scalars().all()
    return [{"id": r.id, "job_id": r.job_id, "status": r.status, "triggered_by": r.triggered_by,
             "started_at": r.started_at.isoformat() if r.started_at else None,
             "ended_at": r.ended_at.isoformat() if r.ended_at else None,
             "duration_seconds": r.duration_seconds, "logs": r.logs,
             "error_message": r.error_message} for r in runs]


async def get_schedules(job_id: str | None = None) -> list:
    if SessionLocal is None:
        return []
    async with SessionLocal() as session:
        q = select(KafkaJobSchedule)
        if job_id:
            q = q.where(KafkaJobSchedule.job_id == job_id)
        result = await session.execute(q)
        scheds = result.scalars().all()
    return [{"id": s.id, "job_id": s.job_id, "cron_expression": s.cron_expression,
             "enabled": s.enabled, "timeout_secs": s.timeout_secs,
             "created_at": s.created_at.isoformat()} for s in scheds]


async def create_schedule(job_id: str, cron_expression: str, enabled: bool = True, timeout_secs: int = 60) -> dict:
    if SessionLocal is None:
        return {"ok": False, "error": "DB not configured"}
    async with SessionLocal() as session:
        sched = KafkaJobSchedule(
            job_id=job_id,
            cron_expression=cron_expression,
            enabled=enabled,
            timeout_secs=timeout_secs,
            created_at=datetime.now(timezone.utc),
        )
        session.add(sched)
        await session.commit()
        await session.refresh(sched)
    await load_schedules()
    return {"ok": True, "id": sched.id}


async def update_schedule(schedule_id: int, cron_expression: str, enabled: bool, timeout_secs: int = 60) -> dict:
    if SessionLocal is None:
        return {"ok": False}
    async with SessionLocal() as session:
        await session.execute(
            update(KafkaJobSchedule).where(KafkaJobSchedule.id == schedule_id)
            .values(cron_expression=cron_expression, enabled=enabled, timeout_secs=timeout_secs)
        )
        await session.commit()
    await load_schedules()
    return {"ok": True}


async def delete_schedule(schedule_id: int) -> dict:
    if SessionLocal is None:
        return {"ok": False}
    async with SessionLocal() as session:
        sched = await session.get(KafkaJobSchedule, schedule_id)
        if sched:
            await session.delete(sched)
            await session.commit()
    await load_schedules()
    return {"ok": True}


def start_scheduler() -> None:
    _scheduler.start()
    logger.info("Job scheduler started")


def stop_scheduler() -> None:
    _scheduler.shutdown(wait=False)
