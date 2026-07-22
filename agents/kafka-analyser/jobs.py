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
from models import KafkaJobRun, KafkaJobSchedule

logger = logging.getLogger(__name__)

_scheduler = AsyncIOScheduler(timezone="UTC")
_jobs: dict = {}


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
    active = await _get_active_run(job_id)
    if active:
        elapsed = (datetime.now(timezone.utc) - active.started_at).total_seconds()
        return {"ok": False, "error": f"Job already running (started {elapsed:.0f}s ago)"}
    run = await _create_run(job_id, triggered_by)
    asyncio.create_task(_execute_job(job, run))
    return {"ok": True, "run_id": run.id}


async def _execute_job(job: dict, run: KafkaJobRun) -> None:
    await _update_run(run.id, status="running")
    started = datetime.now(timezone.utc)
    timeout = job.get("default_timeout_secs", 60)
    try:
        logger.info("Job %s starting (run_id=%d, timeout=%ds)", job["id"], run.id, timeout)
        await asyncio.wait_for(job["handler"](), timeout=timeout)
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        result_note = getattr(job["handler"], '_last_result', None)
        log_msg = f"{result_note} (completed in {duration:.1f}s)" if result_note else f"Completed in {duration:.1f}s"
        await _update_run(run.id, status="success", ended_at=ended, duration_seconds=duration, logs=log_msg)
        logger.info("Job %s completed in %.1fs", job["id"], duration)
    except asyncio.TimeoutError:
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        await _update_run(run.id, status="failed", ended_at=ended, duration_seconds=duration,
                         error_message=f"Timed out after {timeout}s",
                         logs=f"Job timed out after {timeout}s")
        logger.error("Job %s timed out after %ds", job["id"], timeout)
    except Exception as e:
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
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


async def get_runs(job_id: str | None = None, limit: int = 50) -> list:
    if SessionLocal is None:
        return []
    async with SessionLocal() as session:
        q = select(KafkaJobRun).order_by(desc(KafkaJobRun.created_at)).limit(limit)
        if job_id:
            q = q.where(KafkaJobRun.job_id == job_id)
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
