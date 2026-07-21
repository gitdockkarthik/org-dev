"""Job execution engine — overlap check, HTTP call, status tracking."""
import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import select, update

from database import SessionLocal
from models import Job, JobRun

logger = logging.getLogger(__name__)


async def _get_active_run(job_id: str) -> JobRun | None:
    if SessionLocal is None:
        return None
    async with SessionLocal() as session:
        result = await session.execute(
            select(JobRun).where(JobRun.job_id == job_id, JobRun.status == "running")
        )
        return result.scalar_one_or_none()


async def _create_run(job_id: str, schedule_id: int | None, triggered_by: str, attempt: int = 1) -> JobRun:
    async with SessionLocal() as session:
        run = JobRun(
            job_id=job_id,
            schedule_id=schedule_id,
            triggered_by=triggered_by,
            status="pending",
            attempt_number=attempt,
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
        await session.execute(update(JobRun).where(JobRun.id == run_id).values(**kwargs))
        await session.commit()


async def execute_job(job: Job, schedule_id: int | None = None, triggered_by: str = "schedule") -> JobRun:
    """Execute a job with overlap check and timeout detection."""
    active = await _get_active_run(job.id)
    if active:
        if active.started_at:
            elapsed = (datetime.now(timezone.utc) - active.started_at).total_seconds()
            if elapsed > job.timeout_seconds:
                logger.warning("Job %s timed out after %ds — marking failed", job.id, elapsed)
                await _update_run(
                    active.id,
                    status="failed",
                    ended_at=datetime.now(timezone.utc),
                    duration_seconds=elapsed,
                    error_message=f"Timeout after {elapsed:.0f}s (limit: {job.timeout_seconds}s)",
                )
            else:
                logger.info("Job %s skipped — previous run still active (run_id=%d)", job.id, active.id)
                run = await _create_run(job.id, schedule_id, triggered_by)
                await _update_run(
                    run.id,
                    status="skipped",
                    started_at=datetime.now(timezone.utc),
                    ended_at=datetime.now(timezone.utc),
                    duration_seconds=0,
                    logs=f"Skipped: run {active.id} still active",
                )
                return run

    run = await _create_run(job.id, schedule_id, triggered_by)
    asyncio.create_task(_run_job(job, run))
    return run


async def _run_job(job: Job, run: JobRun, attempt: int = 1) -> None:
    started = datetime.now(timezone.utc)
    await _update_run(run.id, status="running", started_at=started)
    logs = [f"[{started.isoformat()}] Starting job {job.id} (attempt {attempt})"]
    try:
        async with httpx.AsyncClient(timeout=job.timeout_seconds) as client:
            resp = await client.post(job.endpoint, json={"job_run_id": run.id, "triggered_by": run.triggered_by})
            resp.raise_for_status()
            logs.append(f"Response: {resp.text[:500]}")
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        logs.append(f"[{ended.isoformat()}] Completed in {duration:.1f}s")
        await _update_run(
            run.id, status="success", ended_at=ended,
            duration_seconds=duration, logs="\n".join(logs),
        )
        logger.info("Job %s completed in %.1fs", job.id, duration)
    except Exception as e:
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        logs.append(f"[{ended.isoformat()}] Error: {e}")
        logger.error("Job %s failed: %s", job.id, e)
        if job.retry_on_failure and attempt < job.max_retries:
            logs.append(f"Retrying (attempt {attempt + 1}/{job.max_retries})...")
            await _update_run(run.id, status="retrying", logs="\n".join(logs))
            await asyncio.sleep(30)
            await _run_job(job, run, attempt + 1)
        else:
            await _update_run(
                run.id, status="failed", ended_at=ended,
                duration_seconds=duration, logs="\n".join(logs),
                error_message=str(e),
            )
