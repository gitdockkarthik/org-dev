"""APScheduler integration — loads schedules from DB and triggers jobs."""
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from database import SessionLocal
from models import Job, JobSchedule
from engine import execute_job

logger = logging.getLogger(__name__)

_scheduler = AsyncIOScheduler(timezone="UTC")


async def _trigger_job(job_id: str, schedule_id: int) -> None:
    if SessionLocal is None:
        return
    async with SessionLocal() as session:
        job = await session.get(Job, job_id)
        if not job:
            logger.warning("Scheduler: job %s not found", job_id)
            return
    await execute_job(job, schedule_id=schedule_id, triggered_by="schedule")


async def load_schedules() -> int:
    if SessionLocal is None:
        return 0
    _scheduler.remove_all_jobs()
    async with SessionLocal() as session:
        result = await session.execute(
            select(JobSchedule).where(JobSchedule.enabled == True)
        )
        schedules = result.scalars().all()
    count = 0
    for sched in schedules:
        try:
            _scheduler.add_job(
                _trigger_job,
                trigger=CronTrigger.from_crontab(sched.cron_expression, timezone=sched.timezone),
                args=[sched.job_id, sched.id],
                id=f"sched_{sched.id}",
                replace_existing=True,
            )
            count += 1
            logger.info("Scheduled job %s with cron '%s'", sched.job_id, sched.cron_expression)
        except Exception as e:
            logger.warning("Failed to schedule job %s: %s", sched.job_id, e)
    return count


def start_scheduler() -> None:
    _scheduler.start()
    logger.info("Scheduler started")


def stop_scheduler() -> None:
    _scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
