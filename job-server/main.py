"""Operative Job Server — manages and executes scheduled jobs for all agents."""
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import select, desc

from config import settings
from database import SessionLocal, engine
from models import Base, Job, JobSchedule, JobRun
from engine import execute_job
from scheduler import load_schedules, start_scheduler, stop_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if engine is not None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("DB tables ensured")
    count = await load_schedules()
    logger.info("Loaded %d schedule(s)", count)
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(title="Operative Job Server", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def index():
    from pathlib import Path
    html = Path("static/index.html").read_text()
    return HTMLResponse(content=html)


def _check_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=403, detail="Invalid API key")


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    from sqlalchemy import text
    if SessionLocal is None:
        return JSONResponse(status_code=503, content={"status": "error", "reason": "database not configured"})
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok", "service": "job-server"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "reason": str(e)})


# ── Job Definitions ───────────────────────────────────────────────────────────
class JobCreate(BaseModel):
    id: str
    name: str
    owner_agent: str
    endpoint: str
    timeout_seconds: int = 300
    retry_on_failure: bool = True
    max_retries: int = 2
    description: str = ""


@app.get("/jobs")
async def list_jobs():
    if SessionLocal is None:
        return []
    async with SessionLocal() as session:
        result = await session.execute(select(Job))
        jobs = result.scalars().all()
    return [{"id": j.id, "name": j.name, "owner_agent": j.owner_agent,
             "endpoint": j.endpoint, "timeout_seconds": j.timeout_seconds,
             "description": j.description,
             "created_at": j.created_at.isoformat()} for j in jobs]


@app.post("/jobs", status_code=201)
async def create_job(body: JobCreate):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        existing = await session.get(Job, body.id)
        if existing:
            raise HTTPException(status_code=409, detail=f"Job '{body.id}' already exists")
        job = Job(**body.model_dump())
        session.add(job)
        await session.commit()
    return {"ok": True, "id": body.id}


@app.put("/jobs/{job_id}")
async def update_job(job_id: str, body: JobCreate):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        job = await session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        for k, v in body.model_dump().items():
            setattr(job, k, v)
        await session.commit()
    return {"ok": True}


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        job = await session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        await session.delete(job)
        await session.commit()
    await load_schedules()
    return {"ok": True}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        job = await session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        scheds = await session.execute(select(JobSchedule).where(JobSchedule.job_id == job_id))
        schedules = scheds.scalars().all()
        runs = await session.execute(
            select(JobRun).where(JobRun.job_id == job_id).order_by(desc(JobRun.created_at)).limit(10)
        )
        recent_runs = runs.scalars().all()
    return {
        "id": job.id, "name": job.name, "owner_agent": job.owner_agent,
        "endpoint": job.endpoint, "timeout_seconds": job.timeout_seconds,
        "description": job.description,
        "schedules": [{"id": s.id, "cron": s.cron_expression, "enabled": s.enabled,
                       "timezone": s.timezone} for s in schedules],
        "recent_runs": [{"id": r.id, "status": r.status, "triggered_by": r.triggered_by,
                         "started_at": r.started_at.isoformat() if r.started_at else None,
                         "duration_seconds": r.duration_seconds,
                         "error_message": r.error_message} for r in recent_runs],
    }


@app.post("/jobs/{job_id}/run", status_code=202)
async def trigger_job(job_id: str):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        job = await session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    run = await execute_job(job, schedule_id=None, triggered_by="manual")
    return {"ok": True, "run_id": run.id, "status": run.status}


# ── Schedules ─────────────────────────────────────────────────────────────────
class ScheduleCreate(BaseModel):
    cron_expression: str
    timezone: str = "UTC"
    enabled: bool = True
    parameters: dict = {}


@app.get("/jobs/{job_id}/schedules")
async def list_schedules(job_id: str):
    if SessionLocal is None:
        return []
    async with SessionLocal() as session:
        result = await session.execute(select(JobSchedule).where(JobSchedule.job_id == job_id))
        scheds = result.scalars().all()
    return [{"id": s.id, "cron": s.cron_expression, "timezone": s.timezone,
             "enabled": s.enabled, "parameters": s.parameters} for s in scheds]


@app.post("/jobs/{job_id}/schedules", status_code=201)
async def create_schedule(job_id: str, body: ScheduleCreate):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        job = await session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        sched = JobSchedule(
            job_id=job_id,
            cron_expression=body.cron_expression,
            timezone=body.timezone,
            enabled=body.enabled,
            parameters=json.dumps(body.parameters),
        )
        session.add(sched)
        await session.commit()
        await session.refresh(sched)
    await load_schedules()
    return {"ok": True, "id": sched.id}


@app.put("/jobs/{job_id}/schedules/{schedule_id}")
async def update_schedule(job_id: str, schedule_id: int, body: ScheduleCreate):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        sched = await session.get(JobSchedule, schedule_id)
        if not sched or sched.job_id != job_id:
            raise HTTPException(status_code=404, detail="Schedule not found")
        sched.cron_expression = body.cron_expression
        sched.timezone = body.timezone
        sched.enabled = body.enabled
        sched.parameters = json.dumps(body.parameters)
        await session.commit()
    await load_schedules()
    return {"ok": True}


@app.delete("/jobs/{job_id}/schedules/{schedule_id}")
async def delete_schedule(job_id: str, schedule_id: int):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        sched = await session.get(JobSchedule, schedule_id)
        if not sched or sched.job_id != job_id:
            raise HTTPException(status_code=404, detail="Schedule not found")
        await session.delete(sched)
        await session.commit()
    await load_schedules()
    return {"ok": True}


# ── Monitor ───────────────────────────────────────────────────────────────────
@app.get("/runs")
async def list_runs(limit: int = 50, job_id: str | None = None, status: str | None = None):
    if SessionLocal is None:
        return []
    async with SessionLocal() as session:
        q = select(JobRun).order_by(desc(JobRun.created_at)).limit(limit)
        if job_id:
            q = q.where(JobRun.job_id == job_id)
        if status:
            q = q.where(JobRun.status == status)
        result = await session.execute(q)
        runs = result.scalars().all()
    return [{"id": r.id, "job_id": r.job_id, "status": r.status,
             "triggered_by": r.triggered_by, "attempt_number": r.attempt_number,
             "started_at": r.started_at.isoformat() if r.started_at else None,
             "ended_at": r.ended_at.isoformat() if r.ended_at else None,
             "duration_seconds": r.duration_seconds,
             "error_message": r.error_message} for r in runs]


@app.get("/runs/{run_id}")
async def get_run(run_id: int):
    if SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with SessionLocal() as session:
        run = await session.get(JobRun, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
    return {"id": run.id, "job_id": run.job_id, "status": run.status,
            "triggered_by": run.triggered_by, "attempt_number": run.attempt_number,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "ended_at": run.ended_at.isoformat() if run.ended_at else None,
            "duration_seconds": run.duration_seconds,
            "logs": run.logs, "error_message": run.error_message}
