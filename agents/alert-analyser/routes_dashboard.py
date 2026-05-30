from fastapi import APIRouter

from report_store import get_latest_stats, get_latest_meta

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
async def get_dashboard() -> dict:
    """Return precomputed stats for the most recently uploaded/generated report."""
    stats = get_latest_stats()
    if stats is None:
        return {"empty": True}
    return {
        "stats": stats,
        "report": get_latest_meta(),
    }
