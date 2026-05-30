import json
from collections import defaultdict
from datetime import datetime
from typing import Any, ClassVar

from tools.base import ToolExecutor
from tools.noise_detector import classify_alerts


# ── Core stats computation ────────────────────────────────────────────────────

def compute_dashboard_stats(classified: list[dict]) -> dict:
    """Compute full dashboard stats from a classified alert list."""
    total = len(classified)
    noise_list = [a for a in classified if a["classification"] == "noise"]
    genuine_list = [a for a in classified if a["classification"] == "genuine"]
    noise_count = len(noise_list)
    genuine_count = len(genuine_list)

    # MTTR — mean close time for genuine closed alerts
    genuine_closed = [a for a in genuine_list if a.get("status") == "closed"]
    mttr = 0.0
    if genuine_closed:
        mttr = sum(a.get("close_time_seconds", 0) for a in genuine_closed) / len(genuine_closed)

    # Top noisy sources by alert count
    source_noise: dict[str, int] = {}
    for a in noise_list:
        src = a.get("source", "unknown")
        source_noise[src] = source_noise.get(src, 0) + 1
    top_noisy_sources = sorted(source_noise.items(), key=lambda x: x[1], reverse=True)[:10]

    # Average noise score per service
    service_scores: dict[str, list[int]] = {}
    for a in classified:
        src = a.get("source", "unknown")
        service_scores.setdefault(src, []).append(a["noise_score"])
    service_noise_score = {
        src: round(sum(scores) / len(scores), 2)
        for src, scores in service_scores.items()
    }

    # Repeat offenders — aliases with highest cumulative fire count
    alias_counts: dict[str, int] = {}
    for a in classified:
        alias = a.get("alias", "")
        alias_counts[alias] = alias_counts.get(alias, 0) + a.get("count", 1)
    repeat_offenders = sorted(alias_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    # High-severity genuine alerts
    high_severity_genuine = [
        a for a in genuine_list if a.get("priority") in ("P1", "P2")
    ][:20]

    # Team breakdown
    team_counts: dict[str, dict[str, int]] = {}
    for a in classified:
        team = (a.get("teams") or ["unknown"])[0]
        t = team_counts.setdefault(team, {"genuine": 0, "noise": 0})
        if a["classification"] == "noise":
            t["noise"] += 1
        else:
            t["genuine"] += 1

    # Hourly distribution and daily trend (last 7 days)
    hourly_bins: dict[int, int] = defaultdict(int)
    daily_bins: dict[str, dict[str, int]] = defaultdict(lambda: {"genuine": 0, "noise": 0})
    for a in classified:
        try:
            dt = datetime.fromisoformat(a["createdAt"].replace("Z", ""))
            hourly_bins[dt.hour] += 1
            day_key = dt.strftime("%Y-%m-%d")
            daily_bins[day_key][a["classification"]] += 1
        except Exception:
            pass

    return {
        "total": total,
        "noise_count": noise_count,
        "genuine_count": genuine_count,
        "noise_ratio": round(noise_count / total * 100, 1) if total else 0,
        "mttr_seconds": round(mttr),
        "mttr_minutes": round(mttr / 60, 1),
        "top_noisy_sources": [{"source": s, "count": c} for s, c in top_noisy_sources],
        "service_noise_scores": [
            {"service": s, "score": v} for s, v in service_noise_score.items()
        ],
        "repeat_offenders": [{"alias": a, "count": c} for a, c in repeat_offenders],
        "high_severity_genuine": high_severity_genuine,
        "unresolved_genuine": [a for a in genuine_list if a.get("status") == "open"][:20],
        "team_breakdown": [{"team": t, **v} for t, v in team_counts.items()],
        "hourly_distribution": [
            {"hour": h, "count": hourly_bins[h]} for h in range(24)
        ],
        "daily_trend": [
            {"date": d, **v} for d, v in sorted(daily_bins.items())
        ],
    }


# ── ToolExecutor wrapper ──────────────────────────────────────────────────────

class DashboardBuilderTool(ToolExecutor):
    """Compute full dashboard statistics from cached alert data."""

    name: ClassVar[str] = "build_dashboard"
    description: ClassVar[str] = (
        "Compute overview statistics, noise analysis, team breakdown, and trend data "
        "from cached OpsGenie alerts. Returns MTTR, top noisy sources, daily trend, "
        "hourly distribution, high-severity genuine alerts, and team breakdown."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "session_id": {
                "type": "string",
                "description": "Session whose cached alert data to analyse.",
            }
        },
        "required": ["session_id"],
    }

    def __init__(self, cache: dict[str, list[dict]]) -> None:
        self._cache = cache

    async def execute(self, session_id: str) -> str:  # type: ignore[override]
        alerts = self._cache.get(session_id, [])
        if not alerts:
            return json.dumps({"error": "No alert data loaded for this session."})

        classified = classify_alerts(alerts)
        stats = compute_dashboard_stats(classified)
        return json.dumps(stats, default=str)
