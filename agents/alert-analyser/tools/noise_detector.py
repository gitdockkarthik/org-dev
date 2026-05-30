import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, ClassVar

from config import settings
from tools.base import ToolExecutor


# ── Core classification logic ─────────────────────────────────────────────────

def classify_alerts(alerts: list[dict]) -> list[dict]:
    """Classify each alert as noise or genuine using rule-based scoring.

    Scoring (thresholds read from settings / env vars):
      +2  alias fires >NOISE_THRESHOLD_REPEAT times within any 1-hour window
      +2  auto-closes in <NOISE_THRESHOLD_CLOSE_SECS seconds without acknowledgement
      +1  never acknowledged
      -3  priority is P1 or P2
      -2  open >1800 seconds
    classification = 'noise' if noise_score > 0 else 'genuine'
    """
    repeat_threshold: int = settings.noise_threshold_repeat
    close_secs_threshold: int = settings.noise_threshold_close_secs

    # Build alias → [datetime] map for sliding-window repeat detection
    alias_windows: dict[str, list[datetime]] = {}
    for alert in alerts:
        alias = alert.get("alias", "")
        try:
            created = datetime.fromisoformat(alert["createdAt"].replace("Z", ""))
        except Exception:
            created = datetime.utcnow()
        alias_windows.setdefault(alias, []).append(created)

    # Identify aliases that fire >repeat_threshold times in any 1-hour window
    frequent_aliases: set[str] = set()
    for alias, times in alias_windows.items():
        times_sorted = sorted(times)
        for i, t in enumerate(times_sorted):
            window_end = t + timedelta(hours=1)
            count_in_window = sum(1 for tt in times_sorted[i:] if tt <= window_end)
            if count_in_window > repeat_threshold:
                frequent_aliases.add(alias)
                break

    # Per-source auto-resolve rate (supplementary metadata, not used for scoring)
    source_stats: dict[str, dict[str, int]] = {}
    for alert in alerts:
        src = alert.get("source", "unknown")
        close_time = alert.get("report", {}).get("closeTime", 9999)
        auto_resolved = (
            close_time < close_secs_threshold and not alert.get("acknowledged", False)
        )
        s = source_stats.setdefault(src, {"total": 0, "auto_resolved": 0})
        s["total"] += 1
        if auto_resolved:
            s["auto_resolved"] += 1

    auto_resolve_rates: dict[str, float] = {
        src: round(v["auto_resolved"] / v["total"], 2) if v["total"] else 0.0
        for src, v in source_stats.items()
    }

    classified: list[dict] = []
    for alert in alerts:
        noise_score = 0
        noise_reasons: list[str] = []
        genuine_reasons: list[str] = []

        alias = alert.get("alias", "")
        src = alert.get("source", "unknown")
        close_time = alert.get("report", {}).get("closeTime", 9999)
        acknowledged = alert.get("acknowledged", False)
        priority = alert.get("priority", "P5")

        if alias in frequent_aliases:
            noise_score += 2
            noise_reasons.append(f"fires >{repeat_threshold}x within 1 hour")
        if close_time < close_secs_threshold and not acknowledged:
            noise_score += 2
            noise_reasons.append(f"auto-closes in <{close_secs_threshold}s without ACK")
        if not acknowledged:
            noise_score += 1
            noise_reasons.append("never acknowledged")
        if priority in ("P1", "P2"):
            noise_score -= 3
            genuine_reasons.append(f"{priority} priority")
        if close_time > 1800:
            noise_score -= 2
            genuine_reasons.append("open >1800s")

        classified.append(
            {
                **alert,
                "noise_score": noise_score,
                "classification": "noise" if noise_score > 0 else "genuine",
                "noise_reasons": noise_reasons,
                "genuine_reasons": genuine_reasons,
                "close_time_seconds": close_time,
                "auto_resolve_rate": auto_resolve_rates.get(src, 0.0),
            }
        )

    return classified


# ── ToolExecutor wrapper ──────────────────────────────────────────────────────

class NoiseDetectorTool(ToolExecutor):
    """Classify cached alerts as noise or genuine and return the split with scores."""

    name: ClassVar[str] = "classify_alerts"
    description: ClassVar[str] = (
        "Classify OpsGenie alerts as noise or genuine using rule-based scoring. "
        "Returns total counts, noise ratio percentage, and per-alert classification "
        "with noise_score, classification label, noise_reasons, and genuine_reasons."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "session_id": {
                "type": "string",
                "description": "Session whose cached alert data should be classified.",
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
        noise = [a for a in classified if a["classification"] == "noise"]
        genuine = [a for a in classified if a["classification"] == "genuine"]
        total = len(classified)

        return json.dumps(
            {
                "total": total,
                "noise_count": len(noise),
                "genuine_count": len(genuine),
                "noise_ratio_pct": round(len(noise) / total * 100, 1) if total else 0,
                "noise": noise[:50],
                "genuine": genuine[:50],
            },
            default=str,
        )


# ── Dashboard stats ───────────────────────────────────────────────────────────

def compute_dashboard_stats(classified: list[dict]) -> dict:
    """Aggregate a classified alert list into dashboard metrics."""
    noise = [a for a in classified if a["classification"] == "noise"]
    genuine = [a for a in classified if a["classification"] == "genuine"]
    total = len(classified)
    noise_ratio = round(len(noise) / total * 100, 1) if total else 0

    # MTTR — mean close time for genuine alerts in minutes
    close_times = [
        a.get("report", {}).get("closeTime", 0)
        for a in genuine
        if a.get("report", {}).get("closeTime", 0) > 0
    ]
    mttr_minutes = round(sum(close_times) / len(close_times) / 60, 1) if close_times else 0

    # Daily trend
    daily: dict[str, dict[str, int]] = defaultdict(lambda: {"genuine": 0, "noise": 0})
    for a in classified:
        try:
            date = datetime.fromisoformat(a["createdAt"].replace("Z", "")).strftime("%Y-%m-%d")
        except Exception:
            continue
        daily[date]["noise" if a["classification"] == "noise" else "genuine"] += 1
    daily_trend = [{"date": d, **v} for d, v in sorted(daily.items())]

    # Repeat offenders (noisiest aliases by count)
    alias_noise_counts = Counter(a.get("alias", "") for a in noise)
    repeat_offenders = [{"alias": a, "count": c} for a, c in alias_noise_counts.most_common(10)]

    # Top noisy sources
    src_noise = Counter(a.get("source", "unknown") for a in noise)
    top_noisy_sources = [{"source": s, "count": c} for s, c in src_noise.most_common(10)]

    # Service noise scores (noise % per source)
    src_total = Counter(a.get("source", "unknown") for a in classified)
    service_noise_scores = [
        {"service": s, "score": round((c / src_total[s]) * 100, 1)}
        for s, c in src_noise.most_common()
    ]

    # Suppression recommendations (aliases firing ≥3 times as noise)
    suppression_recommendations = [r for r in repeat_offenders if r["count"] >= 3]

    # Team breakdown
    team_buckets: dict[str, dict[str, int]] = defaultdict(lambda: {"genuine": 0, "noise": 0})
    for a in classified:
        teams = a.get("teams", ["Unknown"])
        team = teams[0] if teams else "Unknown"
        team_buckets[team]["noise" if a["classification"] == "noise" else "genuine"] += 1
    team_breakdown = sorted(
        [{"team": t, **v} for t, v in team_buckets.items()],
        key=lambda x: x["genuine"] + x["noise"],
        reverse=True,
    )[:10]

    # Hourly distribution
    hourly: dict[int, int] = defaultdict(int)
    for a in classified:
        try:
            h = datetime.fromisoformat(a["createdAt"].replace("Z", "")).hour
        except Exception:
            continue
        hourly[h] += 1
    hourly_distribution = [{"hour": h, "count": hourly.get(h, 0)} for h in range(24)]

    # Unresolved and high-severity genuine
    unresolved_genuine = [a for a in genuine if a.get("status", "").lower() in ("open", "")][:20]
    high_severity_genuine = [a for a in genuine if a.get("priority", "") in ("P1", "P2")][:10]

    return {
        "total": total,
        "noise_count": len(noise),
        "genuine_count": len(genuine),
        "noise_ratio": noise_ratio,
        "mttr_minutes": mttr_minutes,
        "daily_trend": daily_trend,
        "repeat_offenders": repeat_offenders,
        "top_noisy_sources": top_noisy_sources,
        "service_noise_scores": service_noise_scores,
        "suppression_recommendations": suppression_recommendations,
        "unresolved_genuine": unresolved_genuine,
        "high_severity_genuine": high_severity_genuine,
        "team_breakdown": team_breakdown,
        "hourly_distribution": hourly_distribution,
    }
