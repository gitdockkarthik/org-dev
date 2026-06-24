import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, ClassVar

from tools.base import ToolExecutor
from tools.noise_detector import classify_alerts


# Matches a UUID and captures its first 8-char segment.
_UUID_RE = re.compile(
    r"([0-9a-fA-F]{8})-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def _clean_team_name(team) -> str:
    """Render a team value as an exec-friendly label.

    OpsGenie team fields can arrive as a stringified dict like
    "{'type': 'team', 'id': '9a64f39e-...'}". Those collapse to "Team-9a64f39e".
    "Unknown" is preserved, and clean human names pass through unchanged.
    """
    if team is None:
        return "Unknown"
    s = str(team).strip()
    if not s or s.lower() == "unknown":
        return "Unknown"
    if "id" in s:
        m = _UUID_RE.search(s)
        if m:
            return f"Team-{m.group(1)}"
    return s


# ── Core stats computation ────────────────────────────────────────────────────

def compute_dashboard_stats(classified: list[dict]) -> dict:
    """Compute full dashboard stats from a classified alert list."""
    # Deduplicate by alias — keep most recent per alias
    # Same incident can appear multiple times (open + close events)
    seen: dict[str, dict] = {}
    for a in classified:
        alias = a.get("alias") or a.get("id", "")
        if not alias:
            continue
        existing = seen.get(alias)
        if existing is None:
            seen[alias] = a
        else:
            # Keep most recent
            if a.get("createdAt", "") > existing.get("createdAt", ""):
                seen[alias] = a
    deduplicated = list(seen.values())

    total = len(deduplicated)
    noise_list = [a for a in deduplicated if a["classification"] == "noise"]
    suspect_list = [a for a in deduplicated if a["classification"] == "noise-suspect"]
    genuine_list = [a for a in deduplicated if a["classification"] == "genuine"]
    noise_count = len(noise_list)
    suspect_count = len(suspect_list)
    genuine_count = len(genuine_list)

    # Raw (pre-deduplication) counts — surfaced in the UI so the team can see
    # how many duplicate open/close records were collapsed per classification.
    genuine_count_raw = len([a for a in classified if a["classification"] == "genuine"])
    noise_count_raw = len([a for a in classified if a["classification"] == "noise"])
    suspect_count_raw = len([a for a in classified if a["classification"] == "noise-suspect"])

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
    alias_message: dict[str, str] = {}
    for a in classified:
        alias = a.get("alias", "")
        alias_counts[alias] = alias_counts.get(alias, 0) + a.get("count", 1)
        if alias not in alias_message and a.get("message"):
            alias_message[alias] = a.get("message")
    repeat_offenders = sorted(alias_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    # High-severity genuine alerts
    high_severity_genuine_all = [
        a for a in genuine_list if a.get("priority") in ("P1", "P2")
    ]
    high_severity_count = len(high_severity_genuine_all)
    high_severity_genuine = high_severity_genuine_all[:50]

    # Team breakdown
    team_counts: dict[str, dict[str, int]] = {}
    for a in classified:
        team = (a.get("teams") or ["unknown"])[0]
        t = team_counts.setdefault(team, {"genuine": 0, "noise": 0, "suspect": 0})
        cls = a["classification"]
        if cls == "noise":
            t["noise"] += 1
        elif cls == "noise-suspect":
            t["suspect"] += 1
        else:
            t["genuine"] += 1

    # Hourly distribution and daily trend (last 7 days)
    hourly_bins: dict[int, int] = defaultdict(int)
    daily_bins: dict[str, dict[str, int]] = defaultdict(
        lambda: {"genuine": 0, "noise": 0, "suspect": 0}
    )
    _day_bucket = {"noise": "noise", "noise-suspect": "suspect", "genuine": "genuine"}
    for a in classified:
        try:
            dt = datetime.fromisoformat(a["createdAt"].replace("Z", ""))
            hourly_bins[dt.hour] += 1
            day_key = dt.strftime("%Y-%m-%d")
            daily_bins[day_key][_day_bucket.get(a["classification"], "genuine")] += 1
        except Exception:
            pass

    noise_ratio = round((noise_count + suspect_count) / total * 100, 1) if total else 0
    genuine_ratio = round(genuine_count / total * 100, 1) if total else 0

    return {
        "total": total,
        "total_raw": len(classified),
        "duplicate_count": len(classified) - len(deduplicated),
        "noise_count": noise_count,
        "suspect_count": suspect_count,
        "genuine_count": genuine_count,
        "genuine_count_raw": genuine_count_raw,
        "genuine_duplicates": genuine_count_raw - genuine_count,
        "noise_count_raw": noise_count_raw,
        "noise_duplicates": noise_count_raw - noise_count,
        "suspect_count_raw": suspect_count_raw,
        "suspect_duplicates": suspect_count_raw - suspect_count,
        "noise_ratio": noise_ratio,
        "genuine_ratio": genuine_ratio,
        "mttr_seconds": round(mttr),
        "mttr_minutes": round(mttr / 60, 1),
        "top_noisy_sources": [{"source": s, "count": c} for s, c in top_noisy_sources],
        "service_noise_scores": [
            {"service": s, "score": v} for s, v in service_noise_score.items()
        ],
        "repeat_offenders": [
            {"alias": alias_message.get(a) or (a[:8] if a else ""), "count": c}
            for a, c in repeat_offenders
        ],
        "suppression_recommendations": [{"alias": alias_message.get(a) or (a[:8] if a else ""), "count": c} for a, c in repeat_offenders[:10]],
        "high_severity_count": high_severity_count,
        "high_severity_genuine": high_severity_genuine,
        "unresolved_count": len([a for a in genuine_list if a.get("status") == "open"]),
        "unresolved_genuine": [a for a in genuine_list if a.get("status") == "open"][:50],
        "team_breakdown": [{"team": _clean_team_name(t), **v} for t, v in team_counts.items()],
        "hourly_distribution": [
            {"hour": h, "count": hourly_bins[h]} for h in range(24)
        ],
        "daily_trend": [
            {"date": d, **v} for d, v in sorted(daily_bins.items())
        ],
        "data_quality": compute_data_quality(classified),
    }


def compute_data_quality(classified: list[dict]) -> dict:
    """Derive actionable data-quality insights from classified alerts.

    Powers the Overview "Data Quality Insights" cards: lifecycle health,
    per-source close behaviour, priority distribution, and acknowledgement.
    Recommendation strings (and their trigger thresholds) live here so the
    UI only has to render them.
    """
    total = len(classified)

    def pct(n: int) -> float:
        return round(n / total * 100, 1) if total else 0.0

    # ── Card 1: alert lifecycle health ──
    closed = [a for a in classified if a.get("status") == "closed"]
    never_closed = total - len(closed)
    proper_cycle = sum(1 for a in closed if a.get("close_time_seconds", 0) > 0)
    acked = sum(1 for a in classified if a.get("acknowledged"))
    unacked = total - acked

    never_closed_pct = pct(never_closed)
    lifecycle_rec = (
        f"{never_closed_pct}% of alerts never close in OpsGenie. Configure "
        "auto-close in integrations or train teams to close resolved alerts. "
        "This improves MTTR accuracy and noise detection by ~40%."
    )

    # ── Card 2: per-source health ──
    src_stats: dict[str, dict[str, int]] = {}
    for a in classified:
        src = a.get("source", "unknown")
        d = src_stats.setdefault(src, {"total": 0, "closed": 0, "fast": 0, "close_secs": 0})
        d["total"] += 1
        if a.get("status") == "closed":
            ct = a.get("close_time_seconds", 0)
            d["closed"] += 1
            d["close_secs"] += ct
            if 0 < ct < 300:
                d["fast"] += 1

    sources: list[dict] = []
    for src, d in src_stats.items():
        n = d["total"]
        never_rate = round((n - d["closed"]) / n * 100, 1) if n else 0.0
        avg_close = round(d["close_secs"] / d["closed"]) if d["closed"] else 0
        sources.append({
            "source": src,
            "count": n,
            "auto_resolve_rate": round(d["fast"] / n * 100, 1) if n else 0.0,
            "avg_close_secs": avg_close,
            "avg_close_mins": round(avg_close / 60, 1),
            "never_close_rate": never_rate,
            "flagged": never_rate > 50,
        })
    sources.sort(key=lambda x: x["never_close_rate"], reverse=True)
    flagged_src = next((s for s in sources if s["flagged"]), None)
    source_rec = (
        f"{flagged_src['source']} has {flagged_src['never_close_rate']}% open alerts. "
        "Review webhook configuration."
        if flagged_src else ""
    )

    # ── Card 3: priority distribution ──
    prio_order = ["P1", "P2", "P3", "P4", "P5"]
    prio_counts = {p: 0 for p in prio_order}
    for a in classified:
        p = a.get("priority", "P5")
        prio_counts[p] = prio_counts.get(p, 0) + 1
    distribution = [
        {"priority": p, "count": prio_counts[p], "pct": pct(prio_counts[p])}
        for p in prio_order
    ]
    p5_pct = pct(prio_counts.get("P5", 0))
    prio_flagged = p5_pct > 40
    prio_rec = (
        f"{p5_pct}% of alerts are P5. Consider raising thresholds in monitoring "
        "tools to reduce low-priority alert volume."
        if prio_flagged else ""
    )

    # ── Card 4: acknowledgement rate ──
    # Note: alerts carry only an `acknowledged` flag, not an ack timestamp, so
    # we report overall acknowledgement rather than a within-15-minutes rate.
    never_acked_pct = pct(unacked)
    ack_rec = (
        f"{never_acked_pct}% of alerts are never acknowledged. Review on-call "
        "rotation and escalation policies."
        if never_acked_pct > 30 else ""
    )

    return {
        "lifecycle": {
            "never_closed_pct": never_closed_pct,
            "proper_cycle_pct": pct(proper_cycle),
            "acknowledged_pct": pct(acked),
            "unacknowledged_pct": pct(unacked),
            "recommendation": lifecycle_rec,
        },
        "sources": sources,
        "source_recommendation": source_rec,
        "priority": {
            "distribution": distribution,
            "p5_pct": p5_pct,
            "flagged": prio_flagged,
            "recommendation": prio_rec,
        },
        "acknowledgement": {
            "acknowledged_pct": pct(acked),
            "never_acked_pct": never_acked_pct,
            "recommendation": ack_rec,
        },
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
