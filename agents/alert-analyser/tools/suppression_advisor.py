import json
from typing import Any, ClassVar

from tools.base import ToolExecutor
from tools.noise_detector import classify_alerts


# ── Core suppression logic ────────────────────────────────────────────────────

def build_suppression_recommendations(classified: list[dict]) -> list[dict]:
    """Generate suppression recommendations from noise patterns.

    Strategy:
    - Group noise alerts by alias.
    - Rank by: high auto-resolve rate AND high occurrence count.
    - Return top candidates with rationale and suggested OpsGenie rule.
    """
    noise = [a for a in classified if a["classification"] == "noise"]

    # Aggregate per alias
    alias_data: dict[str, dict] = {}
    for a in noise:
        alias = a.get("alias", "unknown")
        src = a.get("source", "unknown")
        entry = alias_data.setdefault(
            alias,
            {
                "alias": alias,
                "source": src,
                "count": 0,
                "auto_resolve_rate_sum": 0.0,
                "reasons": set(),
            },
        )
        entry["count"] += a.get("count", 1)
        entry["auto_resolve_rate_sum"] += a.get("auto_resolve_rate", 0.0)
        for r in a.get("noise_reasons", []):
            entry["reasons"].add(r)

    recommendations: list[dict] = []
    for alias, data in alias_data.items():
        count = data["count"]
        avg_auto_resolve = (
            data["auto_resolve_rate_sum"] / count if count else 0.0
        )
        # Confidence: high if auto-resolve rate >50% or fires >5 times
        confidence = "high" if avg_auto_resolve > 0.5 or count > 5 else "medium"
        reasons = sorted(data["reasons"])

        recommendations.append(
            {
                "alias": alias,
                "source": data["source"],
                "noise_alert_count": count,
                "avg_auto_resolve_rate": round(avg_auto_resolve, 2),
                "confidence": confidence,
                "noise_reasons": reasons,
                "suggested_rule": (
                    f"Suppress alerts matching alias '{alias}' "
                    f"from source '{data['source']}' "
                    f"when they auto-close within 5 minutes without acknowledgement."
                ),
            }
        )

    # Sort: high-confidence first, then by count descending
    recommendations.sort(
        key=lambda r: (r["confidence"] != "high", -r["noise_alert_count"])
    )
    return recommendations[:10]


# ── ToolExecutor wrapper ──────────────────────────────────────────────────────

class SuppressionAdvisorTool(ToolExecutor):
    """Generate suppression recommendations from noise patterns in cached alert data."""

    name: ClassVar[str] = "get_suppression_recommendations"
    description: ClassVar[str] = (
        "Analyse noise patterns in cached OpsGenie alerts and generate ranked suppression "
        "recommendations. Each recommendation includes alias, source, noise count, "
        "auto-resolve rate, confidence level, and a suggested OpsGenie suppression rule."
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
        alerts, _ = self._cache.get(session_id, ([], 0.0))
        if not alerts:
            return json.dumps({"error": "No alert data loaded for this session."})

        classified = classify_alerts(alerts)
        recs = build_suppression_recommendations(classified)
        return json.dumps({"recommendations": recs}, default=str)
