"""MirrorMaker detector — detects MM1 and MM2 replication from consumer group patterns.

MirrorMaker 1: consumer groups matching pattern 'mirror-*' or '__consumer_offsets'
               producer groups on target cluster
MirrorMaker 2: internal topics starting with '<source>.' prefix and
               consumer groups matching 'mm2-*' or '<cluster>.checkpoint.internal'

No additional connectivity needed — works from existing consumer group data.
"""
from __future__ import annotations
import re
from typing import Any

# MM1 consumer group patterns
_MM1_PATTERNS = [
    re.compile(r"^mirror[-_]"),
    re.compile(r"^kafka[-_]mirror"),
    re.compile(r"^mm1[-_]"),
]

# MM2 consumer group patterns
_MM2_PATTERNS = [
    re.compile(r"^mm2[-_]"),
    re.compile(r"\.checkpoint\.internal$"),
    re.compile(r"^MirrorSourceConnector$"),
    re.compile(r"^MirrorHeartbeatConnector$"),
    re.compile(r"^MirrorCheckpointConnector$"),
]

# MM2 internal topic patterns
_MM2_TOPIC_PATTERNS = [
    re.compile(r"\.heartbeats$"),
    re.compile(r"\.checkpoints\.internal$"),
    re.compile(r"\.offsets\.sync$"),
    re.compile(r"^mm2-"),
]


def detect_mirrormaker(cluster_data: dict[str, Any]) -> dict[str, Any]:
    """Detect MirrorMaker 1 and 2 from cluster data."""
    groups = cluster_data.get("consumer_groups", [])
    topics = cluster_data.get("topics", [])

    group_ids = [g.get("group_id") or g.get("group_name", "") for g in groups]
    topic_names = [t.get("name") or t.get("topic", "") for t in topics]

    # Detect MM1
    mm1_groups = [g for g in group_ids if any(p.search(g) for p in _MM1_PATTERNS)]

    # Detect MM2 via consumer groups
    mm2_groups = [g for g in group_ids if any(p.search(g) for p in _MM2_PATTERNS)]

    # Detect MM2 via internal topics
    mm2_topics = [t for t in topic_names if any(p.search(t) for p in _MM2_TOPIC_PATTERNS)]

    # Determine replication mode
    has_mm1 = len(mm1_groups) > 0
    has_mm2 = len(mm2_groups) > 0 or len(mm2_topics) > 0

    if not has_mm1 and not has_mm2:
        return {
            "detected": False,
            "mode": "none",
            "message": "No MirrorMaker replication detected on this cluster.",
            "mm1": None,
            "mm2": None,
        }

    result = {
        "detected": True,
        "mode": "mm1" if has_mm1 and not has_mm2 else "mm2" if has_mm2 and not has_mm1 else "both",
        "mm1": None,
        "mm2": None,
    }

    if has_mm1:
        # Compute MM1 lag from matching groups
        mm1_group_data = [
            g for g in groups
            if any(p.search(g.get("group_id") or g.get("group_name", "")) for p in _MM1_PATTERNS)
        ]
        total_lag = sum(g.get("total_lag", 0) for g in mm1_group_data)
        result["mm1"] = {
            "consumer_groups": mm1_groups,
            "group_count": len(mm1_groups),
            "total_lag": total_lag,
            "status": "healthy" if total_lag < 10000 else "lagging",
        }

    if has_mm2:
        # Compute MM2 lag from matching groups
        mm2_group_data = [
            g for g in groups
            if any(p.search(g.get("group_id") or g.get("group_name", "")) for p in _MM2_PATTERNS)
        ]
        total_lag = sum(g.get("total_lag", 0) for g in mm2_group_data)
        result["mm2"] = {
            "consumer_groups": mm2_groups,
            "group_count": len(mm2_groups),
            "internal_topics": mm2_topics,
            "topic_count": len(mm2_topics),
            "total_lag": total_lag,
            "status": "healthy" if total_lag < 10000 else "lagging",
        }

    return result
