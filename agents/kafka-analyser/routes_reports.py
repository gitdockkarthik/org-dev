import logging
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import delete, select

import kafka_store
from config import settings
from tools.synthetic import SyntheticCollector

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/generate-sample")
async def generate_sample() -> dict:
    """Generate synthetic Kafka cluster data, load into memory, and persist to DB."""
    collector = SyntheticCollector()
    data = await collector.collect()
    kafka_store.set_cluster_data(data, source_type="synthetic")

    # ── Persist to PostgreSQL ─────────────────────────────────────────────────
    try:
        from database import SessionLocal
        from models import (
            KafkaAnomaly,
            KafkaBrokerMetrics,
            KafkaCluster,
            KafkaConnectorStatus,
            KafkaConsumerLag,
            KafkaTopicMetrics,
        )

        if SessionLocal is not None:
            now = datetime.now(timezone.utc)
            cluster_data = data["cluster"]

            async with SessionLocal() as session:
                # Upsert cluster record
                result = await session.execute(
                    select(KafkaCluster).where(
                        KafkaCluster.agent_slug == settings.agent_slug,
                        KafkaCluster.name == cluster_data["name"],
                    )
                )
                cluster_row = result.scalar_one_or_none()
                if cluster_row is None:
                    cluster_row = KafkaCluster(
                        agent_slug=settings.agent_slug,
                        name=cluster_data["name"],
                        source_type="synthetic",
                        status=cluster_data["status"],
                    )
                    session.add(cluster_row)
                    await session.flush()  # Populate cluster_row.id
                else:
                    cluster_row.status = cluster_data["status"]
                    cluster_row.source_type = "synthetic"

                cid = cluster_row.id

                # Delete old metrics for this cluster
                await session.execute(delete(KafkaBrokerMetrics).where(KafkaBrokerMetrics.cluster_id == cid))
                await session.execute(delete(KafkaConsumerLag).where(KafkaConsumerLag.cluster_id == cid))
                await session.execute(delete(KafkaTopicMetrics).where(KafkaTopicMetrics.cluster_id == cid))
                await session.execute(delete(KafkaConnectorStatus).where(KafkaConnectorStatus.cluster_id == cid))
                await session.execute(delete(KafkaAnomaly).where(KafkaAnomaly.cluster_id == cid))

                # Insert fresh broker metrics
                session.add_all([
                    KafkaBrokerMetrics(
                        time=now,
                        cluster_id=cid,
                        broker_id=b["broker_id"],
                        heap_pct=b["heap_pct"],
                        gc_pause_ms=b["gc_pause_ms"],
                        request_handler_idle_pct=b["request_handler_idle_pct"],
                        urp_count=b["urp_count"],
                        messages_in_per_sec=b["messages_in_per_sec"],
                        cpu_pct=b["cpu_pct"],
                        disk_pct=b["disk_pct"],
                    )
                    for b in data["brokers"]
                ])

                # Insert fresh consumer lag records — one row per partition
                lag_rows = []
                for g in data["consumer_groups"]:
                    for p in g.get("partitions", []):
                        lag_rows.append(KafkaConsumerLag(
                            time=now,
                            cluster_id=cid,
                            group_name=g["group_name"],
                            topic=g["topic"],
                            partition=p["partition"],
                            lag=p["lag"],
                            log_end_offset=p["log_end_offset"],
                            consumer_offset=p["consumer_offset"],
                            group_state=g["state"],
                        ))
                session.add_all(lag_rows)

                # Insert fresh topic metrics
                session.add_all([
                    KafkaTopicMetrics(
                        time=now,
                        cluster_id=cid,
                        topic=t["topic"],
                        partition_count=t["partition_count"],
                        replication_factor=t["replication_factor"],
                        messages_in_per_sec=t["messages_in_per_sec"],
                        bytes_in_per_sec=t["bytes_in_per_sec"],
                        bytes_out_per_sec=t["bytes_out_per_sec"],
                        total_messages=t["total_messages"],
                        size_bytes=t["size_bytes"],
                        retention_bytes=t["retention_bytes"],
                        retention_pct=t["retention_pct"],
                    )
                    for t in data["topics"]
                ])

                # Insert fresh connector status
                session.add_all([
                    KafkaConnectorStatus(
                        time=now,
                        cluster_id=cid,
                        connector_name=c["connector_name"],
                        connector_type=c["connector_type"],
                        state=c["state"],
                        failed_tasks=c["failed_tasks"],
                        total_tasks=c["total_tasks"],
                    )
                    for c in data["connectors"]
                ])

                # Insert fresh anomalies
                session.add_all([
                    KafkaAnomaly(
                        cluster_id=cid,
                        detected_at=now,
                        severity=a["severity"],
                        category=a["category"],
                        description=a["description"],
                        resolved_at=None,
                    )
                    for a in data["anomalies"]
                ])

                await session.commit()

            logger.info(
                "Synthetic data persisted to DB — cluster_id=%d, brokers=%d, groups=%d, topics=%d",
                cid,
                len(data["brokers"]),
                len(data["consumer_groups"]),
                len(data["topics"]),
            )
    except Exception as exc:
        logger.warning("Could not persist to DB: %s", exc)
        # Non-fatal — in-memory data still works

    meta = kafka_store.get_sync_meta()
    return {
        "ok": True,
        "message": (
            f"Synthetic data loaded — {meta['broker_count']} brokers, "
            f"{meta['consumer_group_count']} consumer groups, "
            f"{meta['topic_count']} topics, "
            f"{meta['connector_count']} connectors"
        ),
        **meta,
    }


@router.get("")
async def list_reports() -> dict:
    """Return current sync metadata."""
    return kafka_store.get_sync_meta()
