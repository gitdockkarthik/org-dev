"""Add urp_count column to kafka_topic_metrics -- persists the per-topic
under-replicated-partition count already computed during collect_topic_structure
(via describe_topics -- comparing ISR size to replica count per partition), which
was previously discarded after the job run instead of being written to the
database. Confirmed as a real, live data-accuracy gap: dashboard showed URP=0
during an active broker outage where the Kafka team's own CLI check
(kafka-topics --describe --under-replicated-partitions | wc -l) showed 19,912
genuine under-replicated partitions. The existing urp_count on kafka_broker_metrics
comes from a separate, unreliable Prometheus/JMX scrape path (a known, previously
documented metric-name mismatch against these clusters' actual exporters) and
should not be relied on for this.

Revision ID: 0039
Revises: 0038
Create Date: 2026-08-06
"""
from alembic import op
import sqlalchemy as sa


revision = "0039"
down_revision = "0038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "kafka_topic_metrics",
        sa.Column("urp_count", sa.Integer(), nullable=True, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("kafka_topic_metrics", "urp_count")
