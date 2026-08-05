"""Add kafka_topic_bytes_rate_snapshots table for raw bytes-in throughput tracking
(enables 5-min granularity for the Bytes In chart's 1-hour view, replacing the
flat-line hourly-average appearance -- same architecture as 0031's message-rate table)

Revision ID: 0037
Revises: 0036
Create Date: 2026-08-05
"""
from alembic import op
import sqlalchemy as sa


revision = "0037"
down_revision = "0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_topic_bytes_rate_snapshots",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("bytes_in_per_sec", sa.Double(), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_topic_bytes_rate_cluster_topic_time",
        "kafka_topic_bytes_rate_snapshots",
        ["cluster_id", "topic", "collected_at"],
    )
    op.create_index(
        "ix_topic_bytes_rate_cluster_time",
        "kafka_topic_bytes_rate_snapshots",
        ["cluster_id", "collected_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_topic_bytes_rate_cluster_time", table_name="kafka_topic_bytes_rate_snapshots")
    op.drop_index("ix_topic_bytes_rate_cluster_topic_time", table_name="kafka_topic_bytes_rate_snapshots")
    op.drop_table("kafka_topic_bytes_rate_snapshots")
