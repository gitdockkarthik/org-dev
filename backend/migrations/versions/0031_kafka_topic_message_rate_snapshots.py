"""Add kafka_topic_message_rate_snapshots table for raw topic throughput tracking
Revision ID: 0031
Revises: 0030
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa


revision = "0031"
down_revision = "0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_topic_message_rate_snapshots",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("inflow", sa.BigInteger(), nullable=True),
        sa.Column("outflow", sa.BigInteger(), nullable=True),
        sa.Column("interval_seconds", sa.Double(), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_topic_message_rate_cluster_topic_time",
        "kafka_topic_message_rate_snapshots",
        ["cluster_id", "topic", "collected_at"],
    )
    op.create_index(
        "ix_topic_message_rate_cluster_time",
        "kafka_topic_message_rate_snapshots",
        ["cluster_id", "collected_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_topic_message_rate_cluster_time", table_name="kafka_topic_message_rate_snapshots")
    op.drop_index("ix_topic_message_rate_cluster_topic_time", table_name="kafka_topic_message_rate_snapshots")
    op.drop_table("kafka_topic_message_rate_snapshots")
