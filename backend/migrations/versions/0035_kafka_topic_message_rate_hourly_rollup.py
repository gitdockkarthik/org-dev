"""Add kafka_topic_message_rate_hourly_rollup table for pre-aggregated message rates
Revision ID: 0035
Revises: 0034
Create Date: 2026-08-01
"""
from alembic import op
import sqlalchemy as sa


revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_topic_message_rate_hourly_rollup",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("hour_bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_inflow", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("total_outflow", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cluster_id", "topic", "hour_bucket", name="uq_topic_rate_rollup"),
    )
    op.create_index(
        "ix_topic_rate_rollup_cluster_time",
        "kafka_topic_message_rate_hourly_rollup",
        ["cluster_id", "hour_bucket"],
    )


def downgrade() -> None:
    op.drop_index("ix_topic_rate_rollup_cluster_time", table_name="kafka_topic_message_rate_hourly_rollup")
    op.drop_table("kafka_topic_message_rate_hourly_rollup")
