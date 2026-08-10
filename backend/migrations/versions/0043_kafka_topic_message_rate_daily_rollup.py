"""Add kafka_topic_message_rate_daily_rollup table -- third tier of message-rate
retention (raw <=2h -> hourly 2h-7d -> daily 7-30d), keeps 30-day retention within
Postgres container memory limits at current topic-cardinality scale.
Revision ID: 0043
Revises: 0042
Create Date: 2026-08-10
"""
from alembic import op
import sqlalchemy as sa


revision = "0043"
down_revision = "0042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_topic_message_rate_daily_rollup",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("day_bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_inflow", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("total_outflow", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cluster_id", "topic", "day_bucket", name="uq_topic_rate_daily_rollup"),
    )
    op.create_index(
        "ix_topic_rate_daily_rollup_cluster_time",
        "kafka_topic_message_rate_daily_rollup",
        ["cluster_id", "day_bucket"],
    )


def downgrade() -> None:
    op.drop_index("ix_topic_rate_daily_rollup_cluster_time", table_name="kafka_topic_message_rate_daily_rollup")
    op.drop_table("kafka_topic_message_rate_daily_rollup")
