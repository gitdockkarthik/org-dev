"""Create kafka_consumer_group_topic_lag table for per-topic lag breakdown
Revision ID: 0028
Revises: 0027
Create Date: 2026-07-29
"""
from alembic import op
import sqlalchemy as sa

revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_consumer_group_topic_lag",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("partition_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lag", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("cluster_id", "group_id", "topic", name="uq_consumer_group_topic_lag"),
    )
    op.create_index("ix_consumer_group_topic_lag_group", "kafka_consumer_group_topic_lag", ["cluster_id", "group_id"])


def downgrade() -> None:
    op.drop_index("ix_consumer_group_topic_lag_group", table_name="kafka_consumer_group_topic_lag")
    op.drop_table("kafka_consumer_group_topic_lag")
