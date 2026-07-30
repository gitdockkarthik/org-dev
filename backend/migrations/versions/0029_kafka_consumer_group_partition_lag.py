"""Create kafka_consumer_group_partition_lag table for per-partition lag breakdown
Revision ID: 0029
Revises: 0028
Create Date: 2026-07-30
"""
from alembic import op
import sqlalchemy as sa

revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_consumer_group_partition_lag",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("lag", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("cluster_id", "group_id", "topic", "partition", name="uq_consumer_group_partition_lag"),
    )
    op.create_index("ix_consumer_group_partition_lag_group_topic", "kafka_consumer_group_partition_lag", ["cluster_id", "group_id", "topic"])


def downgrade() -> None:
    op.drop_index("ix_consumer_group_partition_lag_group_topic", table_name="kafka_consumer_group_partition_lag")
    op.drop_table("kafka_consumer_group_partition_lag")
