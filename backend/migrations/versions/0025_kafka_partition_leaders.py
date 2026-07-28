"""Add kafka_partition_leaders table
Revision ID: 0025
Revises: 0024
Create Date: 2026-07-28
"""
from alembic import op
import sqlalchemy as sa

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table(
        "kafka_partition_leaders",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("leader_broker_id", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("cluster_id", "topic", "partition", name="uq_kafka_partition_leaders"),
    )
    op.create_index("ix_kafka_partition_leaders_cluster_broker", "kafka_partition_leaders", ["cluster_id", "leader_broker_id"])

def downgrade() -> None:
    op.drop_index("ix_kafka_partition_leaders_cluster_broker", table_name="kafka_partition_leaders")
    op.drop_table("kafka_partition_leaders")
