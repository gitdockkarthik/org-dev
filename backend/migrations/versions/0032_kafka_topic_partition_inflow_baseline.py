"""Add kafka_topic_partition_inflow_baseline table for persistent end-offset tracking
Revision ID: 0032
Revises: 0031
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa


revision = "0032"
down_revision = "0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_topic_partition_inflow_baseline",
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("end_offset", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("cluster_id", "topic", "partition"),
    )


def downgrade() -> None:
    op.drop_table("kafka_topic_partition_inflow_baseline")
