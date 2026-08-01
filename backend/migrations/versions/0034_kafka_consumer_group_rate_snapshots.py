"""Add kafka_consumer_group_rate_snapshots table for group-level message rate tracking
Revision ID: 0034
Revises: 0033
Create Date: 2026-08-01
"""
from alembic import op
import sqlalchemy as sa


revision = "0034"
down_revision = "0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_consumer_group_rate_snapshots",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("inflow", sa.BigInteger(), nullable=True),
        sa.Column("outflow", sa.BigInteger(), nullable=True),
        sa.Column("interval_seconds", sa.Double(), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_consumer_group_rate_cluster_group_time",
        "kafka_consumer_group_rate_snapshots",
        ["cluster_id", "group_id", "collected_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_consumer_group_rate_cluster_group_time", table_name="kafka_consumer_group_rate_snapshots")
    op.drop_table("kafka_consumer_group_rate_snapshots")
