"""Add inflow/consumption tracking columns to kafka_consumer_group_partition_lag
Revision ID: 0030
Revises: 0029
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa


revision = "0030"
down_revision = "0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "kafka_consumer_group_partition_lag",
        sa.Column("end_offset", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "kafka_consumer_group_partition_lag",
        sa.Column("committed_offset", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "kafka_consumer_group_partition_lag",
        sa.Column("inflow_since_last", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "kafka_consumer_group_partition_lag",
        sa.Column("consumed_since_last", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "kafka_consumer_group_partition_lag",
        sa.Column("interval_seconds", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("kafka_consumer_group_partition_lag", "interval_seconds")
    op.drop_column("kafka_consumer_group_partition_lag", "consumed_since_last")
    op.drop_column("kafka_consumer_group_partition_lag", "inflow_since_last")
    op.drop_column("kafka_consumer_group_partition_lag", "committed_offset")
    op.drop_column("kafka_consumer_group_partition_lag", "end_offset")
