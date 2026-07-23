"""Add bytes and latency columns to kafka_broker_metrics
Revision ID: 0014
Revises: 0013
Create Date: 2026-07-23
"""
from alembic import op
import sqlalchemy as sa

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_broker_metrics", sa.Column("bytes_in_per_sec", sa.Float(), nullable=True, server_default="0"))
    op.add_column("kafka_broker_metrics", sa.Column("bytes_out_per_sec", sa.Float(), nullable=True, server_default="0"))
    op.add_column("kafka_broker_metrics", sa.Column("produce_latency_ms", sa.Float(), nullable=True, server_default="0"))
    op.add_column("kafka_broker_metrics", sa.Column("fetch_latency_ms", sa.Float(), nullable=True, server_default="0"))
    op.add_column("kafka_broker_metrics", sa.Column("isr_shrinks_per_sec", sa.Float(), nullable=True, server_default="0"))
    op.add_column("kafka_broker_metrics", sa.Column("isr_expands_per_sec", sa.Float(), nullable=True, server_default="0"))


def downgrade() -> None:
    op.drop_column("kafka_broker_metrics", "isr_expands_per_sec")
    op.drop_column("kafka_broker_metrics", "isr_shrinks_per_sec")
    op.drop_column("kafka_broker_metrics", "fetch_latency_ms")
    op.drop_column("kafka_broker_metrics", "produce_latency_ms")
    op.drop_column("kafka_broker_metrics", "bytes_out_per_sec")
    op.drop_column("kafka_broker_metrics", "bytes_in_per_sec")
