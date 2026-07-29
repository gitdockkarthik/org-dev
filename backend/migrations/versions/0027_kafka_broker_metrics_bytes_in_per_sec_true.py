"""Add bytes_in_per_sec_true column to kafka_broker_metrics for true per-broker ingestion rate
Revision ID: 0027
Revises: 0026
Create Date: 2026-07-29
"""
from alembic import op
import sqlalchemy as sa

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_broker_metrics", sa.Column("bytes_in_per_sec_true", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("kafka_broker_metrics", "bytes_in_per_sec_true")
