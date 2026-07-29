"""Add data_gb_true column to kafka_broker_metrics for true per-broker log sizes
Revision ID: 0026
Revises: 0025
Create Date: 2026-07-29
"""
from alembic import op
import sqlalchemy as sa

revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_broker_metrics", sa.Column("data_gb_true", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("kafka_broker_metrics", "data_gb_true")
