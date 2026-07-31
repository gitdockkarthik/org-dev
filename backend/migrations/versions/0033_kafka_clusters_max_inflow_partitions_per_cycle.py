"""Add max_inflow_partitions_per_cycle to kafka_clusters

Revision ID: 0033
Revises: 0032
Create Date: 2026-07-31
"""
from alembic import op
import sqlalchemy as sa

revision = "0033"
down_revision = "0032"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column("kafka_clusters", sa.Column("max_inflow_partitions_per_cycle", sa.Integer(), nullable=True))

def downgrade() -> None:
    op.drop_column("kafka_clusters", "max_inflow_partitions_per_cycle")
