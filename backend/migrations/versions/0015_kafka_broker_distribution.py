"""Add kafka_broker_distribution table
Revision ID: 0015
Revises: 0014
Create Date: 2026-07-23
"""
from alembic import op
import sqlalchemy as sa

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_broker_distribution",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("broker_id", sa.String(64), nullable=False),
        sa.Column("leader_partition_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("replica_partition_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("data_gb", sa.Float(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("cluster_id", "broker_id", name="uq_broker_dist_cluster_broker"),
    )


def downgrade() -> None:
    op.drop_table("kafka_broker_distribution")
