"""Add kafka_consumer_group_lag table for per-group lag upsert
Revision ID: 0016
Revises: 0015
Create Date: 2026-07-23
"""
from alembic import op
import sqlalchemy as sa

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_consumer_group_lag",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.String(256), nullable=False),
        sa.Column("state", sa.String(32), nullable=True),
        sa.Column("total_lag", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("topic_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("committed_offsets", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("cluster_id", "group_id", name="uq_consumer_group_lag"),
    )


def downgrade() -> None:
    op.drop_table("kafka_consumer_group_lag")
