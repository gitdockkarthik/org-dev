"""Add sr_restricted to kafka_clusters and create kafka_sr_subjects table
Revision ID: 0024
Revises: 0023
Create Date: 2026-07-28
"""
from alembic import op
import sqlalchemy as sa

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column("kafka_clusters", sa.Column("sr_restricted", sa.Boolean(), nullable=True))
    op.create_table(
        "kafka_sr_subjects",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("latest_version", sa.Integer(), nullable=True),
        sa.Column("schema_type", sa.Text(), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("cluster_id", "subject", name="uq_kafka_sr_subjects_cluster_subject"),
    )
    op.create_index("ix_kafka_sr_subjects_cluster_id", "kafka_sr_subjects", ["cluster_id"])

def downgrade() -> None:
    op.drop_index("ix_kafka_sr_subjects_cluster_id", table_name="kafka_sr_subjects")
    op.drop_table("kafka_sr_subjects")
    op.drop_column("kafka_clusters", "sr_restricted")
