"""Add cluster_id indexes for performance on topic metrics and consumer group lag tables
Revision ID: 0036
Revises: 0035
Create Date: 2026-08-01
"""
from alembic import op


revision = "0036"
down_revision = "0035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_topic_metrics_cluster
        ON kafka_topic_metrics (cluster_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_cg_lag_cluster
        ON kafka_consumer_group_lag (cluster_id)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_topic_metrics_cluster")
    op.execute("DROP INDEX IF EXISTS ix_cg_lag_cluster")
