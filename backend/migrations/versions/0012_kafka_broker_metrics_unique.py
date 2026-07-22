"""Add unique constraint to kafka_broker_metrics
Revision ID: 0012
Revises: 0011
Create Date: 2026-07-22
"""
from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_broker_metrics_cluster_broker",
        "kafka_broker_metrics",
        ["cluster_id", "broker_id"]
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_broker_metrics_cluster_broker",
        "kafka_broker_metrics"
    )
