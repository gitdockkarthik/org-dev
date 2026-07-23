"""Add SLI/SLO tables
Revision ID: 0017
Revises: 0016
Create Date: 2026-07-24
"""
from alembic import op
import sqlalchemy as sa

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # SLO targets per cluster
    op.create_table(
        "kafka_slo_targets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False, unique=True),
        sa.Column("connector_availability_target", sa.Float(), nullable=False, server_default="99.0"),
        sa.Column("consumer_lag_target", sa.BigInteger(), nullable=False, server_default="10000"),
        sa.Column("broker_availability_target", sa.Float(), nullable=False, server_default="100.0"),
        sa.Column("urp_target", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("min_throughput_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Connector state snapshots (every collection cycle)
    op.create_table(
        "kafka_connector_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("connector_name", sa.String(256), nullable=False),
        sa.Column("connector_type", sa.String(32), nullable=True),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("total_tasks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("running_tasks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_tasks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_connector_snapshots_cluster_time", "kafka_connector_snapshots", ["cluster_id", "collected_at"])
    # Hourly SLO compliance snapshots
    op.create_table(
        "kafka_slo_compliance",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("hour_bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("connector_availability_pct", sa.Float(), nullable=True),
        sa.Column("consumer_lag_compliance_pct", sa.Float(), nullable=True),
        sa.Column("broker_availability_pct", sa.Float(), nullable=True),
        sa.Column("urp_compliance_pct", sa.Float(), nullable=True),
        sa.Column("overall_compliance_pct", sa.Float(), nullable=True),
        sa.Column("connector_total", sa.Integer(), nullable=True),
        sa.Column("connector_running", sa.Integer(), nullable=True),
        sa.Column("connector_failed", sa.Integer(), nullable=True),
        sa.UniqueConstraint("cluster_id", "hour_bucket", name="uq_slo_compliance_cluster_hour"),
    )


def downgrade() -> None:
    op.drop_table("kafka_slo_compliance")
    op.drop_index("ix_connector_snapshots_cluster_time", "kafka_connector_snapshots")
    op.drop_table("kafka_connector_snapshots")
    op.drop_table("kafka_slo_targets")
