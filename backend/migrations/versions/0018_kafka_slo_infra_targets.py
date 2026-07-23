"""Add infra SLO targets to kafka_slo_targets
Revision ID: 0018
Revises: 0017
Create Date: 2026-07-24
"""
from alembic import op
import sqlalchemy as sa

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_slo_targets", sa.Column("max_broker_cpu_pct", sa.Float(), nullable=True, server_default="85.0"))
    op.add_column("kafka_slo_targets", sa.Column("max_broker_heap_pct", sa.Float(), nullable=True, server_default="80.0"))
    op.add_column("kafka_slo_targets", sa.Column("min_task_health_pct", sa.Float(), nullable=True, server_default="95.0"))
    op.add_column("kafka_slo_targets", sa.Column("max_failed_tasks", sa.Integer(), nullable=True, server_default="0"))
    # New compliance columns in kafka_slo_compliance
    op.add_column("kafka_slo_compliance", sa.Column("broker_cpu_compliance_pct", sa.Float(), nullable=True))
    op.add_column("kafka_slo_compliance", sa.Column("broker_heap_compliance_pct", sa.Float(), nullable=True))
    op.add_column("kafka_slo_compliance", sa.Column("task_health_compliance_pct", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("kafka_slo_compliance", "task_health_compliance_pct")
    op.drop_column("kafka_slo_compliance", "broker_heap_compliance_pct")
    op.drop_column("kafka_slo_compliance", "broker_cpu_compliance_pct")
    op.drop_column("kafka_slo_targets", "max_failed_tasks")
    op.drop_column("kafka_slo_targets", "min_task_health_pct")
    op.drop_column("kafka_slo_targets", "max_broker_heap_pct")
    op.drop_column("kafka_slo_targets", "max_broker_cpu_pct")
