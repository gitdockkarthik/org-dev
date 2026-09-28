"""Add per_job_failures to kafka_cluster_breaker_state: consecutive failures
tracked per job-type prefix independently (e.g. {"broker-health": 0,
"consumer-lag": 3, ...}), so one job type succeeding can no longer reset
the count and mask another job type's repeated failures from ever
tripping the breaker (confirmed live 2026-09-28: consumer-lag-4 failed
continuously for over an hour while broker-health-4 kept succeeding, and
the breaker never tripped). The existing consecutive_failures column is
left in place unchanged -- still updated, now representing the max across
all tracked job types, for backward compatibility with existing
dashboard/UI code that reads it.

Revision ID: 0052
Revises: 0051
Create Date: 2026-09-28
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "0052"
down_revision = "0051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_cluster_breaker_state", sa.Column("per_job_failures", JSONB, nullable=False, server_default="{}"))


def downgrade() -> None:
    op.drop_column("kafka_cluster_breaker_state", "per_job_failures")
