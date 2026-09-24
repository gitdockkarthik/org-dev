"""Catch-up migration for three kafka-analyser tables created via
SQLAlchemy's create_all() (main.py's startup fallback) rather than through
this project's proper, shared Alembic chain -- confirmed via grep that no
prior migration covers them. Schemas below match the live database exactly
(verified via \\d against each table before writing this), so this is a
faithful catch-up definition, not a new design: kafka_cluster_breaker_state
and kafka_cluster_breaker_events back the per-cluster circuit breaker
(auto-pause after 5 consecutive connection failures, auto-resume after 5
consecutive successful checks); kafka_connection_events is a new audit log
of persistent Kafka client create/close events, built following a real
connection-leak incident (2026-09-24) to give provable, ongoing evidence
of correct connection-closing discipline. All three already exist live in
the database (created via create_all() before this migration was written)
-- this revision should be applied via `alembic stamp 0049`, not `alembic
upgrade head`, to mark it applied without re-running CREATE TABLE against
tables that already exist matching this exact definition.

Revision ID: 0049
Revises: 0048
Create Date: 2026-09-24
"""
from alembic import op
import sqlalchemy as sa


revision = "0049"
down_revision = "0048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_cluster_breaker_state",
        sa.Column("cluster_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("paused", sa.Boolean(), nullable=False),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paused_reason", sa.Text(), nullable=True),
        sa.Column("recovery_successes", sa.Integer(), nullable=False),
        sa.Column("last_recovery_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "kafka_cluster_breaker_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(16), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "kafka_connection_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("bootstrap_servers", sa.Text(), nullable=False),
        sa.Column("event_type", sa.String(16), nullable=False),
        sa.Column("client_type", sa.String(16), nullable=False),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("context", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("kafka_connection_events")
    op.drop_table("kafka_cluster_breaker_events")
    op.drop_table("kafka_cluster_breaker_state")
