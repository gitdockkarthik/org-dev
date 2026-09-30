"""Add kafka_watchdog_events -- one row per safety-net event (a watchdog that
is about to restart the agent), written just before the restart so the
reason survives it. Docker keeps container logs only for the current run, so
without this the cause of a self-restart is lost (2026-09-29 17:11 restart).
details holds the values the watchdog saw (e.g. stale cluster names and ages).
Best-effort write with a hard time cap; the restart never waits on it.

Revision ID: 0056
Revises: 0055
Create Date: 2026-09-30
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "0056"
down_revision = "0055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_watchdog_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("watchdog", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("details", JSONB, nullable=True),
    )
    op.create_index("ix_kafka_watchdog_events_created", "kafka_watchdog_events", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_kafka_watchdog_events_created", table_name="kafka_watchdog_events")
    op.drop_table("kafka_watchdog_events")
