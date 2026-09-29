"""Add severity to kafka_alert_triggers -- records which tier (info, warning,
critical) a threshold-based trigger is currently at, so a tier change on an
open trigger can be detected and escalated in place instead of opening a
second trigger. NULL means "use the rule's own severity", which is what every
existing row (close_wait_spike, broker_unreachable) is, so existing behaviour
is unchanged.

Revision ID: 0055
Revises: 0054
Create Date: 2026-09-29
"""
from alembic import op
import sqlalchemy as sa


revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_alert_triggers", sa.Column("severity", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("kafka_alert_triggers", "severity")
