"""Add resolution tracking to kafka_alert_triggers: resolved_at (NULL while
the trigger is still open/firing, otherwise the time it resolved) and
is_recurrence (true when this trigger fired again shortly after a previous
trigger for the same alert_config_id + cluster_id had resolved -- a
flapping signal).

Revision ID: 0051
Revises: 0050
Create Date: 2026-09-28
"""
from alembic import op
import sqlalchemy as sa


revision = "0051"
down_revision = "0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_alert_triggers", sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("kafka_alert_triggers", sa.Column("is_recurrence", sa.Boolean(), nullable=False, server_default="false"))


def downgrade() -> None:
    op.drop_column("kafka_alert_triggers", "is_recurrence")
    op.drop_column("kafka_alert_triggers", "resolved_at")
