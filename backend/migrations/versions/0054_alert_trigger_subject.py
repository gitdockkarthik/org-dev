"""Add subject to kafka_alert_triggers -- identifies WHAT a trigger is about
when a rule fires per item rather than per cluster (e.g. the broker for
broker reachability, later the topic for replication factor). NULL means a
cluster-level trigger, which is what every existing row (close_wait_spike)
is, so existing behaviour is unchanged.

Revision ID: 0054
Revises: 0053
Create Date: 2026-09-29
"""
from alembic import op
import sqlalchemy as sa


revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_alert_triggers", sa.Column("subject", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("kafka_alert_triggers", "subject")
