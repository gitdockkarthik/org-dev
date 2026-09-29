"""Add email_enabled to kafka_alert_configs -- each alert rule gets an
independent Email on/off toggle alongside its existing Teams toggle
(the existing `enabled` column), so a rule can go to Teams only, Email
only, both, or neither (2026-09-29, per explicit request: "hope 3 will
cover which alert to go to what channels, and not general" -- confirmed
correct). Defaults to false for every existing and future rule, since
real SMTP/email-service credentials do not exist yet; this is
configuration scaffolding only, not yet wired to any actual sending
logic.

Revision ID: 0053
Revises: 0052
Create Date: 2026-09-29
"""
from alembic import op
import sqlalchemy as sa


revision = "0053"
down_revision = "0052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_alert_configs", sa.Column("email_enabled", sa.Boolean, nullable=False, server_default="false"))


def downgrade() -> None:
    op.drop_column("kafka_alert_configs", "email_enabled")
