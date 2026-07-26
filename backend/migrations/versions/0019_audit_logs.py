"""Add audit_logs table
Revision ID: 0019
Revises: 0018
Create Date: 2026-07-24
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("agent_slug", sa.String(64), nullable=True),
        sa.Column("user_email", sa.String(256), nullable=True),
        sa.Column("user_role", sa.String(64), nullable=True),
        sa.Column("resource_type", sa.String(64), nullable=True),
        sa.Column("resource_id", sa.String(256), nullable=True),
        sa.Column("action", sa.String(64), nullable=True),
        sa.Column("outcome", sa.String(16), nullable=True),
        sa.Column("details", JSONB, nullable=True),
        sa.Column("ip_address", sa.String(64), nullable=True),
    )
    op.create_index("ix_audit_logs_timestamp", "audit_logs", ["timestamp"])
    op.create_index("ix_audit_logs_event_type", "audit_logs", ["event_type"])
    op.create_index("ix_audit_logs_user_email", "audit_logs", ["user_email"])


def downgrade() -> None:
    op.drop_index("ix_audit_logs_user_email", "audit_logs")
    op.drop_index("ix_audit_logs_event_type", "audit_logs")
    op.drop_index("ix_audit_logs_timestamp", "audit_logs")
    op.drop_table("audit_logs")
