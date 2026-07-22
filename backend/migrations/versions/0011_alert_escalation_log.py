"""alert_escalation_log table
Revision ID: 0011
Revises: 0010
Create Date: 2026-07-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "alert_escalation_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("agent_slug", sa.String(64), nullable=False, server_default="alert-analyser"),
        sa.Column("channel", sa.String(16), nullable=False),  # teams / email
        sa.Column("severity", sa.String(16), nullable=True),
        sa.Column("alert_count", sa.Integer(), nullable=False, default=0),
        sa.Column("message_summary", sa.Text(), nullable=True),
        sa.Column("recipients", sa.Text(), nullable=True),  # webhook URL or email list
        sa.Column("status", sa.String(16), nullable=False, default="sent"),  # sent / failed
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("idx_escalation_log_sent_at", "alert_escalation_log", ["sent_at"])
    op.create_index("idx_escalation_log_agent", "alert_escalation_log", ["agent_slug"])


def downgrade() -> None:
    op.drop_table("alert_escalation_log")
