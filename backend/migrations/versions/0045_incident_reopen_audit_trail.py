"""incidents — reopen audit trail for false-positive resolution correction

Adds reopened_at and reopen_reason columns to track tickets that were
falsely auto-resolved under the old bounded-window snapshot comparison
logic (fixed in 0044/reconciliation rewrite) and are being corrected
back to ESCALATED after live OpsGenie verification confirmed they were
still genuinely open.

Revision ID: 0045
Revises: 0044
Create Date: 2026-08-17
"""
from alembic import op
import sqlalchemy as sa

revision = "0045"
down_revision = "0044"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "incidents",
        sa.Column("reopened_at", sa.DateTime(timezone=True), nullable=True),
        schema="incident_management",
    )
    op.add_column(
        "incidents",
        sa.Column("reopen_reason", sa.String(), nullable=True),
        schema="incident_management",
    )


def downgrade() -> None:
    op.drop_column("incidents", "reopen_reason", schema="incident_management")
    op.drop_column("incidents", "reopened_at", schema="incident_management")
