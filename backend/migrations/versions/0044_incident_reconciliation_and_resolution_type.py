"""incidents — live reconciliation tracking + explicit resolution_type

Adds last_reconciliation_check_at (tracks when we last verified a ticket's
real OpsGenie state via live per-ticket lookup, replacing the bounded-window
snapshot comparison that was falsely auto-resolving incidents older than
~4 hours) and resolution_type (explicit self_healed/rca_assisted/
action_resolved/manual classification, replacing inference).

Revision ID: 0044
Revises: 0043
Create Date: 2026-08-17
"""
from alembic import op
import sqlalchemy as sa

revision = "0044"
down_revision = "0043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "incidents",
        sa.Column("last_reconciliation_check_at", sa.DateTime(timezone=True), nullable=True),
        schema="incident_management",
    )
    op.add_column(
        "incidents",
        sa.Column("resolution_type", sa.String(), nullable=True),
        schema="incident_management",
    )
    op.create_index(
        "idx_incidents_reconciliation_check",
        "incidents",
        ["last_reconciliation_check_at"],
        schema="incident_management",
    )


def downgrade() -> None:
    op.drop_index("idx_incidents_reconciliation_check", table_name="incidents", schema="incident_management")
    op.drop_column("incidents", "resolution_type", schema="incident_management")
    op.drop_column("incidents", "last_reconciliation_check_at", schema="incident_management")
