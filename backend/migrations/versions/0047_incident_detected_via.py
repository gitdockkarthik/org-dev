"""incidents.detected_via - tracks whether a resolution was detected via
message-text parsing (fast, no human OpsGenie action needed) or live
OpsGenie status check (fallback for sources not yet using the
standardized bracket format).

Revision ID: 0047
Revises: 0046
Create Date: 2026-08-19
"""
from alembic import op
import sqlalchemy as sa

revision = "0047"
down_revision = "0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "incidents",
        sa.Column("detected_via", sa.String(), nullable=True),
        schema="incident_management",
    )
    # Placeholder for future auto-close-OpsGenie feature (not implemented yet,
    # per user decision 2026-08-19 - reconciles agent status back to OpsGenie
    # automatically once built). Column reserved now so no future migration
    # is needed just to add tracking for it.
    op.add_column(
        "incidents",
        sa.Column("opsgenie_sync_status", sa.String(), nullable=True),
        schema="incident_management",
    )


def downgrade() -> None:
    op.drop_column("incidents", "opsgenie_sync_status", schema="incident_management")
    op.drop_column("incidents", "detected_via", schema="incident_management")
