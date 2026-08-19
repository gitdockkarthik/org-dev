"""incident_status_history + incident_creation_failures - powers MTTA,
flow-map popup, and Failed Incident Creation tab for the rebuilt
Escalated Incidents dashboard.

Revision ID: 0046
Revises: 0045
Create Date: 2026-08-18
"""
from alembic import op
import sqlalchemy as sa

revision = "0046"
down_revision = "0045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "incident_status_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("incident_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("from_status", sa.String(), nullable=True),
        sa.Column("to_status", sa.String(), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema="incident_management",
    )
    op.create_index(
        "idx_status_history_incident_id",
        "incident_status_history",
        ["incident_id"],
        schema="incident_management",
    )

    op.create_table(
        "incident_creation_failures",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("alert_id", sa.String(), nullable=False),
        sa.Column("alert_title", sa.Text(), nullable=True),
        sa.Column("alert_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=False),
        sa.Column("alert_payload", sa.Text(), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("retriggered", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("retrigger_status", sa.String(), nullable=True),
        sa.Column("retriggered_at", sa.DateTime(timezone=True), nullable=True),
        schema="incident_management",
    )


def downgrade() -> None:
    op.drop_table("incident_creation_failures", schema="incident_management")
    op.drop_index("idx_status_history_incident_id", table_name="incident_status_history", schema="incident_management")
    op.drop_table("incident_status_history", schema="incident_management")
