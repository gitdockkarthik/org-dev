"""alert_sync_history - persistent, Postgres-backed classified alert
history, replacing the fragile in-memory cache (capped at 3 entries,
lost on every container restart, fire-and-forget background persist
that could silently drop data). Root cause of untraceable "genuine
alert never got a ticket" gaps found 2026-08-19 - the merge/history
logic depended on this unreliable in-memory state.

Revision ID: 0048
Revises: 0047
Create Date: 2026-08-19
"""
from alembic import op
import sqlalchemy as sa

revision = "0048"
down_revision = "0047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "alert_sync_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("alert_id", sa.String(), nullable=False, unique=True),
        sa.Column("alert_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("classification", sa.String(), nullable=True),
        sa.Column("alert_data", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.Column("synced_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("idx_alert_sync_history_created_at", "alert_sync_history", ["alert_created_at"])
    op.create_index("idx_alert_sync_history_synced_at", "alert_sync_history", ["synced_at"])


def downgrade() -> None:
    op.drop_index("idx_alert_sync_history_synced_at", table_name="alert_sync_history")
    op.drop_index("idx_alert_sync_history_created_at", table_name="alert_sync_history")
    op.drop_table("alert_sync_history")
