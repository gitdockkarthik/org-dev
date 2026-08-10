"""alert_lifetime_totals table

Revision ID: 0041
Revises: 0040
Create Date: 2026-08-10
"""
from alembic import op
import sqlalchemy as sa

revision = "0041"
down_revision = "0040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "alert_lifetime_totals",
        sa.Column("agent_slug", sa.String(), primary_key=True, nullable=False),
        sa.Column("total_alerts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("genuine_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("noise_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("suspect_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("counting_since", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_cleanup_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("alert_lifetime_totals")
