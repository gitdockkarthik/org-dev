"""alert_lifetime_totals — split into raw and alias-deduplicated columns

Revision ID: 0042
Revises: 0041
Create Date: 2026-08-10
"""
from alembic import op
import sqlalchemy as sa

revision = "0042"
down_revision = "0041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("alert_lifetime_totals", "total_alerts", new_column_name="total_alerts_raw")
    op.alter_column("alert_lifetime_totals", "genuine_count", new_column_name="genuine_count_raw")
    op.alter_column("alert_lifetime_totals", "noise_count", new_column_name="noise_count_raw")
    op.alter_column("alert_lifetime_totals", "suspect_count", new_column_name="suspect_count_raw")

    op.add_column("alert_lifetime_totals", sa.Column("total_alerts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("alert_lifetime_totals", sa.Column("genuine_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("alert_lifetime_totals", sa.Column("noise_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("alert_lifetime_totals", sa.Column("suspect_count", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    op.drop_column("alert_lifetime_totals", "total_alerts")
    op.drop_column("alert_lifetime_totals", "genuine_count")
    op.drop_column("alert_lifetime_totals", "noise_count")
    op.drop_column("alert_lifetime_totals", "suspect_count")

    op.alter_column("alert_lifetime_totals", "total_alerts_raw", new_column_name="total_alerts")
    op.alter_column("alert_lifetime_totals", "genuine_count_raw", new_column_name="genuine_count")
    op.alter_column("alert_lifetime_totals", "noise_count_raw", new_column_name="noise_count")
    op.alter_column("alert_lifetime_totals", "suspect_count_raw", new_column_name="suspect_count")
