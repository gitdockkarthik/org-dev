"""cur_report add sync_type column
Revision ID: 0010
Revises: 0009
Create Date: 2026-07-21
"""
from alembic import op
import sqlalchemy as sa

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cur_report", sa.Column("sync_type", sa.String(16), nullable=False, server_default="manual"))


def downgrade() -> None:
    op.drop_column("cur_report", "sync_type")
