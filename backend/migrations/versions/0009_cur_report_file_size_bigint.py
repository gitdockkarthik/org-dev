"""cur_report file_size to BIGINT
Revision ID: 0009
Revises: 0008
Create Date: 2026-07-21
"""
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE cur_report ALTER COLUMN file_size TYPE BIGINT")


def downgrade() -> None:
    op.execute("ALTER TABLE cur_report ALTER COLUMN file_size TYPE INTEGER")
