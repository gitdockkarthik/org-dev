"""add must_change_password to operative_users
Revision ID: 0006
Revises: 0005
Create Date: 2026-07-21
"""
import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "operative_users",
        sa.Column("must_change_password", sa.Boolean, nullable=False, server_default="true"),
    )


def downgrade() -> None:
    op.drop_column("operative_users", "must_change_password")
