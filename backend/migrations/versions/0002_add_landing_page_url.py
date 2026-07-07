"""add landing_page_url to agents

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-08
"""
import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("landing_page_url", sa.String, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("agents", "landing_page_url")
