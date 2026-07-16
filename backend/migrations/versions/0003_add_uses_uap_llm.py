"""add uses_uap_llm to agents

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-09
"""
import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("uses_uap_llm", sa.Boolean, nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("agents", "uses_uap_llm")
