"""add agent_owners table
Revision ID: 0007
Revises: 0006
Create Date: 2026-07-21
"""
import sqlalchemy as sa
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_owners",
        sa.Column("agent_slug", sa.String, nullable=False),
        sa.Column("user_email", sa.String, nullable=False),
        sa.Column("assigned_by", sa.String, nullable=False),
        sa.Column(
            "assigned_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("agent_slug", "user_email", name="pk_agent_owners"),
    )
    op.create_index("ix_agent_owners_slug", "agent_owners", ["agent_slug"])
    op.create_index("ix_agent_owners_email", "agent_owners", ["user_email"])


def downgrade() -> None:
    op.drop_index("ix_agent_owners_email", table_name="agent_owners")
    op.drop_index("ix_agent_owners_slug", table_name="agent_owners")
    op.drop_table("agent_owners")
