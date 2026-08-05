"""Add agent_access table for per-user agent catalogue visibility control
(separate from AgentOwner/developer-ownership model; mirrors agent_owners schema exactly --
same columns and primary key structure for agent visibility scoping)

Revision ID: 0038
Revises: 0037
Create Date: 2026-08-05
"""
from alembic import op
import sqlalchemy as sa


revision = "0038"
down_revision = "0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_access",
        sa.Column("agent_slug", sa.String, nullable=False),
        sa.Column("user_email", sa.String, nullable=False),
        sa.Column("assigned_by", sa.String, nullable=False),
        sa.Column(
            "assigned_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("agent_slug", "user_email", name="pk_agent_access"),
    )
    op.create_index("ix_agent_access_slug", "agent_access", ["agent_slug"])
    op.create_index("ix_agent_access_email", "agent_access", ["user_email"])


def downgrade() -> None:
    op.drop_index("ix_agent_access_email", table_name="agent_access")
    op.drop_index("ix_agent_access_slug", table_name="agent_access")
    op.drop_table("agent_access")
