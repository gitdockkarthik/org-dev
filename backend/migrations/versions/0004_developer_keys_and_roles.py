"""add developer_keys table and migrate role to roles
Revision ID: 0004
Revises: 0003
Create Date: 2026-07-17
"""
import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add roles column with backfill from role
    op.add_column(
        "operative_users",
        sa.Column("roles", sa.String, nullable=True),
    )
    op.execute("UPDATE operative_users SET roles = role")
    op.alter_column("operative_users", "roles", nullable=False)
    op.drop_column("operative_users", "role")

    # 2. Create developer_keys table
    op.create_table(
        "developer_keys",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("agent_slug", sa.String, nullable=False, index=True),
        sa.Column("key_prefix", sa.String(12), nullable=False),
        sa.Column("key_hash", sa.String, nullable=False, unique=True),
        sa.Column("label", sa.String, nullable=True),
        sa.Column("created_by", sa.String, nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("developer_keys")
    op.add_column(
        "operative_users",
        sa.Column("role", sa.String, nullable=True),
    )
    op.execute("UPDATE operative_users SET role = roles")
    op.alter_column("operative_users", "role", nullable=False)
    op.drop_column("operative_users", "roles")
