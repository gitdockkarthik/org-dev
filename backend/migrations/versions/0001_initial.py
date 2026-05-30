"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-05-26

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as pgUUID

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agents",
        sa.Column("id", pgUUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("slug", sa.String, nullable=False),
        sa.Column("description", sa.String, nullable=False, server_default=""),
        sa.Column("version", sa.String, nullable=False, server_default="0.1.0"),
        sa.Column("status", sa.String, nullable=False, server_default="draft"),
        sa.Column("invoke_url", sa.String, nullable=True),
        sa.Column("system_prompt", sa.Text, nullable=False, server_default=""),
        sa.Column("model", sa.String, nullable=False, server_default="claude-sonnet-4-6"),
        sa.Column("temperature", sa.Float, nullable=False, server_default="0.7"),
        sa.Column("tools", JSONB, nullable=False, server_default="[]"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("slug", name="uq_agents_slug"),
    )
    op.create_index("ix_agents_slug", "agents", ["slug"])

    op.create_table(
        "agent_versions",
        sa.Column("id", pgUUID(as_uuid=True), primary_key=True),
        sa.Column(
            "agent_id",
            pgUUID(as_uuid=True),
            sa.ForeignKey("agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version", sa.String, nullable=False),
        sa.Column("config_snapshot", JSONB, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("ix_agent_versions_agent_id", "agent_versions", ["agent_id"])

    op.create_table(
        "chat_sessions",
        sa.Column("id", pgUUID(as_uuid=True), primary_key=True),
        sa.Column("session_id", pgUUID(as_uuid=True), nullable=False),
        sa.Column(
            "agent_id",
            pgUUID(as_uuid=True),
            sa.ForeignKey("agents.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("session_id", name="uq_chat_sessions_session_id"),
    )
    op.create_index("ix_chat_sessions_session_id", "chat_sessions", ["session_id"])

    op.create_table(
        "chat_messages",
        sa.Column("id", pgUUID(as_uuid=True), primary_key=True),
        sa.Column(
            "session_id",
            pgUUID(as_uuid=True),
            sa.ForeignKey("chat_sessions.session_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String, nullable=False),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("tokens_used", sa.Integer, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("ix_chat_messages_session_id", "chat_messages", ["session_id"])


def downgrade() -> None:
    op.drop_table("chat_messages")
    op.drop_index("ix_chat_sessions_session_id", "chat_sessions")
    op.drop_table("chat_sessions")
    op.drop_index("ix_agent_versions_agent_id", "agent_versions")
    op.drop_table("agent_versions")
    op.drop_index("ix_agents_slug", "agents")
    op.drop_table("agents")
