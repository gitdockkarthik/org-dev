"""add purge audit fields to incident_management.incidents
Revision ID: 0005
Revises: 0004
Create Date: 2026-07-17
"""
import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE incident_management.incidents
        ADD COLUMN IF NOT EXISTS purged_at TIMESTAMP WITH TIME ZONE,
        ADD COLUMN IF NOT EXISTS purge_reason TEXT
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE incident_management.incidents
        DROP COLUMN IF EXISTS purged_at,
        DROP COLUMN IF EXISTS purge_reason
    """)
