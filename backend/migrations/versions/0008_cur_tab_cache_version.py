"""add cache_version to cur_tab_cache
Revision ID: 0008
Revises: 0007
Create Date: 2026-07-21
"""
import sqlalchemy as sa
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE cur_tab_cache
        DROP CONSTRAINT IF EXISTS uq_cur_tab_cache
    """)
    op.execute("""
        ALTER TABLE cur_tab_cache
        ADD COLUMN IF NOT EXISTS cache_version VARCHAR(16) NOT NULL DEFAULT 'v1'
    """)
    op.execute("""
        ALTER TABLE cur_tab_cache
        ADD CONSTRAINT uq_cur_tab_cache
        UNIQUE (report_id, tab_name, enrichment_enabled, cache_version)
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE cur_tab_cache DROP CONSTRAINT IF EXISTS uq_cur_tab_cache")
    op.execute("ALTER TABLE cur_tab_cache DROP COLUMN IF EXISTS cache_version")
    op.execute("""
        ALTER TABLE cur_tab_cache
        ADD CONSTRAINT uq_cur_tab_cache
        UNIQUE (report_id, tab_name, enrichment_enabled)
    """)
