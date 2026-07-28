"""Add SR auth fields to kafka_clusters
Revision ID: 0023
Revises: 0022
Create Date: 2026-07-28
"""
from alembic import op
import sqlalchemy as sa

revision = "0023"
down_revision = "0019"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column("kafka_clusters", sa.Column("schema_registry_username", sa.Text(), nullable=True))
    op.add_column("kafka_clusters", sa.Column("schema_registry_password", sa.Text(), nullable=True))

def downgrade() -> None:
    op.drop_column("kafka_clusters", "schema_registry_password")
    op.drop_column("kafka_clusters", "schema_registry_username")
