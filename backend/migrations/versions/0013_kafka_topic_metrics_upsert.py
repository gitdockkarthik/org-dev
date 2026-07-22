"""Add unique constraint and last_seen to kafka_topic_metrics
Revision ID: 0013
Revises: 0012
Create Date: 2026-07-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add last_seen column
    op.add_column("kafka_topic_metrics",
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=True))
    # Update existing rows to have last_seen = time
    op.execute("UPDATE kafka_topic_metrics SET last_seen = time")
    # Remove duplicates — keep only the latest row per (cluster_id, topic)
    op.execute("""
        DELETE FROM kafka_topic_metrics
        WHERE id NOT IN (
            SELECT DISTINCT ON (cluster_id, topic) id
            FROM kafka_topic_metrics
            ORDER BY cluster_id, topic, time DESC
        )
    """)
    # Add unique constraint for upsert support
    op.create_unique_constraint(
        "uq_kafka_topic_metrics_cluster_topic",
        "kafka_topic_metrics",
        ["cluster_id", "topic"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_kafka_topic_metrics_cluster_topic", "kafka_topic_metrics")
    op.drop_column("kafka_topic_metrics", "last_seen")
