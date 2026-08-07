"""Add kafka_topic_partition_size_baseline -- persists collect_msg_rate's per-partition
log-dir size baseline (currently only in-memory), so a restart doesn't force the first
post-restart cycle to skip writing throughput data (no previous baseline to compute a
delta against). Root-caused during the Overview/Topics tab Throughput chart audit:
every observed gap in kafka_topic_bytes_rate_snapshots matched a container restart
during today's own testing. Deliberately scoped smaller/safer than the earlier,
rolled-back kafka_topic_partition_inflow_baseline attempt (a different collector,
collect_topic_message_inflow) -- this collector already uses process-isolated
describe_log_dirs_isolated (no run_in_executor cancellation risk), and this migration
adds the table only; the write-path change and its own timing validation are separate,
subsequent steps.

Revision ID: 0040
Revises: 0039
Create Date: 2026-08-07
"""
from alembic import op
import sqlalchemy as sa


revision = "0040"
down_revision = "0039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_topic_partition_size_baseline",
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("topic", sa.String(), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("cluster_id", "topic", "partition"),
    )


def downgrade() -> None:
    op.drop_table("kafka_topic_partition_size_baseline")
