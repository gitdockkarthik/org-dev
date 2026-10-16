"""Record broker restarts -- step 1 of the "broker restarted" alert: capture only, no alert type.
Each broker's JMX exporter exposes process_start_time_seconds (the broker JVM start). The
broker-health collector keeps the last seen value in kafka_broker_metrics.process_start_time
and, when a scrape shows a later start, records one row in kafka_broker_restarts. Both live in
the database so an agent restart (every 4 hours) loses nothing and raises no false event.
The unique index on (cluster_id, broker_id, started_at) makes recording idempotent
(INSERT ... ON CONFLICT DO NOTHING); the index on (cluster_id, detected_at) serves "restarts in
the last N minutes". broker_id is the numeric Kafka node id (kafka_broker_metrics.broker_id is
text, so a non-numeric id is never recorded).
The agent runs Base.metadata.create_all() at startup, which creates kafka_broker_restarts if the
new models are deployed before this revision but never adds a column to an existing table; the
collector therefore writes process_start_time in its own savepoint, and this revision tolerates
what already exists: upgrade() adds only what is missing, downgrade() drops only what exists.

Revision ID: 0059
Revises: 0058
Create Date: 2026-10-16
"""
from alembic import op
import sqlalchemy as sa


revision = "0059"
down_revision = "0058"
branch_labels = None
depends_on = None

METRICS = "kafka_broker_metrics"
RESTARTS = "kafka_broker_restarts"
COLUMN = "process_start_time"
UNIQUE_IX = "uq_kafka_broker_restarts_broker_started"
DETECTED_IX = "ix_kafka_broker_restarts_cluster_detected"


def _index_names(table: str) -> list[str]:
    return [i["name"] for i in sa.inspect(op.get_bind()).get_indexes(table)]


def upgrade() -> None:
    insp = sa.inspect(op.get_bind())
    if COLUMN not in [c["name"] for c in insp.get_columns(METRICS)]:
        op.add_column(METRICS, sa.Column(COLUMN, sa.DateTime(timezone=True), nullable=True))
    if not insp.has_table(RESTARTS):
        op.create_table(
            RESTARTS,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("cluster_id", sa.Integer, nullable=False),
            sa.Column("broker_id", sa.Integer, nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("previous_started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        )
    if UNIQUE_IX not in _index_names(RESTARTS):
        op.create_index(UNIQUE_IX, RESTARTS, ["cluster_id", "broker_id", "started_at"], unique=True)
    if DETECTED_IX not in _index_names(RESTARTS):
        op.create_index(DETECTED_IX, RESTARTS, ["cluster_id", "detected_at"])


def downgrade() -> None:
    insp = sa.inspect(op.get_bind())
    if insp.has_table(RESTARTS):
        for name in (DETECTED_IX, UNIQUE_IX):
            if name in _index_names(RESTARTS):
                op.drop_index(name, table_name=RESTARTS)
        op.drop_table(RESTARTS)
    if insp.has_table(METRICS) and COLUMN in [c["name"] for c in insp.get_columns(METRICS)]:
        op.drop_column(METRICS, COLUMN)
