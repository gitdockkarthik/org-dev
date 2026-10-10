"""Add kafka_notification_outbox and kafka_notification_attempts -- a durable record of every
Kafka Analyser owner notice (hourly health summary, watchdog restart notice) and alert rule
notification (fire, escalate, reminder, resolve), with per-channel status for email and Teams,
and one attempts row per channel attempt. On 2026-10-10 03:27 UTC the health summary went to
Teams with no email and nothing explained why: the inline sends keep no record, and the
container log (3 x 10 MB) holds only seconds of output. Step 1 only records what the inline
sends did (origin 'inline', finished on insert); a later step adds the delivery worker that
sends origin 'outbox' rows.
dedupe_key is unique so recording is idempotent (INSERT ... ON CONFLICT DO NOTHING). Rows
have no foreign key to kafka_alert_triggers or kafka_alert_configs, so the trigger purge is
never blocked; attempts cascade with their outbox row. The CHECK lists repeat models.py
verbatim.
The agent runs Base.metadata.create_all() at startup, so if the new models are deployed before
this revision is applied the tables already exist; that case is tolerated (nothing is dropped
or changed), so the order of the two cannot break either. downgrade() is tolerant in the same
way: it drops only what exists, attempts first.

Revision ID: 0058
Revises: 0057
Create Date: 2026-10-10
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB


revision = "0058"
down_revision = "0057"
branch_labels = None
depends_on = None

OUTBOX = "kafka_notification_outbox"
ATTEMPTS = "kafka_notification_attempts"
STATUS_SQL = "('pending', 'standby', 'sending', 'sent', 'failed', 'skipped', 'abandoned', 'superseded')"

# (name, table, columns, postgresql_where)
INDEXES = [
    ("ix_kafka_notification_outbox_due", OUTBOX, ["priority", "next_attempt_at"], "finished_at IS NULL"),
    ("ix_kafka_notification_outbox_created_at", OUTBOX, ["created_at"], None),
    ("ix_kafka_notification_outbox_config_created", OUTBOX, ["alert_config_id", "created_at"], None),
    ("ix_kafka_notification_attempts_outbox_id", ATTEMPTS, ["outbox_id"], None),
    ("ix_kafka_notification_attempts_started_at", ATTEMPTS, ["started_at"], None),
]


def _ts(name, nullable=True, now=False):
    return sa.Column(
        name, sa.DateTime(timezone=True), nullable=nullable,
        server_default=sa.text("now()") if now else None,
    )


def upgrade() -> None:
    if not sa.inspect(op.get_bind()).has_table(OUTBOX):
        op.create_table(
            OUTBOX,
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            _ts("created_at", nullable=False, now=True),
            _ts("updated_at", nullable=False, now=True),
            sa.Column("kind", sa.Text, nullable=False),
            sa.Column("source", sa.Text, nullable=False),
            sa.Column("routing", sa.Text, nullable=False),
            sa.Column("origin", sa.Text, nullable=False),
            sa.Column("dedupe_key", sa.Text, nullable=False),
            sa.Column("priority", sa.SmallInteger, nullable=False, server_default=sa.text("2")),
            sa.Column("alert_config_id", sa.Integer, nullable=True),
            sa.Column("cluster_id", sa.Integer, nullable=True),
            sa.Column("trigger_ids", ARRAY(sa.Integer), nullable=True),
            sa.Column("depends_on_id", sa.BigInteger, nullable=True),
            sa.Column("severity", sa.Text, nullable=True),
            sa.Column("payload", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("email_status", sa.Text, nullable=False),
            sa.Column("teams_status", sa.Text, nullable=False),
            sa.Column("email_attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
            sa.Column("teams_attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
            sa.Column("email_last_error", sa.Text, nullable=True),
            sa.Column("teams_last_error", sa.Text, nullable=True),
            sa.Column("email_warning", sa.Text, nullable=True),
            _ts("email_sent_at"),
            _ts("teams_sent_at"),
            sa.Column("email_message_id", sa.Text, nullable=True),
            _ts("next_attempt_at", nullable=False, now=True),
            sa.Column("claimed_by", sa.Text, nullable=True),
            _ts("claim_expires_at"),
            _ts("expires_at", nullable=False),
            sa.Column("state", sa.Text, nullable=False),
            _ts("delivered_at"),
            _ts("finished_at"),
            _ts("reported_at"),
            sa.UniqueConstraint("dedupe_key", name="uq_kafka_notification_outbox_dedupe_key"),
            sa.CheckConstraint(
                "kind IN ('owner_summary', 'owner_watchdog', 'rule_fire', 'rule_escalate', "
                "'rule_reminder', 'rule_resolve', 'heartbeat', 'deadman')",
                name="ck_kafka_notification_outbox_kind",
            ),
            sa.CheckConstraint("routing IN ('owner', 'rule')", name="ck_kafka_notification_outbox_routing"),
            sa.CheckConstraint("origin IN ('inline', 'outbox')", name="ck_kafka_notification_outbox_origin"),
            sa.CheckConstraint(f"email_status IN {STATUS_SQL}", name="ck_kafka_notification_outbox_email_status"),
            sa.CheckConstraint(f"teams_status IN {STATUS_SQL}", name="ck_kafka_notification_outbox_teams_status"),
            sa.CheckConstraint(
                "state IN ('pending', 'delivered', 'degraded', 'failed', 'suppressed')",
                name="ck_kafka_notification_outbox_state",
            ),
        )
    if not sa.inspect(op.get_bind()).has_table(ATTEMPTS):
        op.create_table(
            ATTEMPTS,
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            sa.Column(
                "outbox_id", sa.BigInteger,
                sa.ForeignKey(f"{OUTBOX}.id", ondelete="CASCADE", name="fk_kafka_notification_attempts_outbox_id"),
                nullable=False,
            ),
            sa.Column("channel", sa.Text, nullable=False),
            sa.Column("attempt_no", sa.Integer, nullable=False),
            sa.Column("boot_id", sa.Text, nullable=True),
            _ts("started_at"),
            _ts("ended_at"),
            sa.Column("duration_ms", sa.Integer, nullable=True),
            sa.Column("phase_reached", sa.Text, nullable=True),
            sa.Column("outcome", sa.Text, nullable=True),
            sa.Column("error", sa.Text, nullable=True),
            sa.Column("detail", JSONB, nullable=True),
            _ts("created_at", nullable=False, now=True),
            sa.CheckConstraint("channel IN ('email', 'teams')", name="ck_kafka_notification_attempts_channel"),
            sa.CheckConstraint(
                "outcome IS NULL OR outcome IN ('sent', 'failed_transient', 'failed_config', "
                "'failed_permanent', 'timeout', 'unknown', 'skipped')",
                name="ck_kafka_notification_attempts_outcome",
            ),
        )
    for name, table, columns, where in INDEXES:
        if name not in [i["name"] for i in sa.inspect(op.get_bind()).get_indexes(table)]:
            op.create_index(
                name, table, columns,
                postgresql_where=sa.text(where) if where else None,
            )


def downgrade() -> None:
    for name, table, _columns, _where in reversed(INDEXES):
        insp = sa.inspect(op.get_bind())
        if insp.has_table(table) and name in [i["name"] for i in insp.get_indexes(table)]:
            op.drop_index(name, table_name=table)
    for table in (ATTEMPTS, OUTBOX):
        if sa.inspect(op.get_bind()).has_table(table):
            op.drop_table(table)
