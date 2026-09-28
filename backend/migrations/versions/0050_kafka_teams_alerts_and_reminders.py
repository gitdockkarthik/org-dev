"""Add three tables for the Kafka Teams integration: kafka_alert_configs
(alert rule definitions -- type, optional cluster scope, severity, JSONB
rule config, optional per-alert Teams webhook override, cooldown) and
kafka_alert_triggers (history of each alert firing and whether the Teams
post succeeded) back Alert Configuration and Reporting;
kafka_teams_reminders (scheduler job_id -> message template and optional
webhook override) backs Schedules and Reminders.

Revision ID: 0050
Revises: 0049
Create Date: 2026-09-28
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "0050"
down_revision = "0049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "kafka_alert_configs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("alert_type", sa.Text(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=True),
        sa.Column("severity", sa.Text(), nullable=False, server_default="warning"),
        sa.Column("config", JSONB, nullable=False, server_default="{}"),
        sa.Column("webhook_url", sa.Text(), nullable=True),
        sa.Column("cooldown_minutes", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "kafka_alert_triggers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "alert_config_id",
            sa.Integer(),
            sa.ForeignKey("kafka_alert_configs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("cluster_id", sa.Integer(), nullable=True),
        sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("metric_value", sa.Text(), nullable=True),
        sa.Column("message_sent", sa.Text(), nullable=True),
        sa.Column("teams_post_success", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("error_detail", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_kafka_alert_triggers_config_time",
        "kafka_alert_triggers",
        ["alert_config_id", sa.text("triggered_at DESC")],
    )

    op.create_table(
        "kafka_teams_reminders",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.Text(), nullable=False, unique=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("message_template", sa.Text(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("kafka_teams_reminders")
    op.drop_index("ix_kafka_alert_triggers_config_time", table_name="kafka_alert_triggers")
    op.drop_table("kafka_alert_triggers")
    op.drop_table("kafka_alert_configs")
