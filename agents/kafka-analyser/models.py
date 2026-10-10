from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import BigInteger, Boolean, CheckConstraint, DateTime, Float, Index, Integer, SmallInteger, String, Text, UniqueConstraint, ForeignKey, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class AgentConfig(Base):
    """Shared config table — scoped by agent_slug. Identical to alert-analyser definition."""

    __tablename__ = "agent_config"
    __table_args__ = (UniqueConstraint("agent_slug", "key", name="uq_agent_config_slug_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    agent_slug: Mapped[str] = mapped_column(String(64), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class KafkaCluster(Base):
    __tablename__ = "kafka_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    agent_slug: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    environment: Mapped[str] = mapped_column(String(32), nullable=False, default="internal")
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default="kafka_internal")
    bootstrap_servers: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auth_type: Mapped[str] = mapped_column(String(32), nullable=False, default="none")
    sasl_username: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sasl_password: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sasl_mechanism: Mapped[str] = mapped_column(String(32), nullable=False, default="PLAIN")
    tls_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    schema_registry_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    schema_registry_username: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    schema_registry_password: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sr_restricted: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    zookeeper_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    kafka_connect_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    jmx_port: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    prometheus_port: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cpu_cores: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    max_inflow_partitions_per_cycle: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mirror_source_cluster_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mirror_mode: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    config_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="unchecked")
    last_tested_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )


class KafkaBrokerMetrics(Base):
    __tablename__ = "kafka_broker_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    broker_id: Mapped[str] = mapped_column(String(64), nullable=False)
    heap_pct: Mapped[float] = mapped_column(Float, nullable=False)
    gc_pause_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    request_handler_idle_pct: Mapped[float] = mapped_column(Float, nullable=False, default=100.0)
    urp_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    messages_in_per_sec: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cpu_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    disk_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class KafkaConsumerLag(Base):
    __tablename__ = "kafka_consumer_lag"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    group_name: Mapped[str] = mapped_column(String(128), nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    lag: Mapped[int] = mapped_column(BigInteger, nullable=False)
    log_end_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    consumer_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    group_state: Mapped[str] = mapped_column(String(32), nullable=False)


class KafkaTopicMetrics(Base):
    __tablename__ = "kafka_topic_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    partition_count: Mapped[int] = mapped_column(Integer, nullable=False)
    replication_factor: Mapped[int] = mapped_column(Integer, nullable=False)
    messages_in_per_sec: Mapped[float] = mapped_column(Float, nullable=False)
    bytes_in_per_sec: Mapped[float] = mapped_column(Float, nullable=False)
    bytes_out_per_sec: Mapped[float] = mapped_column(Float, nullable=False)
    total_messages: Mapped[int] = mapped_column(BigInteger, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    retention_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    retention_pct: Mapped[float] = mapped_column(Float, nullable=False)


class KafkaConnectorStatus(Base):
    __tablename__ = "kafka_connector_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    connector_name: Mapped[str] = mapped_column(String(128), nullable=False)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    failed_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_tasks: Mapped[int] = mapped_column(Integer, nullable=False)


class KafkaAnomaly(Base):
    __tablename__ = "kafka_anomalies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class KafkaTopicMetricsHourly(Base):
    __tablename__ = "kafka_topic_metrics_hourly"
    __table_args__ = (
        UniqueConstraint("cluster_id", "topic", "hour_bucket", name="uq_topic_metrics_hourly"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    topic: Mapped[str] = mapped_column(String(256), nullable=False)
    hour_bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    avg_msgs: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    max_msgs: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class KafkaTopicName(Base):
    __tablename__ = "kafka_topic_names"
    __table_args__ = (
        UniqueConstraint("cluster_id", "topic", name="uq_topic_names"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    topic: Mapped[str] = mapped_column(String(256), nullable=False)
    partition_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    replication_factor: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaJobSchedule(Base):
    __tablename__ = "kafka_job_schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), nullable=False)
    cron_expression: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    timeout_secs: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaJobRun(Base):
    __tablename__ = "kafka_job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), nullable=False)
    triggered_by: Mapped[str] = mapped_column(String(16), nullable=False, default="schedule")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    logs: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaClusterBreakerState(Base):
    """Per-cluster circuit-breaker state. Tracks consecutive broker-connection
    job failures (broker-health, consumer-lag, topic-structure, msg-rate,
    topic-inflow, topic-sizes -- the job types that make raw Kafka-protocol connections via
    the worker-process pool, as opposed to REST-based or DB-only job types).
    After enough consecutive failures, the cluster's job dispatch is paused
    (kafka_job_schedules stays untouched -- pausing happens at dispatch time,
    not by disabling schedules) to stop contributing further connections while
    the broker is struggling. A separate, lightweight TCP-only recovery check
    (no Kafka protocol handshake, cannot itself leak or add load) resumes
    dispatch automatically after enough consecutive successful checks.
    Persisted (not in-memory) so a container restart cannot silently
    un-pause a cluster that is still genuinely unreachable."""
    __tablename__ = "kafka_cluster_breaker_state"

    cluster_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Consecutive failures tracked per job-type prefix independently (e.g.
    # {"broker-health": 0, "consumer-lag": 3, ...}), added 2026-09-28 after
    # a confirmed incident where the single shared consecutive_failures
    # counter above let one job type succeeding reset the count, masking
    # another job type's repeated failures from ever tripping the breaker.
    # consecutive_failures is still updated (now as the max across all
    # tracked job types) for backward compatibility with existing
    # dashboard/UI code that reads it directly.
    per_job_failures: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    paused_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    recovery_successes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_recovery_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaClusterBreakerEvent(Base):
    """Append-only audit log of circuit-breaker trip/resume events, one row
    per event (not overwritten, unlike kafka_cluster_breaker_state which only
    holds current state). Source of truth for the breaker-status dashboard
    tab and future Teams/email alerting on trip/resume events."""
    __tablename__ = "kafka_cluster_breaker_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(16), nullable=False)  # "tripped" | "resumed"
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    consecutive_failures: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaConnectionEvent(Base):
    """Audit log of persistent Kafka client lifecycle events (create/close),
    across both the main-process shared client cache (shared_kafka_clients.py)
    and the worker-process pool cache (kafka_process_pool.py). Built in
    response to a real connection-leak incident (2026-09-24) to give
    provable, queryable evidence that connections are being closed
    correctly going forward -- not just point-in-time spot checks.
    context='startup' marks events from the first few minutes after a
    process/worker started (likely warm-up activity); 'normal' marks
    everything after."""
    __tablename__ = "kafka_connection_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bootstrap_servers: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(String(16), nullable=False)  # "created" | "closed"
    client_type: Mapped[str] = mapped_column(String(16), nullable=False)  # "admin" | "consumer"
    source: Mapped[str] = mapped_column(String(16), nullable=False)  # "shared" (main process) | "worker" (process pool)
    context: Mapped[str] = mapped_column(String(16), nullable=False, default="normal")  # "startup" | "normal"
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaAlertConfig(Base):
    """Condition-based alert rule backing the "Alert Configuration and
    Reporting" Teams tab. Each rule is evaluated periodically and notifies
    only when its condition is true -- not a fixed-schedule message (see
    kafka_teams_reminders for those). cluster_id NULL means the rule applies
    to all clusters. webhook_url NULL falls back to the agent-level default
    Teams webhook. Replaces the old global Teams severity filter/cooldown
    settings, which are now per-rule (severity, cooldown_minutes). `enabled`
    is the Teams toggle; `email_enabled` is an independent per-rule Email
    toggle (scaffolding only, not yet wired to sending)."""
    __tablename__ = "kafka_alert_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    alert_type: Mapped[str] = mapped_column(Text, nullable=False)  # rule kind; values not yet defined in code
    cluster_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    severity: Mapped[str] = mapped_column(Text, nullable=False, default="warning")  # "critical" | "warning" | "info"
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    webhook_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    cooldown_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Independent Email on/off toggle alongside `enabled` (Teams) -- a
    # rule can go to Teams only, Email only, both, or neither. Added
    # 2026-09-29 as configuration scaffolding for a future Email
    # channel; defaults to False since real SMTP credentials don't
    # exist yet.
    email_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaAlertTrigger(Base):
    """Firing/resolution history for kafka_alert_configs, one row per firing
    -- backs the tab's "last 10 + show more" view. Also drives the
    notification model agreed 2026-09-28: an immediate Teams card on firing,
    only if no other trigger for the same alert_config_id + cluster_id is
    already open (resolved_at NULL); on resolution a resolve card is sent
    through teams_alerts._send_resolve_cards (only when the original card
    reached Teams and the rule allows it).
    is_recurrence=True marks a trigger that fired again shortly after the
    same alert+cluster's previous trigger had resolved (flapping signal)."""
    __tablename__ = "kafka_alert_triggers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_config_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("kafka_alert_configs.id", ondelete="CASCADE"), nullable=False
    )
    cluster_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # What this trigger is about when a rule fires per item rather than per
    # cluster (broker for reachability, topic for replication factor). NULL
    # means a cluster-level trigger -- every existing close_wait_spike row.
    subject: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Current tier of a threshold-based trigger ("info" | "warning" |
    # "critical"), so a tier change on an open trigger can be detected and
    # escalated in place. NULL = use the rule's own severity (every existing
    # close_wait_spike and broker_unreachable row).
    severity: Mapped[str | None] = mapped_column(Text, nullable=True)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metric_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_sent: Mapped[str | None] = mapped_column(Text, nullable=True)
    teams_post_success: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)  # NULL = still open/firing
    is_recurrence: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class KafkaTeamsReminder(Base):
    """Message content for the "Schedules and Reminders" Teams tab. Deliberately
    reuses kafka_job_schedules/kafka_job_runs (linked via job_id) for the actual
    cron expression, enable/disable and run history rather than duplicating
    that scheduling logic -- this table only holds what to send (message
    template and optional webhook override; webhook_url NULL falls back to the
    agent-level default)."""
    __tablename__ = "kafka_teams_reminders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)  # kafka_job_schedules.job_id
    name: Mapped[str] = mapped_column(Text, nullable=False)
    message_template: Mapped[str] = mapped_column(Text, nullable=False)
    webhook_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class KafkaWatchdogEvent(Base):
    """One row per safety-net event: a watchdog that is about to restart the
    agent. Written just before the restart (best-effort, hard time cap) so
    the reason survives it; Docker keeps logs only for the current run."""
    __tablename__ = "kafka_watchdog_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    watchdog: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[dict | None] = mapped_column(JSONB, nullable=True)


# Notification outbox (design 2.1). Step 1 writes only origin='inline' rows:
# a record of what an inline owner or rule send did (shadow mode), always
# finished on insert. The CHECK lists below are repeated verbatim in Alembic
# 0058; keep the two in step.
_OUTBOX_STATUS_SQL = "('pending', 'standby', 'sending', 'sent', 'failed', 'skipped', 'abandoned', 'superseded')"


class KafkaNotificationOutbox(Base):
    """One row per owner notice or rule notification. Per-channel status for
    email and Teams; *_last_error holds a short, secret-free error or the
    reason a channel was skipped. state: delivered (every wanted channel
    sent), degraded (sent, but a channel failed, was not configured or sent
    with a warning), failed (nothing sent although something was wanted),
    suppressed (nothing wanted: every channel skipped by a setting or rule),
    pending (not finished; outbox-driven rows only)."""
    __tablename__ = "kafka_notification_outbox"
    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_kafka_notification_outbox_dedupe_key"),
        CheckConstraint(
            "kind IN ('owner_summary', 'owner_watchdog', 'rule_fire', 'rule_escalate', "
            "'rule_reminder', 'rule_resolve', 'heartbeat', 'deadman')",
            name="ck_kafka_notification_outbox_kind",
        ),
        CheckConstraint("routing IN ('owner', 'rule')", name="ck_kafka_notification_outbox_routing"),
        CheckConstraint("origin IN ('inline', 'outbox')", name="ck_kafka_notification_outbox_origin"),
        CheckConstraint(f"email_status IN {_OUTBOX_STATUS_SQL}", name="ck_kafka_notification_outbox_email_status"),
        CheckConstraint(f"teams_status IN {_OUTBOX_STATUS_SQL}", name="ck_kafka_notification_outbox_teams_status"),
        CheckConstraint(
            "state IN ('pending', 'delivered', 'degraded', 'failed', 'suppressed')",
            name="ck_kafka_notification_outbox_state",
        ),
        Index(
            "ix_kafka_notification_outbox_due", "priority", "next_attempt_at",
            postgresql_where=text("finished_at IS NULL"),
        ),
        Index("ix_kafka_notification_outbox_created_at", "created_at"),
        Index("ix_kafka_notification_outbox_config_created", "alert_config_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)  # producing function
    routing: Mapped[str] = mapped_column(Text, nullable=False)  # owner: email first, Teams fallback; rule: each channel
    origin: Mapped[str] = mapped_column(Text, nullable=False)  # inline: record of an inline send; outbox: sent by the worker
    dedupe_key: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("2"))
    alert_config_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # no FK: purges never block
    cluster_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trigger_ids: Mapped[list[int] | None] = mapped_column(ARRAY(Integer), nullable=True)
    depends_on_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    severity: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    email_status: Mapped[str] = mapped_column(Text, nullable=False)
    teams_status: Mapped[str] = mapped_column(Text, nullable=False)
    email_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    teams_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    email_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    teams_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    email_warning: Mapped[str | None] = mapped_column(Text, nullable=True)
    email_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    teams_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    email_message_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    claimed_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class KafkaNotificationAttempt(Base):
    """One row per channel attempt of a kafka_notification_outbox row.
    attempt_no 0 with outcome 'skipped' records a channel that was not
    attempted, with the reason in error. outcome NULL means in flight."""
    __tablename__ = "kafka_notification_attempts"
    __table_args__ = (
        CheckConstraint("channel IN ('email', 'teams')", name="ck_kafka_notification_attempts_channel"),
        CheckConstraint(
            "outcome IS NULL OR outcome IN ('sent', 'failed_transient', 'failed_config', "
            "'failed_permanent', 'timeout', 'unknown', 'skipped')",
            name="ck_kafka_notification_attempts_outcome",
        ),
        Index("ix_kafka_notification_attempts_outbox_id", "outbox_id"),
        Index("ix_kafka_notification_attempts_started_at", "started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    outbox_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("kafka_notification_outbox.id", ondelete="CASCADE", name="fk_kafka_notification_attempts_outbox_id"),
        nullable=False,
    )
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    boot_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    phase_reached: Mapped[str | None] = mapped_column(Text, nullable=True)
    outcome: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
