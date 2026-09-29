from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
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
    already open (resolved_at NULL); no immediate card on resolution --
    instead a separate periodic digest covers Resolved/Pending/Recurrence.
    is_recurrence=True marks a trigger that fired again shortly after the
    same alert+cluster's previous trigger had resolved (flapping signal)."""
    __tablename__ = "kafka_alert_triggers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_config_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("kafka_alert_configs.id", ondelete="CASCADE"), nullable=False
    )
    cluster_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
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
