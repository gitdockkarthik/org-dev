from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint
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
    zookeeper_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    kafka_connect_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    jmx_port: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
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
