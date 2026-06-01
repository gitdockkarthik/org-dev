from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class AgentConfig(Base):
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


class AlertReport(Base):
    __tablename__ = "alert_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_slug: Mapped[str] = mapped_column(String, nullable=False, index=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    total_alerts: Mapped[int] = mapped_column(Integer, default=0)
    genuine_count: Mapped[int] = mapped_column(Integer, default=0)
    noise_count: Mapped[int] = mapped_column(Integer, default=0)
    suspect_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String, default="ready")
    report_data: Mapped[str] = mapped_column(Text, nullable=True)  # JSON blob
    stats_data: Mapped[str] = mapped_column(Text, nullable=True)  # JSON blob
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
