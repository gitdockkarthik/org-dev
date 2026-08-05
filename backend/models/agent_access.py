from datetime import datetime
from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column
from core.database import Base


class AgentAccess(Base):
    __tablename__ = "agent_access"

    agent_slug: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)
    user_email: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)
    assigned_by: Mapped[str] = mapped_column(String, nullable=False)
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
