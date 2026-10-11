from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shiftledger.database import Base, IdentityMixin, utc_now


class ShiftReport(IdentityMixin, Base):
    __tablename__ = "shift_reports"
    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id", "submitted_by"],
            ["memberships.organization_id", "memberships.user_id"],
        ),
        ForeignKeyConstraint(
            ["organization_id", "template_version_id"],
            ["template_versions.organization_id", "template_versions.id"],
        ),
        UniqueConstraint("organization_id", "id"),
        UniqueConstraint("organization_id", "submitted_by", "idempotency_key"),
        UniqueConstraint("organization_id", "location_code", "business_date", "shift_key"),
        Index("ix_shift_reports_history", "organization_id", "location_code", "business_date"),
    )

    organization_id: Mapped[UUID]
    submitted_by: Mapped[UUID]
    template_version_id: Mapped[UUID]
    business_date: Mapped[date]
    location_code: Mapped[str] = mapped_column(String(64))
    shift_key: Mapped[str] = mapped_column(String(64))
    values: Mapped[dict[str, Any]] = mapped_column(JSONB)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
