from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKeyConstraint, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shiftledger.database import Base, IdentityMixin, utc_now


class ReconciliationResult(IdentityMixin, Base):
    __tablename__ = "reconciliation_results"
    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id", "report_id"], ["shift_reports.organization_id", "shift_reports.id"]
        ),
        ForeignKeyConstraint(
            ["organization_id", "baseline_report_id"],
            ["shift_reports.organization_id", "shift_reports.id"],
        ),
        UniqueConstraint("report_id"),
    )

    organization_id: Mapped[UUID]
    report_id: Mapped[UUID]
    baseline_report_id: Mapped[UUID | None]
    currency: Mapped[str] = mapped_column(String(3))
    expected_cash: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    actual_cash: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    difference: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    findings: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    ai_status: Mapped[str] = mapped_column(String(30), default="not_configured")
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
