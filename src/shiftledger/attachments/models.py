from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from shiftledger.database import Base, IdentityMixin


class Attachment(IdentityMixin, Base):
    __tablename__ = "attachments"
    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id", "uploaded_by"],
            ["memberships.organization_id", "memberships.user_id"],
        ),
        ForeignKeyConstraint(
            ["organization_id", "report_id"], ["shift_reports.organization_id", "shift_reports.id"]
        ),
        UniqueConstraint("organization_id", "id"),
        CheckConstraint("size_bytes > 0", name="positive_size"),
        Index("ix_attachments_owner", "organization_id", "uploaded_by"),
    )

    organization_id: Mapped[UUID]
    uploaded_by: Mapped[UUID]
    report_id: Mapped[UUID | None]
    object_key: Mapped[str] = mapped_column(String(200), unique=True)
    content_type: Mapped[str] = mapped_column(String(50))
    size_bytes: Mapped[int]
    sha256: Mapped[str] = mapped_column(String(64), index=True)
