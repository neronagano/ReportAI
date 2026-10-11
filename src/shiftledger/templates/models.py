from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, ForeignKeyConstraint, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shiftledger.database import Base, IdentityMixin


class Template(IdentityMixin, Base):
    __tablename__ = "templates"
    __table_args__ = (UniqueConstraint("organization_id", "id"),)

    organization_id: Mapped[UUID] = mapped_column(ForeignKey("organizations.id"), index=True)
    name: Mapped[str] = mapped_column(String(200))


class TemplateVersion(IdentityMixin, Base):
    __tablename__ = "template_versions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id", "template_id"], ["templates.organization_id", "templates.id"]
        ),
        UniqueConstraint("organization_id", "id"),
        UniqueConstraint("template_id", "version"),
        CheckConstraint("version > 0", name="positive_version"),
    )

    organization_id: Mapped[UUID] = mapped_column()
    template_id: Mapped[UUID] = mapped_column(index=True)
    version: Mapped[int]
    definition: Mapped[dict[str, Any]] = mapped_column(JSONB)
