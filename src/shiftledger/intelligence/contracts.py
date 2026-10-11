from datetime import date
from decimal import Decimal
from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from shiftledger.templates.domain import ReportValues


class ReceiptItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str = Field(max_length=1000)
    amount: Decimal = Field(ge=0, max_digits=18, decimal_places=2)


class ReceiptExtraction(BaseModel):
    """An interpretation of a document, stored separately from the employee's input."""

    model_config = ConfigDict(extra="forbid")

    attachment_id: UUID
    merchant: str | None = Field(default=None, max_length=500)
    purchased_on: date | None = None
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    total: Decimal | None = Field(default=None, ge=0, max_digits=18, decimal_places=2)
    items: list[ReceiptItem] = Field(default_factory=list, max_length=200)
    confidence: float = Field(ge=0, le=1)
    uncertainties: list[str] = Field(default_factory=list, max_length=30)
    provider: str = Field(max_length=100)
    model: str = Field(max_length=100)
    schema_version: Literal[1] = 1


class ReviewContext(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    organization_id: UUID
    report_id: UUID
    submitted_values: ReportValues
    receipts: list[ReceiptExtraction]
    deterministic_findings: list[str]


class ReviewFinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    explanation: str = Field(min_length=1, max_length=2000)
    attachment_ids: list[UUID] = Field(max_length=20)
    field_paths: list[str] = Field(max_length=30)
    confidence: float = Field(ge=0, le=1)
    requires_human_review: Literal[True] = True


class BusinessReview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report_id: UUID
    findings: list[ReviewFinding] = Field(max_length=30)
    provider: str = Field(max_length=100)
    model: str = Field(max_length=100)
    schema_version: Literal[1] = 1


class ReceiptExtractor(Protocol):
    async def extract(
        self, attachment_id: UUID, image: bytes, content_type: str
    ) -> ReceiptExtraction: ...


class BusinessReviewer(Protocol):
    async def review(self, context: ReviewContext) -> BusinessReview: ...
