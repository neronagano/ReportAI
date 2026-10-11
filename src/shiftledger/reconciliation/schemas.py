from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from shiftledger.reconciliation.domain import Finding


class ResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    report_id: UUID
    baseline_report_id: UUID | None
    currency: str
    expected_cash: Decimal | None
    actual_cash: Decimal | None
    difference: Decimal | None
    findings: list[Finding]
    ai_status: str
    completed_at: datetime


class ReconciliationResponse(BaseModel):
    status: Literal["pending", "completed"]
    result: ResultResponse | None = None
