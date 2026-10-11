from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from shiftledger.templates.domain import ReportValues


class ReportSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    template_version_id: UUID
    business_date: date
    location_code: str = Field(default="main", pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    shift_key: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    values: ReportValues


class ReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    organization_id: UUID
    submitted_by: UUID
    template_version_id: UUID
    business_date: date
    location_code: str
    shift_key: str
    status: Literal["submitted"] = "submitted"
    values: ReportValues
    submitted_at: datetime
