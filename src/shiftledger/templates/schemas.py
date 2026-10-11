from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from shiftledger.templates.domain import TemplateDefinition


class TemplateCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    definition: TemplateDefinition


class VersionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    definition: TemplateDefinition


class VersionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    template_id: UUID
    version: int
    definition: TemplateDefinition
    created_at: datetime


class TemplateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    created_at: datetime


class PublishedTemplateResponse(BaseModel):
    template: TemplateResponse
    version: VersionResponse
