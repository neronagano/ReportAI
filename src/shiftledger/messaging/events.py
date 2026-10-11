from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from shiftledger.database import utc_now
from shiftledger.messaging.models import OutboxMessage


class EventType(StrEnum):
    REPORT_SUBMITTED = "report.submitted"
    RECONCILIATION_COMPLETED = "reconciliation.completed"


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    type: EventType
    schema_version: int = Field(default=1, ge=1, le=1)
    organization_id: UUID
    report_id: UUID
    occurred_at: datetime = Field(default_factory=utc_now)
    correlation_id: UUID


def outbox_message(topic: str, event: EventEnvelope) -> OutboxMessage:
    return OutboxMessage(
        id=event.id,
        topic=topic,
        partition_key=str(event.report_id),
        event=event.model_dump(mode="json"),
    )
