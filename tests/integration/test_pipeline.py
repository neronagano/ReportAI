from datetime import date, timedelta
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.attachments.models import Attachment
from shiftledger.cli import demo_definition
from shiftledger.config import Settings
from shiftledger.database import utc_now
from shiftledger.errors import ApplicationError, PermanentMessageError
from shiftledger.identity.access import MemberContext
from shiftledger.identity.models import Membership, Organization, Role, User
from shiftledger.messaging.events import EventEnvelope, EventType
from shiftledger.messaging.models import InboxMessage, OutboxMessage
from shiftledger.messaging.outbox import publish_next
from shiftledger.reconciliation.models import ReconciliationResult
from shiftledger.reconciliation.service import process_report_event
from shiftledger.reports.models import ShiftReport
from shiftledger.reports.schemas import ReportSubmit
from shiftledger.reports.service import get_report, submit_report
from shiftledger.templates.models import TemplateVersion
from shiftledger.templates.schemas import TemplateCreate
from shiftledger.templates.service import create_template, get_definition, publish_version

pytestmark = pytest.mark.integration


def values(revenue: str = "7000.00", closing: str = "3500.00") -> dict:
    return {
        "cash": {
            "opening": "1000.00",
            "sales": "5000.00",
            "refunds": "200.00",
            "collected": "2000.00",
            "closing": closing,
            "revenue": revenue,
        },
        "expenses": [{"amount": "300.00", "description": "Упаковка для магазина"}],
        "notes": {"comment": "Смена закрыта"},
    }


async def prepare(session: AsyncSession) -> tuple[MemberContext, TemplateVersion]:
    organization = Organization(name="Integration store")
    user = User(email=f"{uuid4()}@example.com", password_hash="unused-in-service-tests")
    session.add_all([organization, user])
    await session.flush()
    session.add(Membership(organization_id=organization.id, user_id=user.id, role=Role.ADMIN))
    await session.flush()
    _, version = await create_template(
        session, organization.id, TemplateCreate(name="Shift", definition=demo_definition())
    )
    return MemberContext(organization.id, user.id, Role.ADMIN), version


async def submit(
    session: AsyncSession,
    cache: Redis,
    settings: Settings,
    member: MemberContext,
    version: TemplateVersion,
    *,
    day: date | None = None,
    revenue: str = "7000.00",
    closing: str = "3500.00",
    key: str | None = None,
) -> ShiftReport:
    report, _ = await submit_report(
        session,
        cache,
        settings,
        member,
        ReportSubmit(
            template_version_id=version.id,
            business_date=day or date.today(),
            shift_key="evening",
            values=values(revenue, closing),
        ),
        key or str(uuid4()),
        uuid4(),
    )
    return report


async def event_for(session: AsyncSession, report: ShiftReport) -> EventEnvelope:
    event = await session.scalar(
        select(OutboxMessage).where(OutboxMessage.partition_key == str(report.id))
    )
    assert event is not None
    return EventEnvelope.model_validate(event.event)


async def test_submission_and_outbox_are_atomic(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    async with db.begin_nested() as savepoint:
        report = await submit(db, cache, integration_settings, member, version)
        report_id = report.id
        event_id = (await event_for(db, report)).id
        await savepoint.rollback()
    assert await db.get(ShiftReport, report_id) is None
    assert await db.get(OutboxMessage, event_id) is None


async def test_idempotency_replays_original_and_rejects_changed_payload(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    key = str(uuid4())
    first = await submit(db, cache, integration_settings, member, version, key=key)
    replay = await submit(db, cache, integration_settings, member, version, key=key)
    assert replay.id == first.id
    assert await db.scalar(select(func.count()).select_from(OutboxMessage)) == 1
    with pytest.raises(ApplicationError, match="different data"):
        await submit(db, cache, integration_settings, member, version, key=key, revenue="6000.00")


async def test_shift_cannot_be_submitted_twice_with_different_keys(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    await submit(db, cache, integration_settings, member, version)
    with pytest.raises(ApplicationError, match="already have a report"):
        await submit(db, cache, integration_settings, member, version)


async def test_consumer_checks_correct_baseline_and_is_idempotent(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    previous = await submit(
        db, cache, integration_settings, member, version, day=date.today() - timedelta(days=1)
    )
    current = await submit(
        db, cache, integration_settings, member, version, revenue="3500.00", closing="3250.00"
    )
    event = await event_for(db, current)
    assert await process_report_event(db, event, integration_settings)
    assert not await process_report_event(db, event, integration_settings)
    assert not await process_report_event(
        db, event.model_copy(update={"id": uuid4()}), integration_settings
    )
    result = await db.scalar(
        select(ReconciliationResult).where(ReconciliationResult.report_id == current.id)
    )
    assert result is not None
    assert result.baseline_report_id == previous.id
    assert str(result.difference) == "-250.00"
    assert {finding["code"] for finding in result.findings} == {"cash_shortage", "revenue_drop"}
    assert result.ai_status == "not_configured"
    completed = await db.scalar(
        select(func.count())
        .select_from(OutboxMessage)
        .where(OutboxMessage.topic == integration_settings.kafka_completed_topic)
    )
    assert completed == 1


async def test_worker_rollback_does_not_mark_event_processed(
    db: AsyncSession, integration_settings: Settings
) -> None:
    missing = EventEnvelope(
        type=EventType.REPORT_SUBMITTED,
        organization_id=uuid4(),
        report_id=uuid4(),
        correlation_id=uuid4(),
    )
    with pytest.raises(PermanentMessageError):
        async with db.begin_nested():
            await process_report_event(db, missing, integration_settings)
    assert (
        await db.get(InboxMessage, (integration_settings.kafka_consumer_group, missing.id)) is None
    )


class RecordingPublisher:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[EventEnvelope] = []

    async def publish(self, topic: str, key: str, event: EventEnvelope) -> None:
        if self.fail:
            raise ConnectionError("simulated outage")
        self.sent.append(event)


async def test_outbox_keeps_failed_events_and_retries(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    report = await submit(db, cache, integration_settings, member, version)
    event = await event_for(db, report)
    assert await publish_next(db, RecordingPublisher(fail=True))
    message = await db.get(OutboxMessage, event.id)
    assert message is not None
    assert message.published_at is None and message.attempts == 1
    message.next_attempt_at = utc_now() - timedelta(seconds=1)
    await db.flush()
    publisher = RecordingPublisher()
    assert await publish_next(db, publisher)
    assert message.published_at is not None
    assert publisher.sent[0].id == event.id


async def test_template_cache_is_scoped_by_tenant(db: AsyncSession, cache: Redis) -> None:
    member, version = await prepare(db)
    await get_definition(db, cache, member.organization_id, version.id, 60)
    with pytest.raises(ApplicationError, match="not found"):
        await get_definition(db, cache, uuid4(), version.id, 60)


async def test_database_rejects_cross_tenant_report_references(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    _member, version = await prepare(db)
    other, _ = await prepare(db)
    with pytest.raises(IntegrityError):
        async with db.begin_nested():
            db.add(
                ShiftReport(
                    organization_id=other.organization_id,
                    submitted_by=other.user_id,
                    template_version_id=version.id,
                    business_date=date.today(),
                    location_code="main",
                    shift_key="evening",
                    values=values(),
                    idempotency_key=str(uuid4()),
                    request_hash="0" * 64,
                )
            )
            await db.flush()


async def test_published_versions_and_reports_are_immutable_in_database(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    report = await submit(db, cache, integration_settings, member, version)
    with pytest.raises(DBAPIError):
        async with db.begin_nested():
            await db.execute(
                update(ShiftReport).where(ShiftReport.id == report.id).values(values={})
            )
    with pytest.raises(DBAPIError):
        async with db.begin_nested():
            await db.execute(
                update(TemplateVersion)
                .where(TemplateVersion.id == version.id)
                .values(definition={})
            )


async def test_schema_evolution_retains_old_report_meaning(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    report = await submit(db, cache, integration_settings, member, version)
    definition = demo_definition()
    definition.blocks[0].fields[0].label = "Уточнённое название остатка"
    published = await publish_version(db, member.organization_id, version.template_id, definition)
    assert published.version == 2
    assert report.template_version_id == version.id
    assert (
        version.definition["blocks"][0]["fields"][0]["label"]
        != definition.blocks[0].fields[0].label
    )


async def test_foreign_receipt_rolls_back_report_and_event(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    member, version = await prepare(db)
    other, _ = await prepare(db)
    attachment = Attachment(
        organization_id=other.organization_id,
        uploaded_by=other.user_id,
        object_key=f"{other.organization_id}/{uuid4()}.png",
        content_type="image/png",
        size_bytes=100,
        sha256="0" * 64,
    )
    db.add(attachment)
    await db.flush()
    payload = values()
    payload["expenses"][0]["receipts"] = [str(attachment.id)]
    with pytest.raises(ApplicationError, match="Attachment not found"):
        await submit_report(
            db,
            cache,
            integration_settings,
            member,
            ReportSubmit(
                template_version_id=version.id,
                business_date=date.today(),
                shift_key="evening",
                values=payload,
            ),
            str(uuid4()),
            uuid4(),
        )
    assert await db.scalar(select(func.count()).select_from(ShiftReport)) == 0
    assert await db.scalar(select(func.count()).select_from(OutboxMessage)) == 0
    assert attachment.report_id is None


async def test_employee_cannot_read_other_employees_report(
    db: AsyncSession, cache: Redis, integration_settings: Settings
) -> None:
    administrator, version = await prepare(db)
    report = await submit(db, cache, integration_settings, administrator, version)
    employee = User(email=f"{uuid4()}@example.com", password_hash="unused")
    db.add(employee)
    await db.flush()
    db.add(
        Membership(
            organization_id=administrator.organization_id, user_id=employee.id, role=Role.EMPLOYEE
        )
    )
    await db.flush()
    context = MemberContext(administrator.organization_id, employee.id, Role.EMPLOYEE)
    with pytest.raises(ApplicationError, match="Report not found"):
        await get_report(db, context, report.id)
    assert (await get_report(db, administrator, report.id)).id == report.id
