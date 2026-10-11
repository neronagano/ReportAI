import hashlib
import json
from uuid import UUID, uuid4

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.attachments.service import bind_attachments
from shiftledger.config import Settings
from shiftledger.errors import ApplicationError, not_found
from shiftledger.identity.access import MemberContext
from shiftledger.messaging.events import EventEnvelope, EventType, outbox_message
from shiftledger.reports.models import ShiftReport
from shiftledger.reports.schemas import ReportSubmit
from shiftledger.templates.domain import validate_report
from shiftledger.templates.service import get_definition


def request_fingerprint(data: ReportSubmit) -> str:
    canonical = json.dumps(
        data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def _existing(session: AsyncSession, member: MemberContext, key: str) -> ShiftReport | None:
    reports = await session.scalars(
        select(ShiftReport).where(
            ShiftReport.organization_id == member.organization_id,
            ShiftReport.submitted_by == member.user_id,
            ShiftReport.idempotency_key == key,
        )
    )
    return reports.one_or_none()


def _ensure_same_request(report: ShiftReport, fingerprint: str) -> None:
    if report.request_hash != fingerprint:
        raise ApplicationError(
            "idempotency_conflict", "Idempotency key was used for different data", 409
        )


async def submit_report(
    session: AsyncSession,
    cache: Redis,
    settings: Settings,
    member: MemberContext,
    data: ReportSubmit,
    idempotency_key: str,
    correlation_id: UUID,
) -> tuple[ShiftReport, bool]:
    fingerprint = request_fingerprint(data)
    existing = await _existing(session, member, idempotency_key)
    if existing:
        _ensure_same_request(existing, fingerprint)
        return existing, False
    definition = await get_definition(
        session,
        cache,
        member.organization_id,
        data.template_version_id,
        settings.template_cache_ttl_seconds,
    )
    validated = validate_report(definition, data.values)
    report = ShiftReport(
        id=uuid4(),
        organization_id=member.organization_id,
        submitted_by=member.user_id,
        template_version_id=data.template_version_id,
        business_date=data.business_date,
        location_code=data.location_code,
        shift_key=data.shift_key,
        values=validated.values,
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
    )
    try:
        async with session.begin_nested():
            session.add(report)
            await session.flush()
            await bind_attachments(session, member, report.id, validated.attachment_ids)
            session.add(
                outbox_message(
                    settings.kafka_report_topic,
                    EventEnvelope(
                        type=EventType.REPORT_SUBMITTED,
                        organization_id=member.organization_id,
                        report_id=report.id,
                        correlation_id=correlation_id,
                    ),
                )
            )
            await session.flush()
    except IntegrityError as exc:
        # A concurrent submission can win between the initial SELECT and INSERT.
        existing = await _existing(session, member, idempotency_key)
        if existing:
            _ensure_same_request(existing, fingerprint)
            return existing, False
        raise ApplicationError(
            "shift_already_submitted", "This location and shift already have a report", 409
        ) from exc
    return report, True


async def get_report(session: AsyncSession, member: MemberContext, report_id: UUID) -> ShiftReport:
    query = select(ShiftReport).where(
        ShiftReport.organization_id == member.organization_id, ShiftReport.id == report_id
    )
    if not member.is_admin:
        query = query.where(ShiftReport.submitted_by == member.user_id)
    report = await session.scalar(query)
    if report is None:
        raise not_found("Report")
    return report
