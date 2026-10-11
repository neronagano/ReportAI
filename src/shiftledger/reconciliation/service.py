from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.config import Settings
from shiftledger.errors import PermanentMessageError
from shiftledger.messaging.events import EventEnvelope, EventType, outbox_message
from shiftledger.messaging.models import InboxMessage
from shiftledger.reconciliation.domain import reconcile
from shiftledger.reconciliation.models import ReconciliationResult
from shiftledger.reports.models import ShiftReport
from shiftledger.templates.domain import TemplateDefinition, validate_report
from shiftledger.templates.models import TemplateVersion


async def process_report_event(
    session: AsyncSession, event: EventEnvelope, settings: Settings
) -> bool:
    if event.type != EventType.REPORT_SUBMITTED:
        raise PermanentMessageError("Unsupported event type on report topic")
    claimed = await session.scalar(
        insert(InboxMessage)
        .values(consumer_name=settings.kafka_consumer_group, event_id=event.id)
        .on_conflict_do_nothing()
        .returning(InboxMessage.event_id)
    )
    if claimed is None:
        return False
    # Also serialize different event IDs for the same report, not only duplicate delivery.
    report = await session.scalar(
        select(ShiftReport)
        .where(
            ShiftReport.organization_id == event.organization_id, ShiftReport.id == event.report_id
        )
        .with_for_update()
    )
    if report is None:
        raise PermanentMessageError("Event references an unavailable report")
    existing = await session.scalar(
        select(ReconciliationResult.id).where(
            ReconciliationResult.organization_id == event.organization_id,
            ReconciliationResult.report_id == report.id,
        )
    )
    if existing:
        return False
    version = await session.scalar(
        select(TemplateVersion).where(
            TemplateVersion.organization_id == report.organization_id,
            TemplateVersion.id == report.template_version_id,
        )
    )
    if version is None:
        raise PermanentMessageError("Report references an unavailable template version")
    definition = TemplateDefinition.model_validate(version.definition)
    current = validate_report(definition, report.values)
    baseline_row = (
        await session.execute(
            select(ShiftReport, TemplateVersion)
            .join(TemplateVersion, ShiftReport.template_version_id == TemplateVersion.id)
            .where(
                ShiftReport.organization_id == report.organization_id,
                ShiftReport.location_code == report.location_code,
                ShiftReport.shift_key == report.shift_key,
                ShiftReport.business_date == report.business_date - timedelta(days=1),
                TemplateVersion.template_id == version.template_id,
            )
        )
    ).one_or_none()
    previous = None
    baseline_id = None
    if baseline_row:
        baseline, baseline_version = baseline_row
        baseline_definition = TemplateDefinition.model_validate(baseline_version.definition)
        if baseline_definition.currency == definition.currency:
            previous = validate_report(baseline_definition, baseline.values).amounts
            baseline_id = baseline.id
    checked = reconcile(current.amounts, previous, Decimal(str(settings.revenue_change_threshold)))
    session.add(
        ReconciliationResult(
            organization_id=report.organization_id,
            report_id=report.id,
            baseline_report_id=baseline_id,
            currency=definition.currency,
            expected_cash=checked.expected_cash,
            actual_cash=checked.actual_cash,
            difference=checked.difference,
            findings=[finding.model_dump(mode="json") for finding in checked.findings],
            ai_status="not_configured",
        )
    )
    session.add(
        outbox_message(
            settings.kafka_completed_topic,
            EventEnvelope(
                type=EventType.RECONCILIATION_COMPLETED,
                organization_id=event.organization_id,
                report_id=report.id,
                correlation_id=event.correlation_id,
            ),
        )
    )
    await session.flush()
    return True
