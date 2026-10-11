"""Import all tables for Alembic metadata discovery; no schema creation at app startup."""

from shiftledger.attachments.models import Attachment
from shiftledger.identity.models import Membership, Organization, User
from shiftledger.messaging.models import InboxMessage, OutboxMessage
from shiftledger.reconciliation.models import ReconciliationResult
from shiftledger.reports.models import ShiftReport
from shiftledger.templates.models import Template, TemplateVersion

__all__ = [
    "Attachment",
    "InboxMessage",
    "Membership",
    "Organization",
    "OutboxMessage",
    "ReconciliationResult",
    "ShiftReport",
    "Template",
    "TemplateVersion",
    "User",
]
