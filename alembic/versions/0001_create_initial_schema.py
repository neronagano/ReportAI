"""create initial schema

Revision ID: 0001
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "inbox_messages",
        sa.Column("consumer_name", sa.String(length=100), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("consumer_name", "event_id", name=op.f("pk_inbox_messages")),
    )
    op.create_table(
        "organizations",
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_organizations")),
    )
    op.create_table(
        "outbox_messages",
        sa.Column("topic", sa.String(length=200), nullable=False),
        sa.Column("partition_key", sa.String(length=200), nullable=False),
        sa.Column("event", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_messages")),
    )
    op.create_index(
        "ix_outbox_pending",
        "outbox_messages",
        ["next_attempt_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "users",
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
        sa.UniqueConstraint("email", name=op.f("uq_users_email")),
    )
    op.create_table(
        "memberships",
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.String(length=20), nullable=False),
        sa.CheckConstraint("role IN ('admin', 'employee')", name=op.f("ck_memberships_valid_role")),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_memberships_organization_id_organizations"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_memberships_user_id_users")
        ),
        sa.PrimaryKeyConstraint("organization_id", "user_id", name=op.f("pk_memberships")),
    )
    op.create_table(
        "templates",
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name=op.f("fk_templates_organization_id_organizations"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_templates")),
        sa.UniqueConstraint("organization_id", "id", name=op.f("uq_templates_organization_id_id")),
    )
    op.create_index(
        op.f("ix_templates_organization_id"), "templates", ["organization_id"], unique=False
    )
    op.create_table(
        "template_versions",
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("template_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("definition", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("version > 0", name=op.f("ck_template_versions_positive_version")),
        sa.ForeignKeyConstraint(
            ["organization_id", "template_id"],
            ["templates.organization_id", "templates.id"],
            name=op.f("fk_template_versions_organization_id_template_id_templates"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_template_versions")),
        sa.UniqueConstraint(
            "organization_id", "id", name=op.f("uq_template_versions_organization_id_id")
        ),
        sa.UniqueConstraint(
            "template_id", "version", name=op.f("uq_template_versions_template_id_version")
        ),
    )
    op.create_index(
        op.f("ix_template_versions_template_id"), "template_versions", ["template_id"], unique=False
    )
    op.create_table(
        "shift_reports",
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("submitted_by", sa.Uuid(), nullable=False),
        sa.Column("template_version_id", sa.Uuid(), nullable=False),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("location_code", sa.String(length=64), nullable=False),
        sa.Column("shift_key", sa.String(length=64), nullable=False),
        sa.Column("values", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "submitted_by"],
            ["memberships.organization_id", "memberships.user_id"],
            name=op.f("fk_shift_reports_organization_id_submitted_by_memberships"),
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "template_version_id"],
            ["template_versions.organization_id", "template_versions.id"],
            name=op.f("fk_shift_reports_organization_id_template_version_id_template_versions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_shift_reports")),
        sa.UniqueConstraint(
            "organization_id", "id", name=op.f("uq_shift_reports_organization_id_id")
        ),
        sa.UniqueConstraint(
            "organization_id",
            "location_code",
            "business_date",
            "shift_key",
            name=op.f("uq_shift_reports_organization_id_location_code_business_date_shift_key"),
        ),
        sa.UniqueConstraint(
            "organization_id",
            "submitted_by",
            "idempotency_key",
            name=op.f("uq_shift_reports_organization_id_submitted_by_idempotency_key"),
        ),
    )
    op.create_index(
        "ix_shift_reports_history",
        "shift_reports",
        ["organization_id", "location_code", "business_date"],
        unique=False,
    )
    op.create_table(
        "attachments",
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("uploaded_by", sa.Uuid(), nullable=False),
        sa.Column("report_id", sa.Uuid(), nullable=True),
        sa.Column("object_key", sa.String(length=200), nullable=False),
        sa.Column("content_type", sa.String(length=50), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("size_bytes > 0", name=op.f("ck_attachments_positive_size")),
        sa.ForeignKeyConstraint(
            ["organization_id", "report_id"],
            ["shift_reports.organization_id", "shift_reports.id"],
            name=op.f("fk_attachments_organization_id_report_id_shift_reports"),
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "uploaded_by"],
            ["memberships.organization_id", "memberships.user_id"],
            name=op.f("fk_attachments_organization_id_uploaded_by_memberships"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_attachments")),
        sa.UniqueConstraint("object_key", name=op.f("uq_attachments_object_key")),
        sa.UniqueConstraint(
            "organization_id", "id", name=op.f("uq_attachments_organization_id_id")
        ),
    )
    op.create_index(
        "ix_attachments_owner", "attachments", ["organization_id", "uploaded_by"], unique=False
    )
    op.create_index(op.f("ix_attachments_sha256"), "attachments", ["sha256"], unique=False)
    op.create_table(
        "reconciliation_results",
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("report_id", sa.Uuid(), nullable=False),
        sa.Column("baseline_report_id", sa.Uuid(), nullable=True),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("expected_cash", sa.Numeric(precision=20, scale=2), nullable=True),
        sa.Column("actual_cash", sa.Numeric(precision=20, scale=2), nullable=True),
        sa.Column("difference", sa.Numeric(precision=20, scale=2), nullable=True),
        sa.Column("findings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ai_status", sa.String(length=30), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "baseline_report_id"],
            ["shift_reports.organization_id", "shift_reports.id"],
            name=op.f("fk_reconciliation_results_organization_id_baseline_report_id_shift_reports"),
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "report_id"],
            ["shift_reports.organization_id", "shift_reports.id"],
            name=op.f("fk_reconciliation_results_organization_id_report_id_shift_reports"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_reconciliation_results")),
        sa.UniqueConstraint("report_id", name=op.f("uq_reconciliation_results_report_id")),
    )
    op.execute("""
        CREATE FUNCTION shiftledger_reject_immutable_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'Published template versions and submitted reports are immutable'
                USING ERRCODE = '55000';
        END;
        $$;
    """)
    op.execute("""
        CREATE TRIGGER template_versions_immutable BEFORE UPDATE OR DELETE ON template_versions
        FOR EACH ROW EXECUTE FUNCTION shiftledger_reject_immutable_mutation();
    """)
    op.execute("""
        CREATE TRIGGER shift_reports_immutable BEFORE UPDATE OR DELETE ON shift_reports
        FOR EACH ROW EXECUTE FUNCTION shiftledger_reject_immutable_mutation();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER shift_reports_immutable ON shift_reports")
    op.execute("DROP TRIGGER template_versions_immutable ON template_versions")
    op.execute("DROP FUNCTION shiftledger_reject_immutable_mutation()")
    op.drop_table("reconciliation_results")
    op.drop_index(op.f("ix_attachments_sha256"), table_name="attachments")
    op.drop_index("ix_attachments_owner", table_name="attachments")
    op.drop_table("attachments")
    op.drop_index("ix_shift_reports_history", table_name="shift_reports")
    op.drop_table("shift_reports")
    op.drop_index(op.f("ix_template_versions_template_id"), table_name="template_versions")
    op.drop_table("template_versions")
    op.drop_index(op.f("ix_templates_organization_id"), table_name="templates")
    op.drop_table("templates")
    op.drop_table("memberships")
    op.drop_table("users")
    op.drop_index(
        "ix_outbox_pending",
        table_name="outbox_messages",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox_messages")
    op.drop_table("organizations")
    op.drop_table("inbox_messages")
