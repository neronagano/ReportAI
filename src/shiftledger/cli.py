import argparse
import asyncio
import base64
import getpass
import json
import os
from uuid import UUID

from aiokafka import AIOKafkaConsumer, TopicPartition
from pydantic import EmailStr, TypeAdapter
from sqlalchemy import select

from shiftledger.attachments.storage import S3Storage
from shiftledger.config import Environment, get_settings
from shiftledger.database import create_engine, session_factory
from shiftledger.identity.models import Membership, Organization, Role, User
from shiftledger.identity.security import hash_password
from shiftledger.messaging.events import EventEnvelope, EventType
from shiftledger.messaging.kafka import connection_options, create_producer, ensure_topics
from shiftledger.observability import configure_logging
from shiftledger.templates.domain import (
    BlockDefinition,
    FieldDefinition,
    FieldKind,
    Semantic,
    TemplateDefinition,
)
from shiftledger.templates.models import TemplateVersion
from shiftledger.templates.schemas import TemplateCreate
from shiftledger.templates.service import create_template

DEMO_ORGANIZATION_ID = UUID("6d152388-a2cc-4f12-97c3-c935d9c097e0")


def demo_definition() -> TemplateDefinition:
    return TemplateDefinition(
        blocks=[
            BlockDefinition(
                key="cash",
                label="Касса за смену",
                fields=[
                    FieldDefinition(
                        key="opening",
                        label="Остаток на начало смены",
                        kind=FieldKind.MONEY,
                        semantic=Semantic.OPENING_CASH,
                    ),
                    FieldDefinition(
                        key="sales",
                        label="Продажи наличными",
                        kind=FieldKind.MONEY,
                        semantic=Semantic.CASH_SALES,
                    ),
                    FieldDefinition(
                        key="refunds",
                        label="Возвраты наличными",
                        kind=FieldKind.MONEY,
                        semantic=Semantic.CASH_REFUNDS,
                    ),
                    FieldDefinition(
                        key="collected",
                        label="Инкассация",
                        kind=FieldKind.MONEY,
                        semantic=Semantic.CASH_COLLECTED,
                    ),
                    FieldDefinition(
                        key="closing",
                        label="Остаток на конец смены",
                        kind=FieldKind.MONEY,
                        semantic=Semantic.CLOSING_CASH,
                    ),
                    FieldDefinition(
                        key="revenue",
                        label="Общая выручка",
                        kind=FieldKind.MONEY,
                        semantic=Semantic.REVENUE,
                    ),
                ],
            ),
            BlockDefinition(
                key="expenses",
                label="Расходы из кассы",
                repeatable=True,
                fields=[
                    FieldDefinition(
                        key="amount",
                        label="Сумма",
                        kind=FieldKind.MONEY,
                        semantic=Semantic.CASH_EXPENSES,
                    ),
                    FieldDefinition(
                        key="description", label="Назначение покупки", kind=FieldKind.TEXT
                    ),
                    FieldDefinition(
                        key="receipts",
                        label="Фотографии чеков",
                        kind=FieldKind.ATTACHMENTS,
                        required=False,
                    ),
                ],
            ),
            BlockDefinition(
                key="notes",
                label="Комментарии",
                fields=[
                    FieldDefinition(
                        key="comment",
                        label="Пояснение сотрудника",
                        kind=FieldKind.TEXT,
                        required=False,
                    ),
                ],
            ),
        ]
    )


async def bootstrap() -> None:
    settings = get_settings()
    storage = S3Storage(settings)
    try:
        for attempt in range(30):
            try:
                await storage.ensure_bucket()
                break
            except Exception:
                if attempt == 29:
                    raise
                await asyncio.sleep(2)
        await ensure_topics(settings)
    finally:
        await storage.close()
    print("S3 bucket and Kafka topics are ready")


async def seed() -> None:
    settings = get_settings()
    if settings.app_env == Environment.PRODUCTION:
        raise SystemExit("Demo seeding is disabled in production")
    if not settings.seed_admin_password or not settings.seed_employee_password:
        raise SystemExit("Set SEED_ADMIN_PASSWORD and SEED_EMPLOYEE_PASSWORD before seeding")
    engine = create_engine(settings)
    try:
        async with session_factory(engine)() as session, session.begin():
            organization = await session.get(Organization, DEMO_ORGANIZATION_ID)
            if organization is None:
                session.add(Organization(id=DEMO_ORGANIZATION_ID, name="Demo Store"))
                await session.flush()
            for email, password, role in [
                ("admin@example.com", settings.seed_admin_password, Role.ADMIN),
                ("employee@example.com", settings.seed_employee_password, Role.EMPLOYEE),
            ]:
                user = await session.scalar(select(User).where(User.email == email))
                if user is None:
                    user = User(
                        email=email, password_hash=await hash_password(password.get_secret_value())
                    )
                    session.add(user)
                    await session.flush()
                existing = await session.get(Membership, (DEMO_ORGANIZATION_ID, user.id))
                if existing is None:
                    session.add(
                        Membership(organization_id=DEMO_ORGANIZATION_ID, user_id=user.id, role=role)
                    )
            await session.flush()
            version = await session.scalar(
                select(TemplateVersion)
                .where(TemplateVersion.organization_id == DEMO_ORGANIZATION_ID)
                .order_by(TemplateVersion.created_at)
                .limit(1)
            )
            if version is None:
                _, version = await create_template(
                    session,
                    DEMO_ORGANIZATION_ID,
                    TemplateCreate(name="Закрытие смены", definition=demo_definition()),
                )
            print(
                json.dumps(
                    {
                        "organization_id": str(DEMO_ORGANIZATION_ID),
                        "template_version_id": str(version.id),
                        "users": ["admin@example.com", "employee@example.com"],
                    }
                )
            )
    finally:
        await engine.dispose()


async def replay_dead_letter(partition: int, offset: int) -> None:
    settings = get_settings()
    consumer = AIOKafkaConsumer(**connection_options(settings), enable_auto_commit=False)
    producer = create_producer(settings)
    try:
        await consumer.start()
        await producer.start()
        topic_partition = TopicPartition(settings.kafka_dlq_topic, partition)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, offset)
        async with asyncio.timeout(15):
            record = await consumer.getone(topic_partition)
        if record.offset != offset:
            raise SystemExit("Requested DLQ offset is unavailable or has expired")
        dead_letter = json.loads(record.value)
        if dead_letter.get("payload_truncated"):
            raise SystemExit("Oversized truncated messages cannot be replayed")
        event = EventEnvelope.model_validate_json(
            base64.b64decode(dead_letter["value_base64"], validate=True)
        )
        if event.type != EventType.REPORT_SUBMITTED:
            raise SystemExit("Only valid report.submitted events can be replayed")
        async with asyncio.timeout(15):
            await producer.send_and_wait(
                settings.kafka_report_topic,
                event.model_dump_json().encode(),
                key=str(event.report_id).encode(),
            )
        print(
            json.dumps(
                {"replayed_event_id": str(event.id), "correlation_id": str(event.correlation_id)}
            )
        )
    finally:
        await consumer.stop()
        await producer.stop()


async def provision_organization(name: str, email: str, password: str) -> None:
    settings = get_settings()
    normalized_email = str(TypeAdapter(EmailStr).validate_python(email)).casefold()
    if not name.strip() or len(name) > 200 or not 12 <= len(password) <= 128:
        raise SystemExit("Organization name must be 1..200 characters; password 12..128 characters")
    engine = create_engine(settings)
    try:
        async with session_factory(engine)() as session, session.begin():
            if await session.scalar(select(User.id).where(User.email == normalized_email)):
                raise SystemExit("An account with this email already exists")
            organization = Organization(name=name.strip())
            administrator = User(
                email=normalized_email, password_hash=await hash_password(password)
            )
            session.add_all([organization, administrator])
            await session.flush()
            session.add(
                Membership(
                    organization_id=organization.id, user_id=administrator.id, role=Role.ADMIN
                )
            )
            await session.flush()
            output = {
                "organization_id": str(organization.id),
                "administrator_id": str(administrator.id),
            }
        print(json.dumps(output))
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="ShiftLedger operational commands")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("bootstrap", help="Create missing Kafka topics and S3 bucket")
    subparsers.add_parser("seed", help="Create an idempotent local demo organization and users")
    provision = subparsers.add_parser(
        "create-organization", help="Provision an organization and its first administrator"
    )
    provision.add_argument("--name", required=True)
    provision.add_argument("--admin-email", required=True)
    provision.add_argument(
        "--password-env",
        help="Read a password from this environment variable; otherwise prompt securely",
    )
    replay = subparsers.add_parser(
        "replay-dlq", help="Replay one valid event after fixing the underlying failure"
    )
    replay.add_argument("--partition", type=int, required=True)
    replay.add_argument("--offset", type=int, required=True)
    args = parser.parse_args()
    configure_logging(get_settings().log_level)
    if args.command == "bootstrap":
        asyncio.run(bootstrap())
    elif args.command == "seed":
        asyncio.run(seed())
    elif args.command == "create-organization":
        password = (
            os.environ.get(args.password_env, "")
            if args.password_env
            else getpass.getpass("Administrator password: ")
        )
        asyncio.run(provision_organization(args.name, args.admin_email, password))
    else:
        if args.partition < 0 or args.offset < 0:
            parser.error("partition and offset must be nonnegative")
        asyncio.run(replay_dead_letter(args.partition, args.offset))


if __name__ == "__main__":
    main()
