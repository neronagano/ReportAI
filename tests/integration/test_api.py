import asyncio
from collections.abc import AsyncIterator
from datetime import date
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from shiftledger.cli import demo_definition
from shiftledger.config import Settings
from shiftledger.database import session_factory
from shiftledger.identity.models import Membership, Organization, Role, User
from shiftledger.identity.security import hash_password, issue_token
from shiftledger.main import create_app
from shiftledger.messaging.models import OutboxMessage
from shiftledger.templates.schemas import TemplateCreate
from shiftledger.templates.service import create_template

pytestmark = pytest.mark.integration


@pytest.fixture
async def api_context(
    integration_engine: AsyncEngine, integration_settings: Settings
) -> AsyncIterator[tuple[httpx.AsyncClient, dict]]:
    organization = Organization(name="API integration store")
    user = User(
        email=f"{uuid4()}@example.com", password_hash=await hash_password("integration-password")
    )
    admin = User(email=f"{uuid4()}@example.com", password_hash="unused")
    async with session_factory(integration_engine)() as session, session.begin():
        session.add_all([organization, user, admin])
        await session.flush()
        session.add_all(
            [
                Membership(organization_id=organization.id, user_id=user.id, role=Role.EMPLOYEE),
                Membership(organization_id=organization.id, user_id=admin.id, role=Role.ADMIN),
            ]
        )
        await session.flush()
        _, version = await create_template(
            session, organization.id, TemplateCreate(name="Shift", definition=demo_definition())
        )
    app = create_app(integration_settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client,
    ):
        yield (
            client,
            {
                "organization_id": str(organization.id),
                "version_id": str(version.id),
                "email": user.email,
                "employee_token": issue_token(user.id, integration_settings),
                "admin_token": issue_token(admin.id, integration_settings),
            },
        )


async def test_missing_authentication_returns_standard_error(api_context: tuple) -> None:
    client, context = api_context
    response = await client.get(f"/api/v1/organizations/{context['organization_id']}/templates")
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json()["error"]["request_id"] == response.headers["x-request-id"]


async def test_employee_cannot_publish_templates_or_access_other_organization(
    api_context: tuple,
) -> None:
    client, context = api_context
    headers = {"Authorization": f"Bearer {context['employee_token']}"}
    response = await client.post(
        f"/api/v1/organizations/{context['organization_id']}/templates",
        headers=headers,
        json={"name": "Unauthorized", "definition": demo_definition().model_dump(mode="json")},
    )
    assert response.status_code == 403
    response = await client.get(f"/api/v1/organizations/{uuid4()}/templates", headers=headers)
    assert response.status_code == 403


async def test_login_and_password_are_not_echoed_on_validation_error(api_context: tuple) -> None:
    client, context = api_context
    valid = await client.post(
        "/api/v1/auth/login", json={"email": context["email"], "password": "integration-password"}
    )
    assert valid.status_code == 200
    invalid = await client.post(
        "/api/v1/auth/login", json={"email": "invalid", "password": "PRIVATE-PASSWORD"}
    )
    assert invalid.status_code == 422
    assert "PRIVATE-PASSWORD" not in invalid.text
    assert '"input"' not in invalid.text


async def test_api_submission_commits_before_response(
    api_context: tuple, integration_engine: AsyncEngine
) -> None:
    from datetime import date

    from sqlalchemy import select

    from shiftledger.reports.models import ShiftReport

    client, context = api_context
    headers = {
        "Authorization": f"Bearer {context['employee_token']}",
        "Idempotency-Key": str(uuid4()),
    }
    payload = {
        "template_version_id": context["version_id"],
        "business_date": date.today().isoformat(),
        "shift_key": "evening",
        "values": {
            "cash": {
                "opening": "0",
                "sales": "10",
                "refunds": "0",
                "collected": "0",
                "closing": "10",
                "revenue": "10",
            },
            "expenses": [],
            "notes": {},
        },
    }
    path = f"/api/v1/organizations/{context['organization_id']}/reports"
    response = await client.post(path, headers=headers, json=payload)
    assert response.status_code == 201
    async with session_factory(integration_engine)() as session:
        assert (
            await session.scalar(
                select(ShiftReport.id).where(
                    ShiftReport.organization_id == context["organization_id"]
                )
            )
            is not None
        )
    repeated = await client.post(path, headers=headers, json=payload)
    assert repeated.status_code == 200
    assert repeated.json()["id"] == response.json()["id"]


async def test_request_size_is_bounded_before_parsing(api_context: tuple) -> None:
    client, _ = api_context
    response = await client.post(
        "/api/v1/auth/login",
        content=b"x" * (513 * 1024),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 413


async def test_concurrent_submission_with_same_key_creates_one_report_and_event(
    api_context: tuple, integration_engine: AsyncEngine
) -> None:
    client, context = api_context
    headers = {
        "Authorization": f"Bearer {context['employee_token']}",
        "Idempotency-Key": str(uuid4()),
    }
    payload = {
        "template_version_id": context["version_id"],
        "business_date": date.today().isoformat(),
        "shift_key": "evening",
        "values": {
            "cash": {
                "opening": "0",
                "sales": "10",
                "refunds": "0",
                "collected": "0",
                "closing": "10",
                "revenue": "10",
            },
            "expenses": [],
            "notes": {},
        },
    }
    path = f"/api/v1/organizations/{context['organization_id']}/reports"
    responses = await asyncio.gather(
        *(client.post(path, headers=headers, json=payload) for _ in range(4))
    )
    assert sorted(response.status_code for response in responses) == [200, 200, 200, 201]
    assert len({response.json()["id"] for response in responses}) == 1
    async with session_factory(integration_engine)() as session:
        events = await session.scalar(select(func.count()).select_from(OutboxMessage))
        assert events == 1


async def test_streaming_request_body_is_bounded_without_content_length(api_context: tuple) -> None:
    client, _ = api_context

    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(3):
            yield b"x" * (200 * 1024)

    response = await client.post(
        "/api/v1/auth/login", content=chunks(), headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 413
