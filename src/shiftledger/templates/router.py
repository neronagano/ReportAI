from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from sqlalchemy import select

from shiftledger.api.dependencies import AdminDep, MemberDep, SessionDep
from shiftledger.templates.models import Template, TemplateVersion
from shiftledger.templates.schemas import (
    PublishedTemplateResponse,
    TemplateCreate,
    TemplateResponse,
    VersionCreate,
    VersionResponse,
)
from shiftledger.templates.service import create_template, get_version, publish_version

router = APIRouter(prefix="/organizations/{organization_id}", tags=["templates"])


@router.post("/templates", response_model=PublishedTemplateResponse, status_code=201)
async def add_template(
    data: TemplateCreate, member: AdminDep, session: SessionDep
) -> PublishedTemplateResponse:
    template, version = await create_template(session, member.organization_id, data)
    return PublishedTemplateResponse(
        template=TemplateResponse.model_validate(template),
        version=VersionResponse.model_validate(version),
    )


@router.get("/templates", response_model=list[TemplateResponse])
async def list_templates(
    member: MemberDep,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
) -> list[TemplateResponse]:
    templates = await session.scalars(
        select(Template)
        .where(Template.organization_id == member.organization_id)
        .order_by(Template.created_at, Template.id)
        .limit(limit)
        .offset(offset)
    )
    return [TemplateResponse.model_validate(template) for template in templates]


@router.post("/templates/{template_id}/versions", response_model=VersionResponse, status_code=201)
async def add_version(
    template_id: UUID, data: VersionCreate, member: AdminDep, session: SessionDep
) -> VersionResponse:
    version = await publish_version(session, member.organization_id, template_id, data.definition)
    return VersionResponse.model_validate(version)


@router.get("/templates/{template_id}/versions", response_model=list[VersionResponse])
async def list_versions(
    template_id: UUID,
    member: MemberDep,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
) -> list[VersionResponse]:
    versions = await session.scalars(
        select(TemplateVersion)
        .where(
            TemplateVersion.organization_id == member.organization_id,
            TemplateVersion.template_id == template_id,
        )
        .order_by(TemplateVersion.version.desc())
        .limit(limit)
        .offset(offset)
    )
    return [VersionResponse.model_validate(version) for version in versions]


@router.get("/template-versions/{version_id}", response_model=VersionResponse)
async def show_version(version_id: UUID, member: MemberDep, session: SessionDep) -> VersionResponse:
    return VersionResponse.model_validate(
        await get_version(session, member.organization_id, version_id)
    )
