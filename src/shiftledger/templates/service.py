from uuid import UUID

import structlog
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.errors import not_found
from shiftledger.templates.domain import TemplateDefinition, ensure_compatible_version
from shiftledger.templates.models import Template, TemplateVersion
from shiftledger.templates.schemas import TemplateCreate

log = structlog.get_logger()


async def create_template(
    session: AsyncSession, organization_id: UUID, data: TemplateCreate
) -> tuple[Template, TemplateVersion]:
    template = Template(organization_id=organization_id, name=data.name)
    session.add(template)
    await session.flush()
    version = TemplateVersion(
        organization_id=organization_id,
        template_id=template.id,
        version=1,
        definition=data.definition.model_dump(mode="json"),
    )
    session.add(version)
    await session.flush()
    return template, version


async def publish_version(
    session: AsyncSession, organization_id: UUID, template_id: UUID, definition: TemplateDefinition
) -> TemplateVersion:
    # Lock the parent to serialize concurrent version number allocation.
    template = await session.scalar(
        select(Template)
        .where(Template.id == template_id, Template.organization_id == organization_id)
        .with_for_update()
    )
    if template is None:
        raise not_found("Template")
    previous = (
        await session.scalars(
            select(TemplateVersion)
            .where(
                TemplateVersion.organization_id == organization_id,
                TemplateVersion.template_id == template_id,
            )
            .order_by(TemplateVersion.version)
        )
    ).all()
    for version in previous:
        ensure_compatible_version(TemplateDefinition.model_validate(version.definition), definition)
    published = TemplateVersion(
        organization_id=organization_id,
        template_id=template_id,
        version=previous[-1].version + 1,
        definition=definition.model_dump(mode="json"),
    )
    session.add(published)
    await session.flush()
    return published


async def get_version(
    session: AsyncSession, organization_id: UUID, version_id: UUID
) -> TemplateVersion:
    version = await session.scalar(
        select(TemplateVersion).where(
            TemplateVersion.organization_id == organization_id, TemplateVersion.id == version_id
        )
    )
    if version is None:
        raise not_found("Template version")
    return version


async def get_definition(
    session: AsyncSession, cache: Redis, organization_id: UUID, version_id: UUID, ttl: int
) -> TemplateDefinition:
    # Membership is authorized before this call. Immutable versions need no invalidation.
    key = f"shiftledger:template:v1:{organization_id}:{version_id}"
    try:
        cached = await cache.get(key)
        if cached:
            return TemplateDefinition.model_validate_json(cached)
    except (RedisError, ValidationError):
        log.warning("template_cache_read_failed", version_id=str(version_id))
    version = await get_version(session, organization_id, version_id)
    definition = TemplateDefinition.model_validate(version.definition)
    try:
        await cache.set(key, definition.model_dump_json(), ex=ttl)
    except RedisError:
        log.warning("template_cache_write_failed", version_id=str(version_id))
    return definition
