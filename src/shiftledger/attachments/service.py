import hashlib
import warnings
from io import BytesIO
from uuid import UUID, uuid4

import anyio
from PIL import Image, UnidentifiedImageError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.attachments.models import Attachment
from shiftledger.attachments.storage import ObjectStorage
from shiftledger.config import Settings
from shiftledger.errors import ApplicationError, not_found
from shiftledger.identity.access import MemberContext

IMAGE_TYPES = {
    "JPEG": ("image/jpeg", "jpg"),
    "PNG": ("image/png", "png"),
    "WEBP": ("image/webp", "webp"),
}


def inspect_image(data: bytes, max_pixels: int) -> tuple[str, str]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(data)) as image:
                if image.format not in IMAGE_TYPES:
                    raise ApplicationError(
                        "invalid_file", "Only JPEG, PNG and WebP receipts are accepted", 415
                    )
                if image.width * image.height > max_pixels:
                    raise ApplicationError(
                        "invalid_file", "Image dimensions exceed the permitted limit", 413
                    )
                detected = IMAGE_TYPES[image.format]
                image.verify()
                return detected
    except (
        UnidentifiedImageError,
        OSError,
        SyntaxError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ) as exc:
        raise ApplicationError("invalid_file", "Receipt must be a valid image", 415) from exc


async def save_attachment(
    session: AsyncSession,
    storage: ObjectStorage,
    settings: Settings,
    member: MemberContext,
    data: bytes,
    supplied_type: str | None,
) -> Attachment:
    if not data or len(data) > settings.upload_max_bytes:
        raise ApplicationError(
            "invalid_file_size", "Receipt size is outside the permitted range", 413
        )
    content_type, extension = await anyio.to_thread.run_sync(
        inspect_image, data, settings.upload_max_pixels
    )
    if supplied_type and supplied_type != content_type:
        raise ApplicationError(
            "invalid_file_type", "Declared content type does not match the image", 415
        )
    attachment_id = uuid4()
    key = f"{member.organization_id}/{attachment_id}.{extension}"
    await storage.put(key, data, content_type)
    attachment = Attachment(
        id=attachment_id,
        organization_id=member.organization_id,
        uploaded_by=member.user_id,
        object_key=key,
        content_type=content_type,
        size_bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )
    try:
        session.add(attachment)
        await session.flush()
    except Exception:
        await storage.delete(key)
        raise
    return attachment


async def get_attachment(
    session: AsyncSession, member: MemberContext, attachment_id: UUID
) -> Attachment:
    query = select(Attachment).where(
        Attachment.organization_id == member.organization_id, Attachment.id == attachment_id
    )
    if not member.is_admin:
        query = query.where(Attachment.uploaded_by == member.user_id)
    attachment = await session.scalar(query)
    if attachment is None:
        raise not_found("Attachment")
    return attachment


async def bind_attachments(
    session: AsyncSession, member: MemberContext, report_id: UUID, attachment_ids: frozenset[UUID]
) -> None:
    if not attachment_ids:
        return
    query = (
        select(Attachment)
        .where(
            Attachment.organization_id == member.organization_id, Attachment.id.in_(attachment_ids)
        )
        .order_by(Attachment.id)
        .with_for_update()
    )
    if not member.is_admin:
        query = query.where(Attachment.uploaded_by == member.user_id)
    rows = (await session.scalars(query)).all()
    if len(rows) != len(attachment_ids):
        raise not_found("Attachment")
    for attachment in rows:
        if attachment.report_id is not None and attachment.report_id != report_id:
            raise ApplicationError(
                "attachment_already_used", "Receipt is already linked to another report", 409
            )
        attachment.report_id = report_id
