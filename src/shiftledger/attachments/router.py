from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, File, Response, UploadFile

from shiftledger.api.dependencies import MemberDep, ResourcesDep, SessionDep
from shiftledger.attachments.schemas import AttachmentResponse
from shiftledger.attachments.service import get_attachment, save_attachment

router = APIRouter(prefix="/organizations/{organization_id}/attachments", tags=["attachments"])


@router.post("", response_model=AttachmentResponse, status_code=201)
async def upload_receipt(
    file: Annotated[UploadFile, File()],
    member: MemberDep,
    session: SessionDep,
    resources: ResourcesDep,
) -> AttachmentResponse:
    try:
        data = await file.read(resources.settings.upload_max_bytes + 1)
        attachment = await save_attachment(
            session, resources.storage, resources.settings, member, data, file.content_type
        )
    finally:
        await file.close()
    return AttachmentResponse.model_validate(attachment)


@router.get("/{attachment_id}", response_model=AttachmentResponse)
async def attachment_info(
    attachment_id: UUID, member: MemberDep, session: SessionDep
) -> AttachmentResponse:
    return AttachmentResponse.model_validate(await get_attachment(session, member, attachment_id))


@router.get("/{attachment_id}/content")
async def download_receipt(
    attachment_id: UUID, member: MemberDep, session: SessionDep, resources: ResourcesDep
) -> Response:
    attachment = await get_attachment(session, member, attachment_id)
    data = await resources.storage.get(attachment.object_key)
    extension = attachment.object_key.rsplit(".", 1)[-1]
    return Response(
        data,
        media_type=attachment.content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{attachment.id}.{extension}"',
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )
