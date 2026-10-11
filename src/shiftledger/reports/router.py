from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Query, Request, Response
from sqlalchemy import select

from shiftledger.api.dependencies import MemberDep, ResourcesDep, SessionDep
from shiftledger.reports.models import ShiftReport
from shiftledger.reports.schemas import ReportResponse, ReportSubmit
from shiftledger.reports.service import get_report, submit_report

router = APIRouter(prefix="/organizations/{organization_id}/reports", tags=["reports"])


@router.post("", response_model=ReportResponse, status_code=201)
async def send_report(
    data: ReportSubmit,
    request: Request,
    response: Response,
    member: MemberDep,
    session: SessionDep,
    resources: ResourcesDep,
    idempotency_key: Annotated[
        str, Header(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    ],
) -> ReportResponse:
    report, created = await submit_report(
        session,
        resources.redis,
        resources.settings,
        member,
        data,
        idempotency_key,
        UUID(request.state.request_id),
    )
    if not created:
        response.status_code = 200
    return ReportResponse.model_validate(report)


@router.get("", response_model=list[ReportResponse])
async def list_reports(
    member: MemberDep,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
) -> list[ReportResponse]:
    query = select(ShiftReport).where(ShiftReport.organization_id == member.organization_id)
    if not member.is_admin:
        query = query.where(ShiftReport.submitted_by == member.user_id)
    reports = await session.scalars(
        query.order_by(ShiftReport.submitted_at.desc(), ShiftReport.id).limit(limit).offset(offset)
    )
    return [ReportResponse.model_validate(report) for report in reports]


@router.get("/{report_id}", response_model=ReportResponse)
async def show_report(report_id: UUID, member: MemberDep, session: SessionDep) -> ReportResponse:
    return ReportResponse.model_validate(await get_report(session, member, report_id))
