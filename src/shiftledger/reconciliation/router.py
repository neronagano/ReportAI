from uuid import UUID

from fastapi import APIRouter, Response
from sqlalchemy import select

from shiftledger.api.dependencies import MemberDep, SessionDep
from shiftledger.reconciliation.models import ReconciliationResult
from shiftledger.reconciliation.schemas import ReconciliationResponse, ResultResponse
from shiftledger.reports.service import get_report

router = APIRouter(prefix="/organizations/{organization_id}/reports", tags=["reconciliation"])


@router.get(
    "/{report_id}/reconciliation",
    response_model=ReconciliationResponse,
    responses={202: {"description": "Processing is pending"}},
)
async def result_for_report(
    report_id: UUID, response: Response, member: MemberDep, session: SessionDep
) -> ReconciliationResponse:
    await get_report(session, member, report_id)
    result = await session.scalar(
        select(ReconciliationResult).where(
            ReconciliationResult.organization_id == member.organization_id,
            ReconciliationResult.report_id == report_id,
        )
    )
    if result is None:
        response.status_code = 202
        return ReconciliationResponse(status="pending")
    return ReconciliationResponse(status="completed", result=ResultResponse.model_validate(result))
