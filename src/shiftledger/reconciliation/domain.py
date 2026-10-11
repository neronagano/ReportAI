from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel

from shiftledger.templates.domain import Semantic


class Evidence(BaseModel):
    source: Literal["report", "previous_report"]
    semantic: Semantic
    value: Decimal


class Finding(BaseModel):
    code: str
    severity: Literal["info", "warning"]
    message: str
    evidence: list[Evidence]


@dataclass(frozen=True)
class CheckResult:
    expected_cash: Decimal | None
    actual_cash: Decimal | None
    difference: Decimal | None
    findings: list[Finding]


CASH_INPUTS = frozenset(
    {
        Semantic.OPENING_CASH,
        Semantic.CASH_SALES,
        Semantic.CASH_REFUNDS,
        Semantic.CASH_EXPENSES,
        Semantic.CASH_COLLECTED,
        Semantic.CLOSING_CASH,
    }
)


def reconcile(
    current: dict[Semantic, Decimal],
    previous: dict[Semantic, Decimal] | None,
    revenue_change_threshold: Decimal,
) -> CheckResult:
    """Exact arithmetic produces facts; day-to-day movement is only an anomaly signal."""
    findings: list[Finding] = []
    expected = actual = difference = None
    if CASH_INPUTS.issubset(current):
        expected = (
            current[Semantic.OPENING_CASH]
            + current[Semantic.CASH_SALES]
            - current[Semantic.CASH_REFUNDS]
            - current[Semantic.CASH_EXPENSES]
            - current[Semantic.CASH_COLLECTED]
        )
        actual = current[Semantic.CLOSING_CASH]
        difference = actual - expected
        code = (
            "cash_balanced"
            if difference == 0
            else "cash_shortage"
            if difference < 0
            else "cash_surplus"
        )
        findings.append(
            Finding(
                code=code,
                severity="info" if difference == 0 else "warning",
                message=(
                    f"Expected cash {expected:.2f}; reported {actual:.2f}; "
                    f"difference {difference:.2f}"
                ),
                evidence=[
                    Evidence(source="report", semantic=semantic, value=current[semantic])
                    for semantic in sorted(CASH_INPUTS)
                ],
            )
        )
    else:
        missing = ", ".join(sorted(CASH_INPUTS - current.keys()))
        findings.append(
            Finding(
                code="cash_check_incomplete",
                severity="info",
                message=f"Cash reconciliation requires additional inputs: {missing}",
                evidence=[],
            )
        )
    if previous is None:
        findings.append(
            Finding(
                code="baseline_missing",
                severity="info",
                message=(
                    "No report for the preceding business date and the same "
                    "location, shift and template"
                ),
                evidence=[],
            )
        )
    elif Semantic.REVENUE in current and Semantic.REVENUE in previous:
        before = previous[Semantic.REVENUE]
        now = current[Semantic.REVENUE]
        evidence = [
            Evidence(source="previous_report", semantic=Semantic.REVENUE, value=before),
            Evidence(source="report", semantic=Semantic.REVENUE, value=now),
        ]
        if before == 0:
            if now != 0:
                findings.append(
                    Finding(
                        code="revenue_zero_baseline",
                        severity="info",
                        message="Previous revenue is zero; percentage comparison is undefined",
                        evidence=evidence,
                    )
                )
        elif abs((now - before) / before) >= revenue_change_threshold:
            change = ((now - before) / before * 100).quantize(Decimal("0.01"))
            findings.append(
                Finding(
                    code="revenue_drop" if change < 0 else "revenue_spike",
                    severity="warning",
                    message=(
                        f"Revenue changed by {change}% compared with the preceding "
                        "business date; review the business context"
                    ),
                    evidence=evidence,
                )
            )
    else:
        findings.append(
            Finding(
                code="revenue_check_incomplete",
                severity="info",
                message="Both reports need a revenue semantic for comparison",
                evidence=[],
            )
        )
    return CheckResult(expected, actual, difference, findings)
