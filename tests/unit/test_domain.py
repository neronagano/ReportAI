from decimal import Decimal

import pytest

from shiftledger.cli import demo_definition
from shiftledger.errors import ApplicationError
from shiftledger.reconciliation.domain import reconcile
from shiftledger.templates.domain import Semantic, ensure_compatible_version, validate_report


def sample_values() -> dict:
    return {
        "cash": {
            "opening": "1000",
            "sales": "5000",
            "refunds": "200",
            "collected": "2000",
            "closing": "3500",
            "revenue": "7000",
        },
        "expenses": [{"amount": "300", "description": "Расходные материалы"}],
        "notes": {"comment": "Доставка задержалась"},
    }


def test_cash_reconciliation_uses_exact_decimal_arithmetic() -> None:
    values = sample_values()
    values["cash"]["opening"] = "0.10"
    values["cash"]["sales"] = "0.20"
    values["cash"].update(refunds="0", collected="0", closing="0.30")
    values["expenses"] = []
    validated = validate_report(demo_definition(), values)
    result = reconcile(validated.amounts, None, Decimal("0.2"))
    assert result.expected_cash == Decimal("0.30")
    assert result.difference == Decimal("0.00")
    assert result.findings[0].code == "cash_balanced"


def test_revenue_drop_is_separate_from_cash_shortage() -> None:
    current = validate_report(demo_definition(), sample_values()).amounts
    current[Semantic.REVENUE] = Decimal("3500")
    previous = {Semantic.REVENUE: Decimal("7000")}
    result = reconcile(current, previous, Decimal("0.2"))
    codes = {finding.code for finding in result.findings}
    assert "revenue_drop" in codes
    assert "cash_shortage" not in codes


def test_cash_shortage_and_evidence() -> None:
    current = validate_report(demo_definition(), sample_values()).amounts
    current[Semantic.CLOSING_CASH] -= Decimal("250")
    result = reconcile(current, None, Decimal("0.2"))
    assert result.difference == Decimal("-250.00")
    assert result.findings[0].code == "cash_shortage"
    assert len(result.findings[0].evidence) == 6


@pytest.mark.parametrize("value", [0.1, True, "NaN", "Infinity", "1e3", "10.999", "-1.00"])
def test_money_rejects_inexact_or_invalid_financial_values(value: object) -> None:
    values = sample_values()
    values["cash"]["opening"] = value
    with pytest.raises(ApplicationError):
        validate_report(demo_definition(), values)


def test_missing_cash_inputs_do_not_invent_a_shortage() -> None:
    checked = reconcile({Semantic.REVENUE: Decimal("400")}, None, Decimal("0.2"))
    assert checked.difference is None
    assert checked.findings[0].code == "cash_check_incomplete"


def test_zero_baseline_does_not_divide_by_zero() -> None:
    checked = reconcile(
        {Semantic.REVENUE: Decimal("400")}, {Semantic.REVENUE: Decimal(0)}, Decimal("0.2")
    )
    assert "revenue_zero_baseline" in {finding.code for finding in checked.findings}


def test_versions_retain_meaning_of_stable_fields() -> None:
    previous = demo_definition()
    current = previous.model_copy(deep=True)
    current.blocks[0].fields[0].semantic = Semantic.CLOSING_CASH
    with pytest.raises(ApplicationError, match="changed meaning"):
        ensure_compatible_version(previous, current)


def test_report_rejects_unknown_fields_and_missing_required_input() -> None:
    values = sample_values()
    values["cash"]["unknown"] = "100"
    with pytest.raises(ApplicationError, match="unknown fields"):
        validate_report(demo_definition(), values)
    del values["cash"]["unknown"]
    del values["cash"]["opening"]
    with pytest.raises(ApplicationError, match="required field"):
        validate_report(demo_definition(), values)
