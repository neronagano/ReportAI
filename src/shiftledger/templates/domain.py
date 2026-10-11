import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from shiftledger.errors import ApplicationError

Identifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")]
ReportValues = dict[str, JsonValue]
MONEY_PATTERN = re.compile(r"^-?\d{1,16}(?:\.\d{1,2})?$")


class FieldKind(StrEnum):
    MONEY = "money"
    INTEGER = "integer"
    TEXT = "text"
    BOOLEAN = "boolean"
    ATTACHMENTS = "attachments"


class Semantic(StrEnum):
    OPENING_CASH = "opening_cash"
    CASH_SALES = "cash_sales"
    CASH_REFUNDS = "cash_refunds"
    CASH_EXPENSES = "cash_expenses"
    CASH_COLLECTED = "cash_collected"
    CLOSING_CASH = "closing_cash"
    REVENUE = "revenue"


class FieldDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: Identifier
    label: str = Field(min_length=1, max_length=200)
    kind: FieldKind
    required: bool = True
    semantic: Semantic | None = None

    @model_validator(mode="after")
    def check_semantic_type(self) -> "FieldDefinition":
        if self.semantic and self.kind != FieldKind.MONEY:
            raise ValueError("Financial semantics require a money field")
        if self.semantic and not self.required:
            raise ValueError("Financial semantic fields must be required")
        return self


class BlockDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: Identifier
    label: str = Field(min_length=1, max_length=200)
    repeatable: bool = False
    fields: list[FieldDefinition] = Field(min_length=1, max_length=30)

    @model_validator(mode="after")
    def check_field_keys(self) -> "BlockDefinition":
        keys = [field.key for field in self.fields]
        if len(keys) != len(set(keys)):
            raise ValueError("Field keys must be unique within a block")
        return self


class TemplateDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    currency: str = Field(default="RUB", pattern=r"^[A-Z]{3}$")
    blocks: list[BlockDefinition] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def check_keys_and_semantics(self) -> "TemplateDefinition":
        keys = [block.key for block in self.blocks]
        if len(keys) != len(set(keys)):
            raise ValueError("Block keys must be unique")
        semantics = [
            field.semantic for block in self.blocks for field in block.fields if field.semantic
        ]
        if len(semantics) != len(set(semantics)):
            raise ValueError("A financial semantic may be assigned to only one field")
        for block in self.blocks:
            if block.repeatable and any(
                field.semantic and field.semantic != Semantic.CASH_EXPENSES
                for field in block.fields
            ):
                raise ValueError("Only cash_expenses can be a repeated financial semantic")
        return self


def ensure_compatible_version(previous: TemplateDefinition, current: TemplateDefinition) -> None:
    """Stable keys retain their meaning when a new template version is published."""
    if previous.currency != current.currency:
        raise ApplicationError("incompatible_template", "Create a new template to change currency")
    old_blocks = {block.key: block for block in previous.blocks}
    for block in current.blocks:
        old = old_blocks.get(block.key)
        if old is None:
            continue
        if old.repeatable != block.repeatable:
            raise ApplicationError(
                "incompatible_template", f"Block {block.key} changed cardinality"
            )
        old_fields = {field.key: field for field in old.fields}
        for field in block.fields:
            before = old_fields.get(field.key)
            if before and (before.kind, before.semantic) != (field.kind, field.semantic):
                raise ApplicationError(
                    "incompatible_template", f"Field {block.key}.{field.key} changed meaning"
                )


@dataclass(frozen=True)
class ValidatedReport:
    values: ReportValues
    amounts: dict[Semantic, Decimal]
    attachment_ids: frozenset[UUID]


def _invalid(path: str, reason: str) -> ApplicationError:
    return ApplicationError("invalid_report_values", f"{path}: {reason}", 422)


def _validate_value(field: FieldDefinition, raw: JsonValue, path: str) -> JsonValue:
    match field.kind:
        case FieldKind.MONEY:
            # JSON floats have already lost precision before Decimal sees them.
            if isinstance(raw, bool) or not isinstance(raw, str | int):
                raise _invalid(path, "money must be a decimal string, e.g. '1250.50'")
            if not MONEY_PATTERN.fullmatch(str(raw)):
                raise _invalid(path, "money allows up to 16 digits and two decimal places")
            try:
                amount = Decimal(str(raw))
            except InvalidOperation as exc:
                raise _invalid(path, "invalid money") from exc
            if field.semantic and amount < 0:
                raise _invalid(path, "financial inputs must be nonnegative")
            return format(amount, ".2f")
        case FieldKind.INTEGER:
            if isinstance(raw, bool) or not isinstance(raw, int) or abs(raw) > 2**53 - 1:
                raise _invalid(path, "expected an integer within the JSON safe integer range")
            return raw
        case FieldKind.TEXT:
            if not isinstance(raw, str) or len(raw) > 4000:
                raise _invalid(path, "expected text up to 4000 characters")
            if field.required and not raw.strip():
                raise _invalid(path, "required text cannot be blank")
            return raw
        case FieldKind.BOOLEAN:
            if not isinstance(raw, bool):
                raise _invalid(path, "expected a boolean")
            return raw
        case FieldKind.ATTACHMENTS:
            if not isinstance(raw, list) or len(raw) > 10:
                raise _invalid(path, "expected a list of up to 10 attachment IDs")
            if field.required and not raw:
                raise _invalid(path, "at least one attachment is required")
            ids: list[JsonValue] = []
            for item in raw:
                try:
                    if not isinstance(item, str):
                        raise ValueError
                    ids.append(str(UUID(item)))
                except ValueError as exc:
                    raise _invalid(path, "invalid attachment ID") from exc
            if len(ids) != len(set(str(item) for item in ids)):
                raise _invalid(path, "duplicate attachment IDs")
            return ids
    raise _invalid(path, "unsupported field type")


def validate_report(definition: TemplateDefinition, raw: ReportValues) -> ValidatedReport:
    block_keys = {block.key for block in definition.blocks}
    if set(raw) - block_keys:
        raise _invalid("report", "unknown blocks")
    normalized: ReportValues = {}
    amounts: dict[Semantic, Decimal] = {}
    attachments: set[UUID] = set()
    for block in definition.blocks:
        value = raw.get(block.key)
        if block.repeatable:
            if not isinstance(value, list) or len(value) > 50:
                raise _invalid(block.key, "expected a list of up to 50 rows")
            rows = value
            # An empty expense list represents an explicit zero, not missing information.
            for field in block.fields:
                if field.semantic:
                    amounts[field.semantic] = Decimal("0.00")
        else:
            if not isinstance(value, dict):
                raise _invalid(block.key, "expected an object")
            rows = [value]
        normalized_rows: list[JsonValue] = []
        for index, row in enumerate(rows):
            path = f"{block.key}[{index}]" if block.repeatable else block.key
            if not isinstance(row, dict):
                raise _invalid(path, "expected an object")
            if set(row) - {field.key for field in block.fields}:
                raise _invalid(path, "unknown fields")
            normalized_row: dict[str, JsonValue] = {}
            for field in block.fields:
                field_path = f"{path}.{field.key}"
                item = row.get(field.key)
                if item is None:
                    if field.required:
                        raise _invalid(field_path, "required field is missing")
                    continue
                validated = _validate_value(field, item, field_path)
                normalized_row[field.key] = validated
                if field.semantic:
                    amounts[field.semantic] = amounts.get(field.semantic, Decimal(0)) + Decimal(
                        str(validated)
                    )
                if field.kind == FieldKind.ATTACHMENTS and isinstance(validated, list):
                    attachments.update(UUID(str(attachment)) for attachment in validated)
            normalized_rows.append(normalized_row)
        normalized[block.key] = normalized_rows if block.repeatable else normalized_rows[0]
    return ValidatedReport(normalized, amounts, frozenset(attachments))
