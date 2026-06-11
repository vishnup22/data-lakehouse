from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

REQUIRED_FIELDS = ("event_id", "user_id", "account_id", "event_timestamp", "event_type")
REASON_MALFORMED_JSON = "malformed_json"
REASON_MISSING_EVENT_ID = "missing_event_id"
REASON_MISSING_USER_ID = "missing_user_id"
REASON_MISSING_ACCOUNT_ID = "missing_account_id"
REASON_MISSING_EVENT_TYPE = "missing_event_type"
REASON_NULL_EVENT_TIMESTAMP = "null_event_timestamp"
REASON_INVALID_AMOUNT = "invalid_amount"


@dataclass
class ValidationResult:
    is_valid: bool
    rejection_reason: str | None = None

    @classmethod
    def valid(cls) -> ValidationResult:
        return cls(is_valid=True)

    @classmethod
    def invalid(cls, reason: str) -> ValidationResult:
        return cls(is_valid=False, rejection_reason=reason)


@dataclass
class QualityMetrics:
    valid_record_count: int
    bad_record_count: int
    batch_id: int = 0
    timestamp: str = ""

    @property
    def total_record_count(self) -> int:
        return self.valid_record_count + self.bad_record_count

    @property
    def bad_record_percentage(self) -> float:
        if self.total_record_count == 0:
            return 0.0
        return round(self.bad_record_count / self.total_record_count * 100, 2)

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_type": "quality_metrics",
            "batch_id": self.batch_id,
            "timestamp": self.timestamp,
            "valid_record_count": self.valid_record_count,
            "bad_record_count": self.bad_record_count,
            "bad_record_percentage": self.bad_record_percentage,
            "total_record_count": self.total_record_count,
        }


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _is_valid_timestamp(value: Any) -> bool:
    if _is_blank(value):
        return False
    if isinstance(value, datetime):
        return True
    try:
        text = str(value).replace("Z", "+00:00")
        datetime.fromisoformat(text)
        return True
    except (ValueError, TypeError):
        return False


def _is_valid_amount(value: Any) -> bool:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return True
    try:
        return float(value) >= 0
    except (TypeError, ValueError):
        return False


def validate_record(record: dict[str, Any] | None) -> ValidationResult:
    if not record:
        return ValidationResult.invalid(REASON_MALFORMED_JSON)
    field_reasons = {
        "event_id": REASON_MISSING_EVENT_ID,
        "user_id": REASON_MISSING_USER_ID,
        "account_id": REASON_MISSING_ACCOUNT_ID,
        "event_type": REASON_MISSING_EVENT_TYPE,
    }
    for field, reason in field_reasons.items():
        if _is_blank(record.get(field)):
            return ValidationResult.invalid(reason)
    if not _is_valid_timestamp(record.get("event_timestamp")):
        return ValidationResult.invalid(REASON_NULL_EVENT_TIMESTAMP)
    if not _is_valid_amount(record.get("amount")):
        return ValidationResult.invalid(REASON_INVALID_AMOUNT)
    return ValidationResult.valid()


def parse_json_payload(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    if not raw or not raw.strip():
        return (None, REASON_MALFORMED_JSON)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return (None, REASON_MALFORMED_JSON)
    if not isinstance(data, dict):
        return (None, REASON_MALFORMED_JSON)
    return (data, None)


def validate_json_string(raw: str) -> tuple[ValidationResult, dict[str, Any] | None]:
    record, parse_reason = parse_json_payload(raw)
    if parse_reason:
        return (ValidationResult.invalid(parse_reason), None)
    result = validate_record(record)
    return (result, record if result.is_valid else record)


def compute_quality_metrics(
    valid_count: int, bad_count: int, batch_id: int = 0, timestamp: str = ""
) -> QualityMetrics:
    return QualityMetrics(
        valid_record_count=valid_count,
        bad_record_count=bad_count,
        batch_id=batch_id,
        timestamp=timestamp,
    )
