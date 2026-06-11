from __future__ import annotations

import json

from src.streaming.data_quality import (
    REASON_INVALID_AMOUNT,
    REASON_MALFORMED_JSON,
    REASON_MISSING_EVENT_ID,
    REASON_NULL_EVENT_TIMESTAMP,
    compute_quality_metrics,
    parse_json_payload,
    validate_json_string,
    validate_record,
)


def _valid_event(**overrides) -> dict:
    base = {
        "event_id": "evt-001",
        "user_id": "user_001",
        "account_id": "acct_001",
        "event_type": "purchase",
        "amount": 19.99,
        "event_timestamp": "2026-06-11T12:00:00Z",
        "ingest_timestamp": "2026-06-11T12:00:01Z",
        "region": "us-east",
        "device_type": "mobile",
    }
    base.update(overrides)
    return base


class TestValidateRecord:
    def test_valid_record(self):
        result = validate_record(_valid_event())
        assert result.is_valid
        assert result.rejection_reason is None

    def test_missing_event_id(self):
        event = _valid_event()
        del event["event_id"]
        result = validate_record(event)
        assert not result.is_valid
        assert result.rejection_reason == REASON_MISSING_EVENT_ID

    def test_blank_event_id(self):
        result = validate_record(_valid_event(event_id="  "))
        assert not result.is_valid
        assert result.rejection_reason == REASON_MISSING_EVENT_ID

    def test_null_event_timestamp(self):
        result = validate_record(_valid_event(event_timestamp=None))
        assert not result.is_valid
        assert result.rejection_reason == REASON_NULL_EVENT_TIMESTAMP

    def test_invalid_event_timestamp(self):
        result = validate_record(_valid_event(event_timestamp="not-a-date"))
        assert not result.is_valid
        assert result.rejection_reason == REASON_NULL_EVENT_TIMESTAMP

    def test_invalid_amount_string(self):
        result = validate_record(_valid_event(amount="bad"))
        assert not result.is_valid
        assert result.rejection_reason == REASON_INVALID_AMOUNT

    def test_negative_amount(self):
        result = validate_record(_valid_event(amount=-1.0))
        assert not result.is_valid
        assert result.rejection_reason == REASON_INVALID_AMOUNT

    def test_missing_amount_is_valid(self):
        event = _valid_event()
        del event["amount"]
        result = validate_record(event)
        assert result.is_valid

    def test_malformed_empty_record(self):
        result = validate_record(None)
        assert not result.is_valid
        assert result.rejection_reason == REASON_MALFORMED_JSON


class TestJsonValidation:
    def test_valid_json_string(self):
        raw = json.dumps(_valid_event())
        result, record = validate_json_string(raw)
        assert result.is_valid
        assert record is not None

    def test_malformed_json(self):
        result, record = validate_json_string('{"event_id": ')
        assert not result.is_valid
        assert result.rejection_reason == REASON_MALFORMED_JSON
        assert record is None

    def test_parse_json_payload_array_rejected(self):
        record, reason = parse_json_payload("[1, 2, 3]")
        assert record is None
        assert reason == REASON_MALFORMED_JSON


class TestQualityMetrics:
    def test_metrics_counts(self):
        m = compute_quality_metrics(valid_count=98, bad_count=2, batch_id=1)
        assert m.valid_record_count == 98
        assert m.bad_record_count == 2
        assert m.bad_record_percentage == 2.0

    def test_metrics_zero_batch(self):
        m = compute_quality_metrics(valid_count=0, bad_count=0)
        assert m.bad_record_percentage == 0.0

    def test_metrics_to_dict(self):
        m = compute_quality_metrics(valid_count=80, bad_count=20, batch_id=3)
        data = m.to_dict()
        assert data["record_type"] == "quality_metrics"
        assert data["bad_record_percentage"] == 20.0
