from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime


def missing_event_id() -> str:
    return json.dumps(
        {
            "user_id": "user_00001",
            "account_id": "acct_00001",
            "event_type": "purchase",
            "amount": 10.0,
            "event_timestamp": datetime.now(UTC).isoformat(),
        }
    )


def null_event_timestamp() -> str:
    return json.dumps(
        {
            "event_id": "evt-bad-null-ts",
            "user_id": "user_00002",
            "account_id": "acct_00002",
            "event_type": "purchase",
            "amount": 10.0,
            "event_timestamp": None,
        }
    )


def invalid_amount() -> str:
    return json.dumps(
        {
            "event_id": "evt-bad-amount",
            "user_id": "user_00003",
            "account_id": "acct_00003",
            "event_type": "refund",
            "amount": "not-a-number",
            "event_timestamp": datetime.now(UTC).isoformat(),
        }
    )


def malformed_json() -> str:
    return '{"event_id": "evt-bad-json", "user_id": '


BAD_EVENT_GENERATORS: list[Callable[[], str]] = [
    missing_event_id,
    null_event_timestamp,
    invalid_amount,
    malformed_json,
]
