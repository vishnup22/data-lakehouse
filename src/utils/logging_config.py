from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "structured_fields") and isinstance(record.structured_fields, dict):
            payload.update(record.structured_fields)
        if record.exc_info and record.exc_info[1]:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(
    service_name: str, level: str = "INFO", log_file: str | None = None, json_format: bool = False
) -> logging.Logger:
    log_level = getattr(logging, level.upper(), logging.INFO)
    if json_format:
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    root = logging.getLogger()
    root.setLevel(log_level)
    if not root.handlers:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        root.addHandler(console)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
    return logging.getLogger(service_name)


def log_event(
    logger: logging.Logger,
    event: str,
    level: int = logging.INFO,
    message: str | None = None,
    **fields: Any,
) -> None:
    record = logger.makeRecord(
        name=logger.name, level=level, fn="", lno=0, msg=message or event, args=(), exc_info=None
    )
    record.structured_fields = {"event": event, **fields}
    logger.handle(record)
