from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_NO_ELIGIBLE = 2


def format_bytes_mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 * 1024), 2)


def format_bytes_gb(size_bytes: int) -> float:
    return round(size_bytes / 1024**3, 3)


def format_bytes(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{format_bytes_gb(size_bytes):.3f} GB"
    if size_bytes >= 1024**2:
        return f"{format_bytes_mb(size_bytes):.2f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    return f"{size_bytes} B"


@dataclass
class CompactionRunMetrics:
    run_id: str
    table_path: str
    started_at: str
    completed_at: str | None = None
    compaction_duration_seconds: float | None = None
    partitions_scanned: int = 0
    eligible_partitions: int = 0
    skipped_partitions: int = 0
    compacted_partitions: int = 0
    lock_conflicts: int = 0
    files_before: int = 0
    files_after: int = 0
    bytes_before: int = 0
    bytes_after: int = 0
    failed_partitions: list[dict[str, str]] = field(default_factory=list)
    partition_details: list[dict[str, Any]] = field(default_factory=list)
    status: str = "running"
    error: str | None = None

    def mark_complete(self, started: datetime) -> None:
        finished = datetime.now(UTC)
        self.completed_at = finished.isoformat().replace("+00:00", "Z")
        self.compaction_duration_seconds = round((finished - started).total_seconds(), 3)
        if self.status == "dry_run":
            return
        if self.error:
            self.status = "error"
        elif self.failed_partitions:
            self.status = "partial_success"
        else:
            self.status = "success"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["bytes_before_human"] = format_bytes(self.bytes_before)
        data["bytes_after_human"] = format_bytes(self.bytes_after)
        data["bytes_before_mb"] = format_bytes_mb(self.bytes_before)
        data["bytes_after_mb"] = format_bytes_mb(self.bytes_after)
        data["files_reduced"] = self.files_before - self.files_after
        data["record_type"] = "compaction_run_summary"
        return data

    def exit_code(self) -> int:
        if self.error:
            return EXIT_FAILURE
        if self.status == "error":
            return EXIT_FAILURE
        if self.eligible_partitions == 0 and self.compacted_partitions == 0:
            return EXIT_NO_ELIGIBLE
        if self.failed_partitions and self.compacted_partitions == 0:
            return EXIT_FAILURE
        return EXIT_SUCCESS

    def summary_message(self) -> str:
        return f"run={self.run_id} status={self.status} partitions={self.compacted_partitions}/{self.eligible_partitions} compacted files {self.files_before}→{self.files_after} size {format_bytes(self.bytes_before)}→{format_bytes(self.bytes_after)} duration={self.compaction_duration_seconds}s lock_conflicts={self.lock_conflicts} failed={len(self.failed_partitions)}"


@dataclass
class PartitionCompactionMetrics:
    partition_id: str
    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int
    duration_seconds: float
    method: str
    status: str = "success"
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["bytes_before_human"] = format_bytes(self.bytes_before)
        data["bytes_after_human"] = format_bytes(self.bytes_after)
        data["bytes_before_mb"] = format_bytes_mb(self.bytes_before)
        data["bytes_after_mb"] = format_bytes_mb(self.bytes_after)
        return data


class MetricsWriter:
    def __init__(self, metrics_path: str) -> None:
        self.path = Path(metrics_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write_run_summary(self, metrics: CompactionRunMetrics) -> None:
        line = json.dumps(metrics.to_dict(), default=str)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
