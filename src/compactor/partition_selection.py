from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from src.compactor.config import CompactionConfig
from src.compactor.file_scanner import (
    PartitionStats,
    is_partition_eligible,
    partition_is_too_recent,
)


class SkipReason(StrEnum):
    TOO_RECENT = "too_recent"
    HEALTHY = "healthy"
    EMPTY = "empty"


@dataclass(frozen=True)
class PartitionSelectionPolicy:
    small_file_threshold_bytes: int
    small_file_count_threshold: int
    partition_file_count_threshold: int
    partition_age_skip_minutes: int

    @classmethod
    def from_config(cls, config: CompactionConfig) -> PartitionSelectionPolicy:
        return cls(
            small_file_threshold_bytes=config.small_file_threshold_bytes,
            small_file_count_threshold=config.small_file_count_threshold,
            partition_file_count_threshold=config.partition_file_count_threshold,
            partition_age_skip_minutes=config.partition_age_skip_minutes,
        )


@dataclass
class PartitionSelectionResult:
    stats: PartitionStats
    eligible: bool
    skip_reason: SkipReason | None = None
    small_file_count: int = 0

    @property
    def partition_id(self) -> str:
        return self.stats.partition_id


def evaluate_partition(
    stats: PartitionStats, policy: PartitionSelectionPolicy, now: datetime | None = None
) -> PartitionSelectionResult:
    if stats.file_count == 0:
        return PartitionSelectionResult(stats=stats, eligible=False, skip_reason=SkipReason.EMPTY)
    if partition_is_too_recent(stats, skip_minutes=policy.partition_age_skip_minutes, now=now):
        return PartitionSelectionResult(
            stats=stats,
            eligible=False,
            skip_reason=SkipReason.TOO_RECENT,
            small_file_count=len(stats.small_files(policy.small_file_threshold_bytes)),
        )
    small_count = len(stats.small_files(policy.small_file_threshold_bytes))
    eligible = is_partition_eligible(
        stats,
        small_file_threshold_bytes=policy.small_file_threshold_bytes,
        small_file_count_threshold=policy.small_file_count_threshold,
        partition_file_count_threshold=policy.partition_file_count_threshold,
    )
    return PartitionSelectionResult(
        stats=stats,
        eligible=eligible,
        skip_reason=None if eligible else SkipReason.HEALTHY,
        small_file_count=small_count,
    )


def select_partitions(
    partitions: list[PartitionStats], policy: PartitionSelectionPolicy, now: datetime | None = None
) -> tuple[list[PartitionStats], list[PartitionSelectionResult]]:
    eligible: list[PartitionStats] = []
    skipped: list[PartitionSelectionResult] = []
    for stats in partitions:
        result = evaluate_partition(stats, policy, now=now)
        if result.eligible:
            eligible.append(stats)
        else:
            skipped.append(result)
    return (eligible, skipped)


def selection_summary(policy: PartitionSelectionPolicy) -> dict[str, int | str]:
    return {
        "partition_age_skip_minutes": policy.partition_age_skip_minutes,
        "small_file_threshold_bytes": policy.small_file_threshold_bytes,
        "small_file_count_threshold": policy.small_file_count_threshold,
        "partition_file_count_threshold": policy.partition_file_count_threshold,
        "small_file_threshold_human": _bytes_label(policy.small_file_threshold_bytes),
    }


def _bytes_label(n: int) -> str:
    mb = n / (1024 * 1024)
    return f"{mb:.0f} MB" if mb >= 1 else f"{n} B"
