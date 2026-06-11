from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.compactor.config import CompactionConfig
from src.compactor.file_scanner import ParquetFileInfo, PartitionStats
from src.compactor.partition_selection import (
    PartitionSelectionPolicy,
    SkipReason,
    evaluate_partition,
    select_partitions,
)


def _file(size: int, minutes_ago: int = 60) -> ParquetFileInfo:
    return ParquetFileInfo(
        key="raw/events/event_date=2024-01-01/event_hour=10/part-000.parquet",
        size_bytes=size,
        last_modified=datetime.now(UTC) - timedelta(minutes=minutes_ago),
    )


def _stats(files: list[ParquetFileInfo]) -> PartitionStats:
    return PartitionStats(event_date="2024-01-01", event_hour=10, files=files)


@pytest.fixture
def policy() -> PartitionSelectionPolicy:
    return PartitionSelectionPolicy(
        small_file_threshold_bytes=32 * 1024 * 1024,
        small_file_count_threshold=20,
        partition_file_count_threshold=100,
        partition_age_skip_minutes=30,
    )


class TestPartitionSelectionPolicy:
    def test_from_config(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("SMALL_FILE_COUNT_THRESHOLD", "15")
        monkeypatch.setenv("PARTITION_AGE_SKIP_MINUTES", "45")
        config = CompactionConfig(_env_file=None)
        p = PartitionSelectionPolicy.from_config(config)
        assert p.small_file_count_threshold == 15
        assert p.partition_age_skip_minutes == 45

    def test_skip_too_recent(self, policy: PartitionSelectionPolicy):
        stats = _stats([_file(1000000, minutes_ago=5)])
        result = evaluate_partition(stats, policy)
        assert not result.eligible
        assert result.skip_reason == SkipReason.TOO_RECENT

    def test_skip_healthy_partition(self, policy: PartitionSelectionPolicy):
        files = [_file(64 * 1024 * 1024) for _ in range(5)]
        result = evaluate_partition(_stats(files), policy)
        assert not result.eligible
        assert result.skip_reason == SkipReason.HEALTHY

    def test_eligible_many_small_files(self, policy: PartitionSelectionPolicy):
        files = [_file(1000000, minutes_ago=60) for _ in range(21)]
        result = evaluate_partition(_stats(files), policy)
        assert result.eligible
        assert result.skip_reason is None
        assert result.small_file_count == 21

    def test_not_eligible_at_exactly_20_small_files(self, policy: PartitionSelectionPolicy):
        files = [_file(1000000, minutes_ago=60) for _ in range(20)]
        result = evaluate_partition(_stats(files), policy)
        assert not result.eligible
        assert result.skip_reason == SkipReason.HEALTHY

    def test_eligible_high_total_file_count(self, policy: PartitionSelectionPolicy):
        files = [_file(64 * 1024 * 1024, minutes_ago=60) for _ in range(101)]
        result = evaluate_partition(_stats(files), policy)
        assert result.eligible

    def test_not_eligible_at_exactly_100_files(self, policy: PartitionSelectionPolicy):
        files = [_file(64 * 1024 * 1024, minutes_ago=60) for _ in range(100)]
        result = evaluate_partition(_stats(files), policy)
        assert not result.eligible

    def test_skip_empty_partition(self, policy: PartitionSelectionPolicy):
        result = evaluate_partition(PartitionStats(event_date="2024-01-01", event_hour=10), policy)
        assert not result.eligible
        assert result.skip_reason == SkipReason.EMPTY

    def test_select_partitions_splits_correctly(self, policy: PartitionSelectionPolicy):
        old_small = _stats([_file(1000, minutes_ago=60) for _ in range(25)])
        recent = _stats([_file(1000, minutes_ago=5) for _ in range(25)])
        healthy = _stats([_file(64 * 1024 * 1024, minutes_ago=60) for _ in range(3)])
        eligible, skipped = select_partitions([old_small, recent, healthy], policy)
        assert len(eligible) == 1
        assert eligible[0].partition_id == old_small.partition_id
        assert len(skipped) == 2
        reasons = {s.skip_reason for s in skipped}
        assert SkipReason.TOO_RECENT in reasons
        assert SkipReason.HEALTHY in reasons

    def test_configurable_thresholds(self):
        custom = PartitionSelectionPolicy(
            small_file_threshold_bytes=1024,
            small_file_count_threshold=5,
            partition_file_count_threshold=10,
            partition_age_skip_minutes=10,
        )
        files = [_file(512, minutes_ago=60) for _ in range(6)]
        result = evaluate_partition(_stats(files), custom)
        assert result.eligible
