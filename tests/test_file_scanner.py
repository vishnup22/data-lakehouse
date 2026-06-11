from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.compactor.config import CompactionConfig
from src.compactor.file_scanner import (
    ParquetFileInfo,
    PartitionStats,
    _parse_partition_from_key,
    is_partition_eligible,
    is_small_file,
    partition_is_too_recent,
)


def _file(size: int, minutes_ago: int = 60) -> ParquetFileInfo:
    return ParquetFileInfo(
        key="raw/events/event_date=2024-01-01/event_hour=10/part-000.parquet",
        size_bytes=size,
        last_modified=datetime.now(UTC) - timedelta(minutes=minutes_ago),
    )


def _stats(files: list[ParquetFileInfo]) -> PartitionStats:
    return PartitionStats(event_date="2024-01-01", event_hour=10, files=files)


class TestSmallFileDetection:
    def test_file_below_threshold_is_small(self):
        threshold = 32 * 1024 * 1024
        assert is_small_file(10 * 1024 * 1024, threshold) is True

    def test_file_at_threshold_is_not_small(self):
        threshold = 32 * 1024 * 1024
        assert is_small_file(threshold, threshold) is False

    def test_file_above_threshold_is_not_small(self):
        threshold = 32 * 1024 * 1024
        assert is_small_file(64 * 1024 * 1024, threshold) is False


class TestPartitionEligibility:
    THRESHOLD = 32 * 1024 * 1024

    def test_eligible_when_many_small_files(self):
        files = [_file(1000000) for _ in range(25)]
        stats = _stats(files)
        assert is_partition_eligible(
            stats,
            small_file_threshold_bytes=self.THRESHOLD,
            small_file_count_threshold=20,
            partition_file_count_threshold=100,
        )

    def test_eligible_when_high_file_count(self):
        files = [_file(64 * 1024 * 1024) for _ in range(101)]
        stats = _stats(files)
        assert is_partition_eligible(
            stats,
            small_file_threshold_bytes=self.THRESHOLD,
            small_file_count_threshold=20,
            partition_file_count_threshold=100,
        )

    def test_not_eligible_when_healthy(self):
        files = [_file(64 * 1024 * 1024) for _ in range(5)]
        stats = _stats(files)
        assert not is_partition_eligible(
            stats,
            small_file_threshold_bytes=self.THRESHOLD,
            small_file_count_threshold=20,
            partition_file_count_threshold=100,
        )


class TestPartitionKeyParsing:
    def test_parses_key_under_configured_table_prefix(self):
        config = CompactionConfig(
            _env_file=None,
            delta_table_path="s3a://lakehouse/raw/events",
        )
        key = "raw/events/event_date=2024-06-11/event_hour=9/part-0001.parquet"
        assert _parse_partition_from_key(key, config) == ("2024-06-11", 9)

    def test_rejects_key_outside_table_prefix(self):
        config = CompactionConfig(
            _env_file=None,
            delta_table_path="s3a://lakehouse/raw/events",
        )
        key = "quarantine/events/event_date=2024-06-11/event_hour=9/part.parquet"
        assert _parse_partition_from_key(key, config) is None


class TestPartitionAge:
    def test_recent_partition_is_skipped(self):
        files = [_file(1000000, minutes_ago=5)]
        stats = _stats(files)
        assert partition_is_too_recent(stats, skip_minutes=30) is True

    def test_old_partition_is_not_skipped(self):
        files = [_file(1000000, minutes_ago=60)]
        stats = _stats(files)
        assert partition_is_too_recent(stats, skip_minutes=30) is False

    def test_empty_partition_is_skipped(self):
        stats = PartitionStats(event_date="2024-01-01", event_hour=10)
        assert partition_is_too_recent(stats, skip_minutes=30) is True
