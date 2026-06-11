from __future__ import annotations

from datetime import UTC, datetime

from src.compactor.metrics import (
    CompactionRunMetrics,
    format_bytes,
    format_bytes_gb,
    format_bytes_mb,
)


class TestByteFormatting:
    def test_format_bytes_kb(self):
        assert format_bytes(2048) == "2.00 KB"

    def test_format_bytes_mb(self):
        assert format_bytes(5 * 1024 * 1024) == "5.00 MB"

    def test_format_bytes_gb(self):
        assert format_bytes(2 * 1024**3) == "2.000 GB"

    def test_format_bytes_mb_helper(self):
        assert format_bytes_mb(1048576) == 1.0

    def test_format_bytes_gb_helper(self):
        assert format_bytes_gb(1073741824) == 1.0


class TestCompactionRunMetrics:
    def test_mark_complete_success(self):
        started = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)
        metrics = CompactionRunMetrics(
            run_id="abc123",
            table_path="s3a://lakehouse/raw/events",
            started_at=started.isoformat(),
            compacted_partitions=2,
            files_before=100,
            files_after=8,
            bytes_before=104857600,
            bytes_after=99614720,
        )
        metrics.mark_complete(started)
        assert metrics.status == "success"
        assert metrics.compaction_duration_seconds is not None

    def test_mark_complete_partial_success(self):
        started = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)
        metrics = CompactionRunMetrics(
            run_id="abc123",
            table_path="s3a://lakehouse/raw/events",
            started_at=started.isoformat(),
            failed_partitions=[{"partition_id": "p1", "error": "spark error"}],
        )
        metrics.mark_complete(started)
        assert metrics.status == "partial_success"

    def test_to_dict_includes_required_fields(self):
        metrics = CompactionRunMetrics(
            run_id="run1",
            table_path="s3a://lakehouse/raw/events",
            started_at="2026-06-11T12:00:00Z",
            partitions_scanned=5,
            eligible_partitions=2,
            skipped_partitions=3,
            files_before=87,
            files_after=4,
            bytes_before=12582912,
            bytes_after=11892032,
            compaction_duration_seconds=14.5,
            lock_conflicts=1,
            failed_partitions=[],
            status="success",
        )
        data = metrics.to_dict()
        assert data["record_type"] == "compaction_run_summary"
        assert data["run_id"] == "run1"
        assert data["table_path"] == "s3a://lakehouse/raw/events"
        assert data["bytes_before_human"] == "12.00 MB"
        assert data["files_reduced"] == 83
