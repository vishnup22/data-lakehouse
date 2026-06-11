from __future__ import annotations

from src.compactor.metrics import EXIT_FAILURE, EXIT_NO_ELIGIBLE, EXIT_SUCCESS, CompactionRunMetrics


def _metrics(**kwargs) -> CompactionRunMetrics:
    defaults = {
        "run_id": "test",
        "table_path": "s3a://lakehouse/raw/events",
        "started_at": "2026-06-11T12:00:00Z",
    }
    defaults.update(kwargs)
    return CompactionRunMetrics(**defaults)


class TestExitCodes:
    def test_success_after_compaction(self):
        m = _metrics(compacted_partitions=2, eligible_partitions=2, files_after=4)
        assert m.exit_code() == EXIT_SUCCESS

    def test_no_eligible_partitions(self):
        m = _metrics(eligible_partitions=0, compacted_partitions=0)
        assert m.exit_code() == EXIT_NO_ELIGIBLE

    def test_failure_on_error(self):
        m = _metrics(error="table not found", status="error")
        assert m.exit_code() == EXIT_FAILURE

    def test_failure_when_all_partitions_fail(self):
        m = _metrics(
            eligible_partitions=2,
            compacted_partitions=0,
            failed_partitions=[{"partition_id": "p1", "error": "spark"}],
        )
        assert m.exit_code() == EXIT_FAILURE

    def test_dry_run_with_eligible_work_is_success(self):
        m = _metrics(
            status="dry_run",
            eligible_partitions=3,
            compacted_partitions=0,
            files_before=100,
            files_after=12,
        )
        assert m.exit_code() == EXIT_SUCCESS
