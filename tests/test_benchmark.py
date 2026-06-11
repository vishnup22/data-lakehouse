from __future__ import annotations

from pathlib import Path

from src.benchmark.benchmark_queries import (
    BenchmarkRun,
    QueryBenchmarkResult,
    find_comparison_pair,
    generate_markdown_report,
    resolve_phase,
)


def _run(
    phase: str, files: int, benchmark_id: str = "abc", query_time: float = 1.0
) -> BenchmarkRun:
    return BenchmarkRun(
        benchmark_id=benchmark_id,
        phase=phase,
        timestamp="2026-06-11T12:00:00Z",
        engine="duckdb",
        input_file_count=files,
        total_data_size_mb=12.0,
        table_path="s3a://lakehouse/raw/events",
        queries=[
            QueryBenchmarkResult(
                query_name="count_total_records",
                runtime_seconds=query_time,
                input_file_count=files,
                total_data_size_mb=12.0,
                timestamp="2026-06-11T12:00:00Z",
                row_count=1,
            )
        ],
    )


class TestResolvePhase:
    def test_explicit_before(self):
        run = _run("pending", files=50)
        assert resolve_phase(run, "before", []) == "before"

    def test_auto_first_run_is_before(self):
        run = _run("pending", files=50)
        assert resolve_phase(run, "auto", []) == "before"

    def test_auto_fewer_files_is_after(self):
        history = [_run("before", files=100, benchmark_id="b1")]
        run = _run("pending", files=10, benchmark_id="a1")
        assert resolve_phase(run, "auto", history) == "after"


class TestComparisonPair:
    def test_finds_before_and_after(self):
        before = _run("before", files=100, benchmark_id="b1", query_time=2.0)
        after = _run("after", files=8, benchmark_id="a1", query_time=0.5)
        history = [before]
        b, a = find_comparison_pair(history + [after], after)
        assert b is not None
        assert a is not None
        assert b.input_file_count == 100
        assert a.input_file_count == 8


class TestMarkdownReport:
    def test_generates_report_file(self, tmp_path: Path):
        before = _run("before", files=87, query_time=0.84)
        after = _run("after", files=4, query_time=0.21)
        report = tmp_path / "benchmark_report.md"
        generate_markdown_report(before, after, str(report))
        content = report.read_text(encoding="utf-8")
        assert "# Lakehouse Compaction Benchmark Report" in content
        assert "Before vs After Comparison" in content
        assert "count_total_records" in content
        assert "87" in content
        assert "4" in content
