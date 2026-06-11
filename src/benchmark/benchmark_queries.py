from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from src.compactor.config import CompactionConfig
from src.compactor.file_scanner import scan_partitions
from src.compactor.metrics import format_bytes, format_bytes_mb
from src.utils.logging_config import setup_logging

Phase = Literal["before", "after", "auto"]
Engine = Literal["duckdb", "pyspark"]
BENCHMARK_RESULTS_PATH = os.getenv("BENCHMARK_RESULTS_PATH", "logs/benchmark_results.jsonl")
BENCHMARK_REPORT_PATH = os.getenv("BENCHMARK_REPORT_PATH", "logs/benchmark_report.md")


@dataclass
class QueryBenchmarkResult:
    query_name: str
    runtime_seconds: float
    input_file_count: int
    total_data_size_mb: float
    timestamp: str
    row_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkRun:
    benchmark_id: str
    phase: str
    timestamp: str
    engine: str
    input_file_count: int
    total_data_size_mb: float
    table_path: str
    queries: list[QueryBenchmarkResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["record_type"] = "benchmark_run"
        return data


DUCKDB_QUERIES: dict[str, str] = {
    "count_by_event_type": "\n        SELECT event_type, COUNT(*) AS cnt\n        FROM read_parquet('{glob}')\n        GROUP BY event_type\n        ORDER BY cnt DESC\n    ",
    "filter_by_region": "\n        SELECT event_id, event_type, amount, user_id\n        FROM read_parquet('{glob}')\n        WHERE region = 'us-east'\n        LIMIT 1000\n    ",
    "aggregate_by_region_date": "\n        SELECT region, CAST(event_date AS VARCHAR) AS event_date,\n               SUM(amount) AS total_amount, COUNT(*) AS cnt\n        FROM read_parquet('{glob}')\n        GROUP BY region, event_date\n        ORDER BY total_amount DESC\n    ",
    "count_total_records": "\n        SELECT COUNT(*) AS total_records\n        FROM read_parquet('{glob}')\n    ",
}
SPARK_QUERIES: dict[str, str] = {
    "count_by_event_type": "\n        SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type ORDER BY cnt DESC\n    ",
    "filter_by_region": "\n        SELECT event_id, event_type, amount, user_id FROM events\n        WHERE region = 'us-east' LIMIT 1000\n    ",
    "aggregate_by_region_date": "\n        SELECT region, event_date, SUM(amount) AS total_amount, COUNT(*) AS cnt\n        FROM events GROUP BY region, event_date ORDER BY total_amount DESC\n    ",
    "count_total_records": "\n        SELECT COUNT(*) AS total_records FROM events\n    ",
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def count_parquet_files(config: CompactionConfig) -> tuple[int, int]:
    partitions = scan_partitions(config)
    files = sum(p.file_count for p in partitions)
    bytes_total = sum(p.total_bytes for p in partitions)
    return (files, bytes_total)


def _parquet_glob_s3(config: CompactionConfig) -> str:
    return f"s3://{config.minio_bucket}/{config.table_key_prefix}/**/*.parquet"


def _configure_duckdb_s3(con: Any, config: CompactionConfig) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = config.minio_endpoint.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")
    con.execute(f"SET s3_access_key_id='{config.minio_root_user}';")
    con.execute(f"SET s3_secret_access_key='{config.minio_root_password}';")


def run_duckdb_queries(
    config: CompactionConfig, file_count: int, total_bytes: int
) -> list[QueryBenchmarkResult]:
    import duckdb

    con = duckdb.connect()
    _configure_duckdb_s3(con, config)
    glob = _parquet_glob_s3(config)
    size_mb = format_bytes_mb(total_bytes)
    results: list[QueryBenchmarkResult] = []
    for name, sql_template in DUCKDB_QUERIES.items():
        sql = sql_template.format(glob=glob)
        ts = _utc_now()
        start = time.perf_counter()
        df = con.execute(sql).fetchdf()
        elapsed = round(time.perf_counter() - start, 4)
        results.append(
            QueryBenchmarkResult(
                query_name=name,
                runtime_seconds=elapsed,
                input_file_count=file_count,
                total_data_size_mb=size_mb,
                timestamp=ts,
                row_count=len(df),
            )
        )
    con.close()
    return results


def run_pyspark_queries(
    config: CompactionConfig, file_count: int, total_bytes: int
) -> list[QueryBenchmarkResult]:
    from src.compactor.delta_compactor import create_spark_session

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.format("delta").load(config.delta_table_path)
    df.createOrReplaceTempView("events")
    size_mb = format_bytes_mb(total_bytes)
    results: list[QueryBenchmarkResult] = []
    for name, sql in SPARK_QUERIES.items():
        ts = _utc_now()
        start = time.perf_counter()
        out = spark.sql(sql).collect()
        elapsed = round(time.perf_counter() - start, 4)
        results.append(
            QueryBenchmarkResult(
                query_name=name,
                runtime_seconds=elapsed,
                input_file_count=file_count,
                total_data_size_mb=size_mb,
                timestamp=ts,
                row_count=len(out),
            )
        )
    spark.stop()
    return results


def measure_parquet_inventory(
    config: CompactionConfig, file_count: int, total_bytes: int
) -> QueryBenchmarkResult:
    ts = _utc_now()
    start = time.perf_counter()
    files, bytes_total = count_parquet_files(config)
    elapsed = round(time.perf_counter() - start, 4)
    return QueryBenchmarkResult(
        query_name="parquet_file_inventory",
        runtime_seconds=elapsed,
        input_file_count=files,
        total_data_size_mb=format_bytes_mb(bytes_total),
        timestamp=ts,
        row_count=files,
    )


def execute_benchmark(config: CompactionConfig, engine: Engine) -> BenchmarkRun:
    file_count, total_bytes = count_parquet_files(config)
    if file_count == 0:
        raise RuntimeError(
            f"No Parquet files found under {config.delta_table_path}. Run `make produce` and `make stream` first."
        )
    inventory = measure_parquet_inventory(config, file_count, total_bytes)
    if engine == "duckdb":
        query_results = run_duckdb_queries(config, file_count, total_bytes)
    else:
        query_results = run_pyspark_queries(config, file_count, total_bytes)
    query_results.append(inventory)
    return BenchmarkRun(
        benchmark_id=uuid.uuid4().hex[:12],
        phase="pending",
        timestamp=_utc_now(),
        engine=engine,
        input_file_count=file_count,
        total_data_size_mb=format_bytes_mb(total_bytes),
        table_path=config.delta_table_path,
        queries=query_results,
    )


def load_benchmark_runs(path: str) -> list[BenchmarkRun]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    runs: list[BenchmarkRun] = []
    for line in file_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        if data.get("record_type") != "benchmark_run":
            continue
        queries = [QueryBenchmarkResult(**q) for q in data.get("queries", [])]
        runs.append(
            BenchmarkRun(
                benchmark_id=data["benchmark_id"],
                phase=data["phase"],
                timestamp=data["timestamp"],
                engine=data["engine"],
                input_file_count=data["input_file_count"],
                total_data_size_mb=data["total_data_size_mb"],
                table_path=data["table_path"],
                queries=queries,
            )
        )
    return runs


def save_benchmark_run(run: BenchmarkRun, path: str) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(run.to_dict(), default=str) + "\n")


def resolve_phase(run: BenchmarkRun, phase: Phase, history: list[BenchmarkRun]) -> str:
    if phase in ("before", "after"):
        return phase
    before_runs = [r for r in history if r.phase == "before"]
    if not before_runs:
        return "before"
    baseline = max(before_runs, key=lambda r: r.input_file_count)
    if run.input_file_count < baseline.input_file_count:
        return "after"
    return "before"


def find_comparison_pair(
    history: list[BenchmarkRun], current: BenchmarkRun
) -> tuple[BenchmarkRun | None, BenchmarkRun | None]:
    before_runs = [r for r in history if r.phase == "before"]
    after_runs = [r for r in history if r.phase == "after"]
    if current.phase == "after":
        baseline = max(before_runs, key=lambda r: r.input_file_count) if before_runs else None
        return (baseline, current)
    if current.phase == "before" and after_runs:
        latest_after = max(after_runs, key=lambda r: r.timestamp)
        baseline = max(before_runs, key=lambda r: r.input_file_count)
        return (baseline, latest_after)
    return (None, None)


def _pct_change(before: float, after: float) -> str:
    if before == 0:
        return "n/a"
    change = (after - before) / before * 100
    sign = "+" if change > 0 else ""
    return f"{sign}{change:.1f}%"


def print_single_run_table(run: BenchmarkRun) -> None:
    print(f"\n{'=' * 78}")
    print(f"  BENCHMARK RUN — {run.phase.upper()} COMPACTION")
    print(f"  benchmark_id: {run.benchmark_id}  |  engine: {run.engine}")
    print(
        f"  Parquet files: {run.input_file_count}  |  Data size: {format_bytes(int(run.total_data_size_mb * 1024 * 1024))}"
    )
    print(f"{'=' * 78}")
    print(f"{'Query':<32} {'Runtime (s)':>12} {'Rows':>8}")
    print(f"{'-' * 32} {'-' * 12} {'-' * 8}")
    for q in run.queries:
        print(f"{q.query_name:<32} {q.runtime_seconds:>12.4f} {q.row_count:>8}")
    print(f"{'=' * 78}\n")


def print_comparison_table(before: BenchmarkRun, after: BenchmarkRun) -> None:
    before_map = {q.query_name: q for q in before.queries}
    after_map = {q.query_name: q for q in after.queries}
    all_names = sorted(set(before_map) | set(after_map))
    print(f"\n{'=' * 90}")
    print("  BENCHMARK COMPARISON: BEFORE vs AFTER COMPACTION")
    print(f"{'=' * 90}")
    print(
        f"  Files:  {before.input_file_count:>6}  -->  {after.input_file_count:>6}  ({_pct_change(before.input_file_count, after.input_file_count)} files)"
    )
    print(
        f"  Size:   {before.total_data_size_mb:>6.2f} MB  -->  {after.total_data_size_mb:>6.2f} MB"
    )
    print(f"{'=' * 90}")
    print(f"{'Query':<30} {'Before (s)':>12} {'After (s)':>12} {'Change':>10} {'Speedup':>10}")
    print(f"{'-' * 30} {'-' * 12} {'-' * 12} {'-' * 10} {'-' * 10}")
    for name in all_names:
        b = before_map.get(name)
        a = after_map.get(name)
        b_time = b.runtime_seconds if b else 0.0
        a_time = a.runtime_seconds if a else 0.0
        change = _pct_change(b_time, a_time)
        speedup = f"{b_time / a_time:.2f}x" if a_time > 0 and b_time > 0 else "n/a"
        print(f"{name:<30} {b_time:>12.4f} {a_time:>12.4f} {change:>10} {speedup:>10}")
    print(f"{'=' * 90}\n")


def generate_markdown_report(
    before: BenchmarkRun | None, after: BenchmarkRun | None, path: str
) -> None:
    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        "# Lakehouse Compaction Benchmark Report",
        "",
        f"Generated: {_utc_now()}",
        "",
        "## Why Fewer, Larger Files Matter",
        "",
        "Query engines pay a per-file cost (S3 metadata reads, Parquet footer parsing,",
        "task scheduling). Hundreds of small files shift time from columnar data reads",
        "to orchestration overhead. Compaction reduces file count, enlarges row groups,",
        "and improves compression — so the same SQL completes with less I/O setup.",
        "",
    ]
    if before is None and after is None:
        lines.append("_No benchmark runs recorded yet._")
        report_path.write_text("\n".join(lines), encoding="utf-8")
        return
    if before:
        lines.extend(
            [
                "## Before Compaction",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Benchmark ID | `{before.benchmark_id}` |",
                f"| Timestamp | {before.timestamp} |",
                f"| Engine | {before.engine} |",
                f"| Parquet files | {before.input_file_count} |",
                f"| Data size | {before.total_data_size_mb:.2f} MB |",
                "",
                "| Query | Runtime (s) | Rows |",
                "|-------|-------------|------|",
            ]
        )
        for q in before.queries:
            lines.append(f"| {q.query_name} | {q.runtime_seconds:.4f} | {q.row_count} |")
        lines.append("")
    if after:
        lines.extend(
            [
                "## After Compaction",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Benchmark ID | `{after.benchmark_id}` |",
                f"| Timestamp | {after.timestamp} |",
                f"| Engine | {after.engine} |",
                f"| Parquet files | {after.input_file_count} |",
                f"| Data size | {after.total_data_size_mb:.2f} MB |",
                "",
                "| Query | Runtime (s) | Rows |",
                "|-------|-------------|------|",
            ]
        )
        for q in after.queries:
            lines.append(f"| {q.query_name} | {q.runtime_seconds:.4f} | {q.row_count} |")
        lines.append("")
    if before and after:
        before_map = {q.query_name: q for q in before.queries}
        after_map = {q.query_name: q for q in after.queries}
        all_names = sorted(set(before_map) | set(after_map))
        lines.extend(
            [
                "## Before vs After Comparison",
                "",
                f"- **Parquet files:** {before.input_file_count} → {after.input_file_count} ({_pct_change(before.input_file_count, after.input_file_count)})",
                f"- **Data size:** {before.total_data_size_mb:.2f} MB → {after.total_data_size_mb:.2f} MB",
                "",
                "| Query | Before (s) | After (s) | Change | Speedup |",
                "|-------|------------|-----------|--------|---------|",
            ]
        )
        for name in all_names:
            b = before_map.get(name)
            a = after_map.get(name)
            b_time = b.runtime_seconds if b else 0.0
            a_time = a.runtime_seconds if a else 0.0
            speedup = f"{b_time / a_time:.2f}x" if a_time > 0 and b_time > 0 else "n/a"
            lines.append(
                f"| {name} | {b_time:.4f} | {a_time:.4f} | {_pct_change(b_time, a_time)} | {speedup} |"
            )
        lines.append("")
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark Delta Lake query performance before/after compaction"
    )
    parser.add_argument(
        "--phase",
        choices=["before", "after", "auto"],
        default="auto",
        help="Label this run (auto detects based on file count vs history)",
    )
    parser.add_argument(
        "--engine",
        choices=["duckdb", "pyspark"],
        default=None,
        help="Query engine (default: duckdb, or BENCHMARK_USE_DUCKDB env)",
    )
    args = parser.parse_args()
    setup_logging("benchmark")
    config = CompactionConfig()
    use_duckdb = os.getenv("BENCHMARK_USE_DUCKDB", "true").lower() == "true"
    engine: Engine = args.engine or ("duckdb" if use_duckdb else "pyspark")
    history = load_benchmark_runs(BENCHMARK_RESULTS_PATH)
    run = execute_benchmark(config, engine)
    run.phase = resolve_phase(run, args.phase, history)
    save_benchmark_run(run, BENCHMARK_RESULTS_PATH)
    history.append(run)
    before_run, after_run = find_comparison_pair(history, run)
    if before_run and after_run:
        print_comparison_table(before_run, after_run)
    else:
        print_single_run_table(run)
        if run.phase == "before":
            print("Tip: Run `make compact-once`, then `make benchmark --phase after`")
            print("     (or `make benchmark` with auto phase) to generate a comparison.\n")
    generate_markdown_report(before_run, after_run, BENCHMARK_REPORT_PATH)
    print(f"Results saved:  {BENCHMARK_RESULTS_PATH}")
    print(f"Report saved:   {BENCHMARK_REPORT_PATH}")


if __name__ == "__main__":
    main()
