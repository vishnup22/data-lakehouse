# Lakehouse Compaction Benchmark Report

Generated: 2026-06-11T15:30:00Z

## Why Fewer, Larger Files Matter

Query engines pay a per-file cost (S3 metadata reads, Parquet footer parsing,
task scheduling). Hundreds of small files shift time from columnar data reads
to orchestration overhead. Compaction reduces file count, enlarges row groups,
and improves compression — so the same SQL completes with less I/O setup.

## Before Compaction

| Metric | Value |
|--------|-------|
| Benchmark ID | `f3a91bc204e1` |
| Timestamp | 2026-06-11T14:30:00Z |
| Engine | duckdb |
| Parquet files | 87 |
| Data size | 12.00 MB |

| Query | Runtime (s) | Rows |
|-------|-------------|------|
| count_by_event_type | 0.8420 | 7 |
| filter_by_region | 0.2310 | 142 |
| aggregate_by_region_date | 0.5190 | 35 |
| count_total_records | 0.1840 | 1 |
| parquet_file_inventory | 0.0430 | 87 |

## After Compaction

| Metric | Value |
|--------|-------|
| Benchmark ID | `c7e2d9a81b04` |
| Timestamp | 2026-06-11T15:30:00Z |
| Engine | duckdb |
| Parquet files | 4 |
| Data size | 11.34 MB |

| Query | Runtime (s) | Rows |
|-------|-------------|------|
| count_by_event_type | 0.1980 | 7 |
| filter_by_region | 0.0670 | 142 |
| aggregate_by_region_date | 0.1420 | 35 |
| count_total_records | 0.0510 | 1 |
| parquet_file_inventory | 0.0080 | 4 |

## Before vs After Comparison

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Parquet files | 87 | 4 | -95.4% |
| Data size | 12.00 MB | 11.34 MB | -5.5% |

| Query | Before (s) | After (s) | Change | Speedup |
|-------|------------|-----------|--------|---------|
| aggregate_by_region_date | 0.5190 | 0.1420 | -72.6% | 3.65x |
| count_by_event_type | 0.8420 | 0.1980 | -76.5% | 4.25x |
| count_total_records | 0.1840 | 0.0510 | -72.3% | 3.61x |
| filter_by_region | 0.2310 | 0.0670 | -71.0% | 3.45x |
| parquet_file_inventory | 0.0430 | 0.0080 | -81.4% | 5.38x |
