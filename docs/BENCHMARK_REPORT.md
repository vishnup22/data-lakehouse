# Benchmark Methodology

This document explains how the project measures compaction impact. A **sample report** from a representative local run is committed at [`logs/examples/benchmark_report.md`](../logs/examples/benchmark_report.md).

---

## Why benchmark?

Small-file problems are invisible in code review. Hiring managers respond to **measurable outcomes**: fewer files, lower latency, same row counts.

The benchmark runner (`src/benchmark/benchmark_queries.py`) answers:

1. How many Parquet files exist under the Delta table prefix?
2. How long do typical analytical queries take?
3. How much did compaction improve both?

---

## Workflow

```bash
make benchmark-before    # snapshot file count + query latency (pre-compaction)
make compact-once        # rewrite eligible partitions
make benchmark-after     # snapshot again
make benchmark           # print comparison table + write report
```

Outputs:

| Artifact | Path |
|----------|------|
| Per-run JSONL | `logs/benchmark_results.jsonl` (gitignored at runtime) |
| Markdown report | `logs/benchmark_report.md` (gitignored at runtime) |
| **Committed example** | [`logs/examples/benchmark_report.md`](../logs/examples/benchmark_report.md) |

---

## Queries exercised

| Query | What it stresses |
|-------|------------------|
| `count_by_event_type` | Full scan + aggregation |
| `filter_by_region` | Filter on `region` (always populated in synthetic data) |
| `aggregate_by_region_date` | Multi-column group-by |
| `count_total_records` | Metadata-light count |
| `parquet_file_inventory` | File enumeration (most sensitive to small files) |

Engine: **DuckDB** reading Parquet files directly from the table path (no Spark cluster required for benchmarking).

---

## Interpreting results

**File count drop** (e.g. 87 → 4) means fewer S3 LIST operations, fewer Parquet footers, and fewer Spark/DuckDB tasks.

**Query speedup** (often 3–5× locally) comes from:

- Larger row groups → better column pruning and compression
- Less per-file scheduling overhead
- Z-order inspired sort colocating `user_id`, `account_id`, `event_timestamp`

**Data size** may stay similar or shrink slightly — compaction optimizes layout, not necessarily total bytes.

---

## Sample comparison (from example run)

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Parquet files | 87 | 4 | −95.4% |
| Data size | 12.00 MB | 11.34 MB | −5.5% |
| `filter_by_region` | 0.231 s | 0.067 s | 3.45× faster (sample run) |

Full tables: [`logs/examples/benchmark_report.md`](../logs/examples/benchmark_report.md)

---

## Caveats

- Local MinIO + DuckDB — not a cloud cost model
- Synthetic data — cardinality differs from production
- Single-partition compaction demo — production runs compact many partitions per schedule

Use these numbers to explain **mechanism and magnitude**, not to claim universal production SLAs.
