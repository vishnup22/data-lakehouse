# Recruiter Overview

**One-line pitch:** A Docker-based data lakehouse that streams JSON events through Kafka into Delta Lake, then compacts small-file partitions in the background — with locks, metrics, data quality, and before/after benchmarks.

**Scope:** Local portfolio project on MinIO + Redpanda. Not a cloud production deployment.

---

## What problem does this solve?

Real-time pipelines often write **one Parquet file per micro-batch**. Over time that creates many tiny files per partition. Queries slow down and S3 metadata overhead grows.

This project shows you understand **table maintenance** alongside ingestion — a common gap in portfolio projects.

---

## What you built (30-second version)

| Piece | Technology | Purpose |
|-------|------------|---------|
| Event source | Python + Kafka | Synthetic JSON traffic |
| Ingestion | Spark Structured Streaming | Kafka → Delta with validation |
| Storage | Delta Lake on MinIO | ACID table format, S3-compatible |
| Maintenance | Python compaction daemon | Rewrites eligible partitions toward ~128 MB |
| Safety | Partition locks + age guard | Avoid duplicate work; skip hot partitions |
| Proof | DuckDB benchmark | File count + query latency before/after |

---

## Skills demonstrated

- **Streaming:** Kafka consumption, Spark checkpoints, micro-batch tuning
- **Lakehouse:** Delta ACID commits, partition overwrite, MVCC reader safety
- **Data engineering ops:** Compaction eligibility rules, advisory locking, stale lock recovery
- **Data quality:** Required-field validation, quarantine table, rejection metrics
- **Software engineering:** Pydantic config, structured JSON logging, pytest (75 tests), GitHub Actions CI
- **Production thinking:** Orchestrator exit codes, dry-run mode, failure scenarios documented

---

## Numbers to cite (sample run only)

From the committed example at [`logs/examples/benchmark_report.md`](../logs/examples/benchmark_report.md) — **not guaranteed on every machine**:

- **87 → 4 files** in one compacted partition
- **Several queries ~3–5× faster** on DuckDB in that sample
- **75 unit tests** without Docker
- **10-second** streaming trigger to surface the small-file problem quickly

Always say: *"Here is a sample run from my repo; I can walk through the demo live."*

---

## How to demo in 5 minutes (no full pipeline required)

1. Architecture diagram in [README](../README.md#architecture)
2. `make compact-dry-run` if stack is running, **or** show dry-run output in README
3. [`logs/examples/compaction_metrics.jsonl`](../logs/examples/compaction_metrics.jsonl)
4. [`logs/examples/benchmark_report.md`](../logs/examples/benchmark_report.md)
5. Mention Delta ACID + advisory locks

**Live demo (~20 min):** `make demo-env && make up` → produce → stream 10 min → stop → `make wait-eligible` → compact → benchmark. See README.

---

## Role fit

| Role | Why this project helps |
|------|------------------------|
| **Junior / mid data engineer** | Ingestion + maintenance in one repo |
| **Analytics engineer** | Delta, partitioning, query performance |
| **Platform / infra-adjacent DE** | Docker stack, CI, observability patterns |
| **Streaming-focused** | Kafka + Spark + checkpointed streaming |

---

## Further reading

- [Architecture](ARCHITECTURE.md)
- [Benchmark methodology](BENCHMARK_REPORT.md)
- [README](../README.md)
