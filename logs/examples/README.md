# Example outputs

Committed sample artifacts for portfolio review — no local run required.

| File | Description |
|------|-------------|
| [`compaction_metrics.jsonl`](compaction_metrics.jsonl) | One compaction run summary (files/bytes before & after) |
| [`benchmark_report.md`](benchmark_report.md) | Before/after DuckDB benchmark comparison |
| [`sample_events.json`](sample_events.json) | Valid Kafka payloads; last object is a quarantine candidate (missing `event_id`) |

Live runs write to `logs/*.jsonl` and `logs/benchmark_report.md` (gitignored).
