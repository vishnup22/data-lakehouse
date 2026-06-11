# Test suite

77 fast unit tests — no Docker, Kafka, MinIO, or Spark required.

## Run locally

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest                    # verbose (configured in pyproject.toml)
make ci                   # lint + mypy + pytest
```

## Modules

| File | Focus |
|------|-------|
| `test_file_scanner.py` | S3 partition scan, small-file thresholds |
| `test_partition_selection.py` | Compaction eligibility and age guard |
| `test_target_file_sizing.py` | `estimate_output_file_count()` |
| `test_lock_manager.py` | Distributed locks, stale cleanup |
| `test_compaction_rules.py` | `CompactionConfig` / env loading |
| `test_data_quality.py` | Streaming validation and quarantine |
| `test_compaction_cli.py` | Exit codes and dry-run metrics |
| `test_metrics.py` | Run metrics and byte formatting |
| `test_benchmark.py` | Benchmark report generation |

All tests are marked `unit` via `conftest.py`.
