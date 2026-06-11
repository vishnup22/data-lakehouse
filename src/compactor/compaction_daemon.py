from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
import uuid
from datetime import UTC, datetime

from src.compactor.config import CompactionConfig
from src.compactor.delta_compactor import compact_partition, create_spark_session
from src.compactor.exceptions import (
    CompactionError,
    EmptyPartitionError,
    SparkCompactionError,
    TableNotFoundError,
)
from src.compactor.file_scanner import PartitionStats, scan_partitions, verify_delta_table_exists
from src.compactor.lock_manager import LockManager
from src.compactor.metrics import (
    EXIT_FAILURE,
    EXIT_SUCCESS,
    CompactionRunMetrics,
    MetricsWriter,
    format_bytes,
)
from src.compactor.partition_selection import (
    PartitionSelectionPolicy,
    PartitionSelectionResult,
    select_partitions,
    selection_summary,
)
from src.compactor.target_sizing import estimate_output_file_count
from src.utils.logging_config import log_event, setup_logging

logger = logging.getLogger(__name__)
_running = True


def _handle_signal(signum: int, frame: object) -> None:
    global _running
    log_event(logger, "daemon_shutdown_signal", level=logging.WARNING, signal=signum)
    _running = False


def _find_partition(partitions: list[PartitionStats], partition_id: str) -> PartitionStats | None:
    for stats in partitions:
        if stats.partition_id == partition_id:
            return stats
    return None


def _finalize_run(
    metrics: CompactionRunMetrics,
    started: datetime,
    writer: MetricsWriter,
    *,
    write_metrics: bool = True,
) -> None:
    metrics.mark_complete(started)
    if write_metrics:
        writer.write_run_summary(metrics)
    log_event(
        logger,
        "compaction_run_complete",
        message=metrics.summary_message(),
        run_id=metrics.run_id,
        table_path=metrics.table_path,
        status=metrics.status,
        partitions_scanned=metrics.partitions_scanned,
        eligible_partitions=metrics.eligible_partitions,
        skipped_partitions=metrics.skipped_partitions,
        compacted_partitions=metrics.compacted_partitions,
        files_before=metrics.files_before,
        files_after=metrics.files_after,
        bytes_before=metrics.bytes_before,
        bytes_after=metrics.bytes_after,
        bytes_before_human=format_bytes(metrics.bytes_before),
        bytes_after_human=format_bytes(metrics.bytes_after),
        compaction_duration_seconds=metrics.compaction_duration_seconds,
        lock_conflicts=metrics.lock_conflicts,
        failed_partitions=metrics.failed_partitions,
    )


def _print_dry_run_table(rows: list[dict], *, target_partition: str | None = None) -> None:
    title = "COMPACTION DRY RUN"
    if target_partition:
        title += f" (partition={target_partition})"
    print(f"\n{'=' * 88}")
    print(f"  {title}")
    print(f"{'=' * 88}")
    if not rows:
        print("  No eligible partitions found.")
        print(f"{'=' * 88}\n")
        return
    print(
        f"{'Partition':<42} {'Files Now':>10} {'Est. After':>12} {'Size Now':>12} {'Skip Reason':>14}"
    )
    print(f"{'-' * 42} {'-' * 10} {'-' * 12} {'-' * 12} {'-' * 14}")
    for row in rows:
        print(
            f"{row['partition_id']:<42} {row['files_before']:>10} {row['files_after_est']:>12} {row['size_before']:>12} {row.get('skip_reason', ''):>14}"
        )
    print(f"{'-' * 88}")
    print(
        f"  Totals: {len(rows)} partition(s) | files {sum(r['files_before'] for r in rows)} -> {sum(r['files_after_est'] for r in rows)} (estimated) | no data rewritten (dry-run)"
    )
    print(f"{'=' * 88}\n")


def run_dry_run(
    config: CompactionConfig, *, target_partition: str | None = None
) -> CompactionRunMetrics:
    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(UTC)
    metrics = CompactionRunMetrics(
        run_id=run_id,
        table_path=config.delta_table_path,
        started_at=started.isoformat().replace("+00:00", "Z"),
    )
    metrics.status = "dry_run"
    log_event(
        logger,
        "dry_run_started",
        run_id=run_id,
        table_path=config.delta_table_path,
        target_partition=target_partition,
    )
    try:
        verify_delta_table_exists(config)
    except TableNotFoundError as exc:
        metrics.error = str(exc)
        metrics.status = "error"
        _finalize_run(metrics, started, MetricsWriter(config.metrics_path), write_metrics=False)
        return metrics
    policy = PartitionSelectionPolicy.from_config(config)
    all_partitions = scan_partitions(config)
    metrics.partitions_scanned = len(all_partitions)
    if target_partition:
        match = _find_partition(all_partitions, target_partition)
        if match is None:
            metrics.error = f"Partition not found: {target_partition}"
            metrics.status = "error"
            _finalize_run(metrics, started, MetricsWriter(config.metrics_path), write_metrics=False)
            return metrics
        eligible = [match]
        skipped_results: list[PartitionSelectionResult] = []
    else:
        eligible, skipped_results = select_partitions(all_partitions, policy)
    metrics.eligible_partitions = len(eligible)
    metrics.skipped_partitions = len(skipped_results)
    dry_rows: list[dict] = []
    for stats in eligible:
        est_after = estimate_output_file_count(stats.total_bytes, config.target_file_size_bytes)
        metrics.files_before += stats.file_count
        metrics.files_after += est_after
        metrics.bytes_before += stats.total_bytes
        metrics.bytes_after += stats.total_bytes
        dry_rows.append(
            {
                "partition_id": stats.partition_id,
                "files_before": stats.file_count,
                "files_after_est": est_after,
                "size_before": format_bytes(stats.total_bytes),
            }
        )
        metrics.partition_details.append(
            {
                "partition_id": stats.partition_id,
                "files_before": stats.file_count,
                "files_after_est": est_after,
                "bytes_before": stats.total_bytes,
                "dry_run": True,
            }
        )
    _print_dry_run_table(dry_rows, target_partition=target_partition)
    log_event(
        logger,
        "dry_run_complete",
        run_id=run_id,
        eligible_partitions=metrics.eligible_partitions,
        files_before=metrics.files_before,
        files_after_estimated=metrics.files_after,
        selection_policy=selection_summary(policy),
    )
    _finalize_run(metrics, started, MetricsWriter(config.metrics_path), write_metrics=False)
    return metrics


def run_compaction_cycle(
    config: CompactionConfig, *, target_partition: str | None = None
) -> CompactionRunMetrics:
    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(UTC)
    metrics = CompactionRunMetrics(
        run_id=run_id,
        table_path=config.delta_table_path,
        started_at=started.isoformat().replace("+00:00", "Z"),
    )
    writer = MetricsWriter(config.metrics_path)
    lock_manager = LockManager(config, run_id=run_id)
    spark = None
    log_event(
        logger,
        "compaction_run_started",
        message=f"Starting compaction run {run_id}",
        run_id=run_id,
        table_path=config.delta_table_path,
        target_partition=target_partition,
    )
    try:
        verify_delta_table_exists(config)
    except TableNotFoundError as exc:
        metrics.error = str(exc)
        log_event(
            logger,
            "table_not_found",
            level=logging.ERROR,
            run_id=run_id,
            table_path=config.delta_table_path,
            error=str(exc),
        )
        _finalize_run(metrics, started, writer)
        return metrics
    try:
        policy = PartitionSelectionPolicy.from_config(config)
        all_partitions = scan_partitions(config)
        metrics.partitions_scanned = len(all_partitions)
        if target_partition:
            match = _find_partition(all_partitions, target_partition)
            if match is None:
                metrics.error = f"Partition not found: {target_partition}"
                metrics.status = "error"
                _finalize_run(metrics, started, writer)
                return metrics
            eligible = [match]
            skipped_results: list[PartitionSelectionResult] = []
            log_event(
                logger, "partition_targeted_run", run_id=run_id, partition_id=target_partition
            )
        else:
            eligible, skipped_results = select_partitions(all_partitions, policy)
        metrics.eligible_partitions = len(eligible)
        metrics.skipped_partitions = len(skipped_results)
        skip_reason_counts: dict[str, int] = {}
        for result in skipped_results:
            reason = result.skip_reason.value if result.skip_reason else "unknown"
            skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1
        log_event(
            logger,
            "partition_scan_complete",
            message="Partition scan finished",
            run_id=run_id,
            partitions_scanned=metrics.partitions_scanned,
            eligible_partitions=metrics.eligible_partitions,
            skipped_partitions=metrics.skipped_partitions,
            skip_reason_counts=skip_reason_counts,
            selection_policy=selection_summary(policy),
        )
        if not eligible:
            log_event(
                logger,
                "no_eligible_partitions",
                run_id=run_id,
                partitions_scanned=metrics.partitions_scanned,
            )
            _finalize_run(metrics, started, writer)
            return metrics
        try:
            spark = create_spark_session(config)
        except SparkCompactionError as exc:
            metrics.error = str(exc)
            _finalize_run(metrics, started, writer)
            return metrics
        for stats in eligible:
            partition_id = stats.partition_id
            if stats.file_count == 0:
                log_event(
                    logger,
                    "empty_partition_skipped",
                    level=logging.WARNING,
                    run_id=run_id,
                    partition_id=partition_id,
                )
                metrics.skipped_partitions += 1
                metrics.eligible_partitions -= 1
                continue
            if not lock_manager.acquire(partition_id, run_id=run_id):
                metrics.lock_conflicts += 1
                metrics.failed_partitions.append(
                    {"partition_id": partition_id, "error": "lock_acquisition_failed"}
                )
                continue
            try:
                metrics.files_before += stats.file_count
                metrics.bytes_before += stats.total_bytes
                part_metrics = compact_partition(spark, config, stats)
                metrics.compacted_partitions += 1
                metrics.files_after += part_metrics.files_after
                metrics.bytes_after += part_metrics.bytes_after
                metrics.partition_details.append(part_metrics.to_dict())
            except EmptyPartitionError as exc:
                log_event(
                    logger,
                    "empty_partition_error",
                    level=logging.WARNING,
                    run_id=run_id,
                    partition_id=partition_id,
                    error=str(exc),
                )
                metrics.failed_partitions.append({"partition_id": partition_id, "error": str(exc)})
            except SparkCompactionError as exc:
                metrics.failed_partitions.append({"partition_id": partition_id, "error": str(exc)})
            except Exception as exc:
                log_event(
                    logger,
                    "partition_compaction_unexpected_error",
                    level=logging.ERROR,
                    run_id=run_id,
                    partition_id=partition_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                metrics.failed_partitions.append({"partition_id": partition_id, "error": str(exc)})
            finally:
                lock_manager.release(partition_id)
        _finalize_run(metrics, started, writer)
        return metrics
    except CompactionError as exc:
        metrics.error = str(exc)
        log_event(
            logger,
            "compaction_run_failed",
            level=logging.ERROR,
            run_id=run_id,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        _finalize_run(metrics, started, writer)
        return metrics
    except Exception as exc:
        metrics.error = str(exc)
        logger.exception("Compaction run %s failed", run_id)
        _finalize_run(metrics, started, writer)
        raise
    finally:
        if spark is not None:
            spark.stop()


def run_daemon(config: CompactionConfig) -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    log_event(
        logger,
        "daemon_started",
        message="Compaction daemon started",
        interval_seconds=config.compaction_interval_seconds,
        table_path=config.delta_table_path,
        metrics_path=config.metrics_path,
    )
    while _running:
        cycle_start = time.monotonic()
        try:
            metrics = run_compaction_cycle(config)
            code = metrics.exit_code()
            if code == EXIT_FAILURE:
                log_event(
                    logger,
                    "compaction_cycle_failed",
                    level=logging.ERROR,
                    exit_code=code,
                    run_id=metrics.run_id,
                )
        except Exception:
            logger.exception("Unhandled error in compaction cycle")
        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0, config.compaction_interval_seconds - elapsed)
        if _running and sleep_for > 0:
            log_event(logger, "daemon_sleeping", sleep_seconds=round(sleep_for, 1))
            time.sleep(sleep_for)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Orchestration-ready Delta Lake compaction daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "\nExamples:\n"
            "  %(prog)s --once\n"
            "  %(prog)s --daemon\n"
            "  %(prog)s --dry-run\n"
            "  %(prog)s --partition event_date=2026-06-11/event_hour=10 --once\n"
            "\nExit codes: 0 success, 1 failure, 2 no eligible partitions\n"
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once", action="store_true", help="Run one compaction cycle and exit (for Airflow/cron)"
    )
    mode.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuously on an interval (default for Docker service)",
    )
    mode.add_argument(
        "--dry-run", action="store_true", help="Scan and estimate compaction; do not rewrite data"
    )
    parser.add_argument(
        "--partition",
        metavar="PARTITION_ID",
        help="Target a single partition, e.g. event_date=2026-06-11/event_hour=10",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = CompactionConfig()
    setup_logging("compaction_daemon", level=config.log_level, json_format=config.log_json_format)
    if args.dry_run:
        metrics = run_dry_run(config, target_partition=args.partition)
        return metrics.exit_code()
    if args.once or args.partition:
        metrics = run_compaction_cycle(config, target_partition=args.partition)
        return metrics.exit_code()
    run_daemon(config)
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
