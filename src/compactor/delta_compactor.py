from __future__ import annotations

import logging
import time

from pyspark.sql import SparkSession

from src.compactor.config import CompactionConfig
from src.compactor.exceptions import EmptyPartitionError, SparkCompactionError
from src.compactor.file_scanner import PartitionStats
from src.compactor.metrics import PartitionCompactionMetrics, format_bytes
from src.compactor.target_sizing import estimate_output_file_count
from src.utils.logging_config import log_event

logger = logging.getLogger(__name__)
_DELTA_JARS = "/opt/spark/jars-extra/delta-spark_2.12-3.1.0.jar,/opt/spark/jars-extra/delta-storage-3.1.0.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar"
CLUSTER_COLUMNS = ("user_id", "account_id", "event_timestamp")


def create_spark_session(config: CompactionConfig) -> SparkSession:
    try:
        builder = (
            SparkSession.builder.appName(config.spark_app_name)
            .master(config.spark_master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", config.minio_root_user)
            .config("spark.hadoop.fs.s3a.secret.key", config.minio_root_password)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
        )
        import os

        if os.path.exists("/opt/spark/jars-extra/delta-spark_2.12-3.1.0.jar"):
            builder = builder.config("spark.jars", _DELTA_JARS)
        spark = builder.getOrCreate()
        log_event(
            logger,
            "spark_session_created",
            spark_master=config.spark_master,
            app_name=config.spark_app_name,
        )
        return spark
    except Exception as exc:
        log_event(
            logger,
            "spark_session_failed",
            level=logging.ERROR,
            message=str(exc),
            spark_master=config.spark_master,
        )
        raise SparkCompactionError("session_init", exc) from exc


def _replace_where_clause(stats: PartitionStats) -> str:
    return f"event_date = '{stats.event_date}' AND event_hour = {stats.event_hour}"


def _validate_partition_has_data(stats: PartitionStats) -> None:
    if stats.file_count == 0 or stats.total_bytes == 0:
        raise EmptyPartitionError(f"Partition {stats.partition_id} has no data files to compact")


def compact_with_delta_optimize(
    spark: SparkSession, config: CompactionConfig, stats: PartitionStats
) -> PartitionCompactionMetrics:
    started = time.perf_counter()
    partition_predicate = _replace_where_clause(stats)
    spark.sql(
        f"\n        OPTIMIZE delta.`{config.delta_table_path}`\n        WHERE {partition_predicate}\n        ZORDER BY ({', '.join(CLUSTER_COLUMNS)})\n        "
    )
    duration = time.perf_counter() - started
    after_count, after_bytes = _count_partition_files(config, stats)
    return PartitionCompactionMetrics(
        partition_id=stats.partition_id,
        files_before=stats.file_count,
        files_after=after_count,
        bytes_before=stats.total_bytes,
        bytes_after=after_bytes,
        duration_seconds=round(duration, 3),
        method="delta_optimize",
    )


def compact_with_zorder_inspired_clustering(
    spark: SparkSession, config: CompactionConfig, stats: PartitionStats
) -> PartitionCompactionMetrics:
    started = time.perf_counter()
    replace_where = _replace_where_clause(stats)
    num_output_files = estimate_output_file_count(stats.total_bytes, config.target_file_size_bytes)
    log_event(
        logger,
        "target_file_sizing",
        partition_id=stats.partition_id,
        total_bytes=stats.total_bytes,
        total_bytes_human=format_bytes(stats.total_bytes),
        target_file_size_bytes=config.target_file_size_bytes,
        num_output_files=num_output_files,
    )
    df = spark.read.format("delta").load(config.delta_table_path).where(replace_where)
    if df.rdd.isEmpty():
        raise EmptyPartitionError(
            f"Partition {stats.partition_id} returned no rows from Delta read"
        )
    clustered = df.repartition(num_output_files).sortWithinPartitions(*CLUSTER_COLUMNS)
    clustered.write.format("delta").mode("overwrite").option("replaceWhere", replace_where).save(
        config.delta_table_path
    )
    duration = time.perf_counter() - started
    after_count, after_bytes = _count_partition_files(config, stats)
    return PartitionCompactionMetrics(
        partition_id=stats.partition_id,
        files_before=stats.file_count,
        files_after=after_count,
        bytes_before=stats.total_bytes,
        bytes_after=after_bytes,
        duration_seconds=round(duration, 3),
        method="zorder_inspired",
    )


def _count_partition_files(config: CompactionConfig, stats: PartitionStats) -> tuple[int, int]:
    from src.compactor.file_scanner import create_s3_client, scan_partitions

    client = create_s3_client(config)
    partitions = scan_partitions(config, client)
    for p in partitions:
        if p.partition_id == stats.partition_id:
            return (p.file_count, p.total_bytes)
    return (0, 0)


def compact_partition(
    spark: SparkSession, config: CompactionConfig, stats: PartitionStats
) -> PartitionCompactionMetrics:
    _validate_partition_has_data(stats)
    num_output_files = estimate_output_file_count(stats.total_bytes, config.target_file_size_bytes)
    log_event(
        logger,
        "partition_compaction_started",
        message=f"Compacting {stats.partition_id}",
        partition_id=stats.partition_id,
        files_before=stats.file_count,
        bytes_before=stats.total_bytes,
        bytes_before_human=format_bytes(stats.total_bytes),
        planned_output_files=num_output_files,
        target_file_size_bytes=config.target_file_size_bytes,
    )
    try:
        if config.use_delta_optimize:
            try:
                result = compact_with_delta_optimize(spark, config, stats)
            except Exception as exc:
                log_event(
                    logger,
                    "delta_optimize_fallback",
                    level=logging.WARNING,
                    partition_id=stats.partition_id,
                    error=str(exc),
                )
                result = compact_with_zorder_inspired_clustering(spark, config, stats)
        else:
            result = compact_with_zorder_inspired_clustering(spark, config, stats)
        log_event(
            logger,
            "partition_compaction_complete",
            message=f"Compacted {stats.partition_id}",
            partition_id=stats.partition_id,
            files_before=result.files_before,
            files_after=result.files_after,
            bytes_before_human=format_bytes(result.bytes_before),
            bytes_after_human=format_bytes(result.bytes_after),
            duration_seconds=result.duration_seconds,
            method=result.method,
        )
        return result
    except EmptyPartitionError:
        raise
    except Exception as exc:
        log_event(
            logger,
            "spark_compaction_failed",
            level=logging.ERROR,
            partition_id=stats.partition_id,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        raise SparkCompactionError(stats.partition_id, exc) from exc
