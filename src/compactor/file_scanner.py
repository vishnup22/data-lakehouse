from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import boto3
from botocore.client import BaseClient

from src.compactor.config import CompactionConfig
from src.compactor.exceptions import TableNotFoundError


@dataclass(frozen=True)
class ParquetFileInfo:
    key: str
    size_bytes: int
    last_modified: datetime


@dataclass
class PartitionStats:
    event_date: str
    event_hour: int
    files: list[ParquetFileInfo] = field(default_factory=list)

    @property
    def partition_id(self) -> str:
        return f"event_date={self.event_date}/event_hour={self.event_hour}"

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def total_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)

    def small_files(self, threshold_bytes: int) -> list[ParquetFileInfo]:
        return [f for f in self.files if is_small_file(f.size_bytes, threshold_bytes)]


def is_small_file(size_bytes: int, threshold_bytes: int) -> bool:
    return size_bytes < threshold_bytes


def is_partition_eligible(
    stats: PartitionStats,
    *,
    small_file_threshold_bytes: int,
    small_file_count_threshold: int,
    partition_file_count_threshold: int,
) -> bool:
    small_count = len(stats.small_files(small_file_threshold_bytes))
    if small_count > small_file_count_threshold:
        return True
    if stats.file_count > partition_file_count_threshold:
        return True
    return False


def partition_is_too_recent(
    stats: PartitionStats, *, skip_minutes: int, now: datetime | None = None
) -> bool:
    if not stats.files:
        return True
    reference = now or datetime.now(UTC)
    cutoff = reference - timedelta(minutes=skip_minutes)
    latest = max(f.last_modified for f in stats.files)
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=UTC)
    return latest > cutoff


def _partition_re(config: CompactionConfig) -> re.Pattern[str]:
    prefix = re.escape(config.table_key_prefix.rstrip("/"))
    return re.compile(rf"{prefix}/event_date=([^/]+)/event_hour=(\d+)/.*\.parquet$")


def _parse_partition_from_key(key: str, config: CompactionConfig) -> tuple[str, int] | None:
    match = _partition_re(config).search(key)
    if not match:
        return None
    return (match.group(1), int(match.group(2)))


def _delta_log_prefix(config: CompactionConfig) -> str:
    return f"{config.table_key_prefix}/_delta_log/"


def verify_delta_table_exists(
    config: CompactionConfig, s3_client: BaseClient | None = None
) -> None:
    client = s3_client or create_s3_client(config)
    prefix = _delta_log_prefix(config)
    response = client.list_objects_v2(Bucket=config.minio_bucket, Prefix=prefix, MaxKeys=1)
    if response.get("KeyCount", 0) == 0:
        raise TableNotFoundError(
            f"Delta table not found at {config.delta_table_path} (no objects under {prefix})"
        )


def create_s3_client(config: CompactionConfig) -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_root_user,
        aws_secret_access_key=config.minio_root_password,
        region_name="us-east-1",
    )


def scan_partitions(
    config: CompactionConfig, s3_client: BaseClient | None = None
) -> list[PartitionStats]:
    client = s3_client or create_s3_client(config)
    prefix = f"{config.table_key_prefix}/"
    partitions: dict[tuple[str, int], PartitionStats] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.minio_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "_delta_log" in key or not key.endswith(".parquet"):
                continue
            parsed = _parse_partition_from_key(key, config)
            if parsed is None:
                continue
            event_date, event_hour = parsed
            bucket_key = (event_date, event_hour)
            if bucket_key not in partitions:
                partitions[bucket_key] = PartitionStats(
                    event_date=event_date, event_hour=event_hour
                )
            modified = obj["LastModified"]
            partitions[bucket_key].files.append(
                ParquetFileInfo(key=key, size_bytes=obj["Size"], last_modified=modified)
            )
    return sorted(partitions.values(), key=lambda p: (p.event_date, p.event_hour))


def find_eligible_partitions(
    config: CompactionConfig, s3_client: BaseClient | None = None, now: datetime | None = None
) -> tuple[list[PartitionStats], list[PartitionStats]]:
    from src.compactor.partition_selection import PartitionSelectionPolicy, select_partitions

    all_partitions = scan_partitions(config, s3_client)
    policy = PartitionSelectionPolicy.from_config(config)
    eligible, skipped_results = select_partitions(all_partitions, policy, now=now)
    skipped = [r.stats for r in skipped_results]
    return (eligible, skipped)
