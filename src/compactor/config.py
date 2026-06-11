from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CompactionConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True
    )
    minio_endpoint: str = Field(
        default="http://localhost:9000",
        validation_alias=AliasChoices("MINIO_ENDPOINT", "S3_ENDPOINT"),
    )
    minio_root_user: str = Field(
        default="minioadmin", validation_alias=AliasChoices("MINIO_ROOT_USER", "AWS_ACCESS_KEY_ID")
    )
    minio_root_password: str = Field(
        default="minioadmin",
        validation_alias=AliasChoices("MINIO_ROOT_PASSWORD", "AWS_SECRET_ACCESS_KEY"),
    )
    minio_bucket: str = Field(default="lakehouse", alias="MINIO_BUCKET")
    delta_table_path: str = Field(
        default="s3a://lakehouse/raw/events",
        validation_alias=AliasChoices("DELTA_TABLE_PATH", "RAW_TABLE_PATH"),
    )
    delta_checkpoint_path: str = Field(
        default="s3a://lakehouse/checkpoints/raw_events",
        validation_alias=AliasChoices("DELTA_CHECKPOINT_PATH", "CHECKPOINT_PATH"),
    )
    spark_master: str = Field(default="local[*]", alias="SPARK_MASTER")
    spark_app_name: str = Field(default="lakehouse-compaction", alias="SPARK_APP_NAME")
    compaction_interval_seconds: int = Field(default=3600, alias="COMPACTION_INTERVAL_SECONDS")
    small_file_threshold_bytes: int = Field(
        default=32 * 1024 * 1024, alias="SMALL_FILE_THRESHOLD_BYTES"
    )
    small_file_threshold_mb: float | None = Field(default=None, alias="SMALL_FILE_THRESHOLD_MB")
    small_file_count_threshold: int = Field(default=20, alias="SMALL_FILE_COUNT_THRESHOLD")
    partition_file_count_threshold: int = Field(default=100, alias="PARTITION_FILE_COUNT_THRESHOLD")
    target_file_size_bytes: int = Field(default=128 * 1024 * 1024, alias="TARGET_FILE_SIZE_BYTES")
    target_file_size_mb: float | None = Field(default=None, alias="TARGET_FILE_SIZE_MB")
    partition_age_skip_minutes: int = Field(default=30, alias="PARTITION_AGE_SKIP_MINUTES")
    lock_timeout_seconds: int = Field(default=3600, alias="LOCK_TIMEOUT_SECONDS")
    lock_base_path: str = Field(default="locks/compaction", alias="LOCK_BASE_PATH")
    use_delta_optimize: bool = Field(default=False, alias="USE_DELTA_OPTIMIZE")
    metrics_path: str = Field(default="logs/compaction_metrics.jsonl", alias="METRICS_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_json_format: bool = Field(default=True, alias="LOG_JSON_FORMAT")

    @model_validator(mode="before")
    @classmethod
    def apply_mb_env_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if data.get("SMALL_FILE_THRESHOLD_MB") and (not data.get("SMALL_FILE_THRESHOLD_BYTES")):
            mb = float(data["SMALL_FILE_THRESHOLD_MB"])
            data["SMALL_FILE_THRESHOLD_BYTES"] = int(mb * 1024 * 1024)
        if data.get("TARGET_FILE_SIZE_MB") and (not data.get("TARGET_FILE_SIZE_BYTES")):
            mb = float(data["TARGET_FILE_SIZE_MB"])
            data["TARGET_FILE_SIZE_BYTES"] = int(mb * 1024 * 1024)
        return data

    @property
    def s3_base_url(self) -> str:
        return f"s3://{self.minio_bucket}"

    @property
    def table_key_prefix(self) -> str:
        path = self.delta_table_path
        for scheme in ("s3a://", "s3://"):
            if path.startswith(scheme):
                path = path[len(scheme) :]
                break
        if "/" in path:
            _, key_prefix = path.split("/", 1)
        else:
            key_prefix = path
        return key_prefix.rstrip("/")

    def partition_columns(self) -> list[str]:
        return ["event_date", "event_hour"]
