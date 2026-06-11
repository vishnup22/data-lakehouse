from __future__ import annotations

from src.compactor.config import CompactionConfig


class TestTableKeyPrefix:
    def test_derives_prefix_from_s3a_path(self):
        cfg = CompactionConfig(
            _env_file=None,
            delta_table_path="s3a://lakehouse/raw/events",
        )
        assert cfg.table_key_prefix == "raw/events"

    def test_derives_prefix_without_bucket_in_path(self, monkeypatch):
        monkeypatch.setenv("RAW_TABLE_PATH", "s3a://lakehouse/custom/table")
        cfg = CompactionConfig(_env_file=None)
        assert cfg.table_key_prefix == "custom/table"


class TestConfigParsing:
    def test_defaults(self):
        config = CompactionConfig(_env_file=None)
        assert config.small_file_threshold_bytes == 32 * 1024 * 1024
        assert config.target_file_size_bytes == 128 * 1024 * 1024
        assert config.partition_age_skip_minutes == 30

    def test_partition_columns(self):
        config = CompactionConfig(_env_file=None)
        assert config.partition_columns() == ["event_date", "event_hour"]

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("SMALL_FILE_COUNT_THRESHOLD", "15")
        config = CompactionConfig(_env_file=None)
        assert config.small_file_count_threshold == 15

    def test_mb_threshold_aliases(self, monkeypatch):
        monkeypatch.setenv("SMALL_FILE_THRESHOLD_MB", "64")
        monkeypatch.setenv("TARGET_FILE_SIZE_MB", "256")
        config = CompactionConfig(_env_file=None)
        assert config.small_file_threshold_bytes == 64 * 1024 * 1024
        assert config.target_file_size_bytes == 256 * 1024 * 1024

    def test_s3_friendly_env_aliases(self, monkeypatch):
        monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testkey")
        monkeypatch.setenv("RAW_TABLE_PATH", "s3a://lakehouse/raw/events")
        monkeypatch.setenv("CHECKPOINT_PATH", "s3a://lakehouse/checkpoints/raw_events")
        config = CompactionConfig(_env_file=None)
        assert config.minio_endpoint == "http://minio:9000"
        assert config.minio_root_user == "testkey"
        assert config.delta_table_path == "s3a://lakehouse/raw/events"
