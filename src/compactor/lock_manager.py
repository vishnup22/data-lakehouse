from __future__ import annotations

import json
import logging
import os
import socket
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from src.compactor.config import CompactionConfig
from src.utils.logging_config import log_event

logger = logging.getLogger(__name__)


@dataclass
class CompactionLock:
    partition_id: str
    owner_id: str
    acquired_at: str
    expires_at: str
    run_id: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, raw: str) -> CompactionLock:
        data = json.loads(raw)
        acquired_at = data["acquired_at"]
        expires_at = data.get("expires_at")
        if not expires_at:
            acquired = datetime.fromisoformat(acquired_at.replace("Z", "+00:00"))
            expires_at = (acquired + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        return cls(
            partition_id=data["partition_id"],
            owner_id=data["owner_id"],
            acquired_at=acquired_at,
            expires_at=expires_at,
            run_id=data.get("run_id", "unknown"),
        )

    @property
    def acquired_datetime(self) -> datetime:
        return _parse_iso(self.acquired_at)

    @property
    def expires_datetime(self) -> datetime:
        return _parse_iso(self.expires_at)


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _default_owner_id() -> str:
    hostname = socket.gethostname()
    return f"{hostname}-{uuid.uuid4().hex[:8]}"


def lock_file_path(base_path: str, partition_id: str) -> str:
    return f"{base_path.rstrip('/')}/{partition_id}.lock"


def _utc_now_iso(now: datetime | None = None) -> str:
    dt = now or datetime.now(UTC)
    return dt.isoformat().replace("+00:00", "Z")


class LockManager:
    def __init__(
        self,
        config: CompactionConfig,
        owner_id: str | None = None,
        run_id: str | None = None,
        s3_client: BaseClient | None = None,
        use_local: bool = False,
    ) -> None:
        self.config = config
        self.owner_id = owner_id or _default_owner_id()
        self.run_id = run_id or "unknown"
        self.use_local = use_local
        self._s3: BaseClient | None = s3_client
        if not use_local and s3_client is None:
            self._s3 = boto3.client(
                "s3",
                endpoint_url=config.minio_endpoint,
                aws_access_key_id=config.minio_root_user,
                aws_secret_access_key=config.minio_root_password,
                region_name="us-east-1",
            )

    def storage_path(self, partition_id: str) -> str:
        return lock_file_path(self.config.lock_base_path, partition_id)

    def _local_path(self, partition_id: str) -> Path:
        base = Path(self.config.lock_base_path)
        current = base
        for segment in partition_id.split("/"):
            current = current / segment
        lock_path = Path(f"{current}.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        return lock_path

    def _s3_lock_key(self, partition_id: str) -> str:
        return lock_file_path(self.config.lock_base_path, partition_id)

    def _read_lock(self, partition_id: str) -> CompactionLock | None:
        if self.use_local:
            path = self._local_path(partition_id)
            if not path.exists():
                return None
            return CompactionLock.from_json(path.read_text(encoding="utf-8"))
        assert self._s3 is not None
        key = self._s3_lock_key(partition_id)
        try:
            resp = self._s3.get_object(Bucket=self.config.minio_bucket, Key=key)
            return CompactionLock.from_json(resp["Body"].read().decode("utf-8"))
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return None
            raise

    def _write_lock(self, lock: CompactionLock) -> None:
        if self.use_local:
            self._local_path(lock.partition_id).write_text(lock.to_json(), encoding="utf-8")
            return
        assert self._s3 is not None
        self._s3.put_object(
            Bucket=self.config.minio_bucket,
            Key=self._s3_lock_key(lock.partition_id),
            Body=lock.to_json().encode("utf-8"),
            ContentType="application/json",
        )

    def _delete_lock(self, partition_id: str) -> None:
        if self.use_local:
            path = self._local_path(partition_id)
            if path.exists():
                os.remove(path)
            return
        assert self._s3 is not None
        try:
            self._s3.delete_object(
                Bucket=self.config.minio_bucket, Key=self._s3_lock_key(partition_id)
            )
        except ClientError:
            pass

    def is_stale(self, lock: CompactionLock, now: datetime | None = None) -> bool:
        reference = now or datetime.now(UTC)
        if reference >= lock.expires_datetime:
            return True
        age_seconds = (reference - lock.acquired_datetime).total_seconds()
        return age_seconds > self.config.lock_timeout_seconds

    def cleanup_stale_lock(self, partition_id: str, now: datetime | None = None) -> bool:
        existing = self._read_lock(partition_id)
        if existing and self.is_stale(existing, now=now):
            reference = now or datetime.now(UTC)
            log_event(
                logger,
                "stale_lock_cleaned",
                level=logging.WARNING,
                message=f"Removed stale lock on {partition_id}",
                partition_id=partition_id,
                lock_path=self.storage_path(partition_id),
                previous_owner=existing.owner_id,
                previous_run_id=existing.run_id,
                acquired_at=existing.acquired_at,
                expires_at=existing.expires_at,
                lock_age_seconds=round((reference - existing.acquired_datetime).total_seconds(), 1),
                lock_timeout_seconds=self.config.lock_timeout_seconds,
            )
            self._delete_lock(partition_id)
            return True
        return False

    def acquire(
        self, partition_id: str, now: datetime | None = None, run_id: str | None = None
    ) -> bool:
        effective_run_id = run_id or self.run_id
        reference = now or datetime.now(UTC)
        if self.cleanup_stale_lock(partition_id, now=reference):
            log_event(
                logger,
                "lock_reacquired_after_stale_cleanup",
                partition_id=partition_id,
                owner_id=self.owner_id,
                run_id=effective_run_id,
            )
        existing = self._read_lock(partition_id)
        if existing and (not self.is_stale(existing, now=reference)):
            if existing.owner_id != self.owner_id:
                log_event(
                    logger,
                    "lock_acquisition_failed",
                    level=logging.WARNING,
                    message=f"Lock held by another worker on {partition_id}",
                    partition_id=partition_id,
                    lock_path=self.storage_path(partition_id),
                    holder=existing.owner_id,
                    holder_run_id=existing.run_id,
                    requester=self.owner_id,
                    requester_run_id=effective_run_id,
                    expires_at=existing.expires_at,
                )
                return False
        expires = reference + timedelta(seconds=self.config.lock_timeout_seconds)
        lock = CompactionLock(
            partition_id=partition_id,
            owner_id=self.owner_id,
            acquired_at=_utc_now_iso(reference),
            expires_at=_utc_now_iso(expires),
            run_id=effective_run_id,
        )
        self._write_lock(lock)
        log_event(
            logger,
            "lock_acquired",
            partition_id=partition_id,
            lock_path=self.storage_path(partition_id),
            owner_id=self.owner_id,
            run_id=effective_run_id,
            acquired_at=lock.acquired_at,
            expires_at=lock.expires_at,
        )
        return True

    def release(self, partition_id: str) -> None:
        existing = self._read_lock(partition_id)
        if existing and existing.owner_id == self.owner_id:
            self._delete_lock(partition_id)
            log_event(
                logger,
                "lock_released",
                partition_id=partition_id,
                lock_path=self.storage_path(partition_id),
                owner_id=self.owner_id,
                run_id=existing.run_id,
            )

    def is_held_by_self(self, partition_id: str) -> bool:
        existing = self._read_lock(partition_id)
        return existing is not None and existing.owner_id == self.owner_id
