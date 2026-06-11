from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from src.compactor.config import CompactionConfig
from src.compactor.lock_manager import CompactionLock, LockManager, lock_file_path


@pytest.fixture
def local_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CompactionConfig:
    lock_dir = str(tmp_path / "locks" / "compaction")
    monkeypatch.setenv("LOCK_BASE_PATH", lock_dir)
    monkeypatch.setenv("LOCK_TIMEOUT_SECONDS", "300")
    return CompactionConfig(_env_file=None)


PARTITION = "event_date=2024-01-01/event_hour=10"


class TestLockFilePath:
    def test_canonical_lock_path(self, local_config: CompactionConfig):
        path = lock_file_path(local_config.lock_base_path, PARTITION)
        assert path.endswith("event_hour=10.lock")
        assert "event_date=2024-01-01" in path


class TestAcquireLock:
    def test_acquire_and_release(self, local_config: CompactionConfig):
        mgr = LockManager(local_config, owner_id="worker-1", run_id="run-abc", use_local=True)
        assert mgr.acquire(PARTITION, run_id="run-abc") is True
        assert mgr.is_held_by_self(PARTITION)
        mgr.release(PARTITION)
        assert not mgr._local_path(PARTITION).exists()

    def test_reject_duplicate_lock(self, local_config: CompactionConfig):
        mgr_a = LockManager(local_config, owner_id="worker-a", run_id="run-a", use_local=True)
        mgr_b = LockManager(local_config, owner_id="worker-b", run_id="run-b", use_local=True)
        assert mgr_a.acquire(PARTITION) is True
        assert mgr_b.acquire(PARTITION) is False
        mgr_a.release(PARTITION)
        assert mgr_b.acquire(PARTITION) is True

    def test_lock_contains_required_fields(self, local_config: CompactionConfig):
        mgr = LockManager(local_config, owner_id="worker-meta", run_id="run-meta", use_local=True)
        mgr.acquire(PARTITION, run_id="run-meta")
        raw = mgr._local_path(PARTITION).read_text(encoding="utf-8")
        lock = CompactionLock.from_json(raw)
        assert lock.partition_id == PARTITION
        assert lock.owner_id == "worker-meta"
        assert lock.run_id == "run-meta"
        assert lock.acquired_at.endswith("Z")
        assert lock.expires_at.endswith("Z")
        assert lock.expires_datetime > lock.acquired_datetime


class TestStaleLockCleanup:
    def test_cleanup_stale_lock_by_expires_at(self, local_config: CompactionConfig):
        mgr = LockManager(local_config, owner_id="worker-stale", use_local=True)
        now = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)
        acquired = now - timedelta(seconds=600)
        expired = now - timedelta(seconds=60)
        lock = CompactionLock(
            partition_id=PARTITION,
            owner_id="crashed-worker",
            acquired_at=acquired.isoformat().replace("+00:00", "Z"),
            expires_at=expired.isoformat().replace("+00:00", "Z"),
            run_id="run-crashed",
        )
        path = mgr._local_path(PARTITION)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(lock.to_json())
        assert mgr.is_stale(lock, now=now) is True
        assert mgr.cleanup_stale_lock(PARTITION, now=now) is True
        assert mgr.acquire(PARTITION, now=now) is True

    def test_active_lock_not_cleaned(self, local_config: CompactionConfig):
        mgr = LockManager(local_config, owner_id="worker-active", use_local=True)
        now = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)
        acquired = now - timedelta(seconds=30)
        expires = now + timedelta(seconds=270)
        lock = CompactionLock(
            partition_id=PARTITION,
            owner_id="other-worker",
            acquired_at=acquired.isoformat().replace("+00:00", "Z"),
            expires_at=expires.isoformat().replace("+00:00", "Z"),
            run_id="run-active",
        )
        mgr._local_path(PARTITION).write_text(lock.to_json())
        assert mgr.cleanup_stale_lock(PARTITION, now=now) is False


class TestFailureRecovery:
    def test_release_lock_after_compaction_failure(self, local_config: CompactionConfig):
        mgr = LockManager(local_config, owner_id="compactor-1", run_id="run-fail", use_local=True)
        partition = PARTITION
        assert mgr.acquire(partition, run_id="run-fail") is True
        lock_held = True
        try:
            raise RuntimeError("Simulated Spark compaction failure")
        except RuntimeError:
            pass
        finally:
            mgr.release(partition)
            lock_held = mgr.is_held_by_self(partition)
        assert lock_held is False
        assert not mgr._local_path(partition).exists()
        mgr_b = LockManager(local_config, owner_id="worker-b", run_id="run-b", use_local=True)
        assert mgr_b.acquire(partition) is True

    def test_failure_on_one_partition_does_not_hold_lock(self, local_config: CompactionConfig):
        mgr = LockManager(local_config, owner_id="w1", run_id="run-1", use_local=True)
        p1 = "event_date=2024-01-01/event_hour=10"
        p2 = "event_date=2024-01-01/event_hour=11"
        mgr.acquire(p1)
        try:
            raise ValueError("compaction failed")
        except ValueError:
            pass
        finally:
            mgr.release(p1)
        assert mgr.acquire(p2) is True
