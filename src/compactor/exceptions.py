from __future__ import annotations


class CompactionError(Exception):
    pass


class TableNotFoundError(CompactionError):
    pass


class EmptyPartitionError(CompactionError):
    pass


class LockAcquisitionError(CompactionError):
    def __init__(self, partition_id: str, holder: str | None = None) -> None:
        self.partition_id = partition_id
        self.holder = holder
        msg = f"Lock held on partition {partition_id}"
        if holder:
            msg += f" by {holder}"
        super().__init__(msg)


class SparkCompactionError(CompactionError):
    def __init__(self, partition_id: str, cause: Exception) -> None:
        self.partition_id = partition_id
        self.cause = cause
        super().__init__(f"Spark compaction failed for {partition_id}: {cause}")
