from __future__ import annotations

import pytest
from src.compactor.target_sizing import DEFAULT_TARGET_FILE_SIZE_BYTES, estimate_output_file_count


class TestEstimateOutputFileCount:
    TARGET = 128 * 1024 * 1024

    def test_zero_bytes_returns_one(self):
        assert estimate_output_file_count(0, self.TARGET) == 1

    def test_small_partition_returns_one(self):
        assert estimate_output_file_count(64 * 1024 * 1024, self.TARGET) == 1

    def test_exact_target_returns_one(self):
        assert estimate_output_file_count(self.TARGET, self.TARGET) == 1

    def test_slightly_over_target_returns_two(self):
        assert estimate_output_file_count(self.TARGET + 1, self.TARGET) == 2

    def test_large_partition(self):
        total = 300 * 1024 * 1024
        assert estimate_output_file_count(total, self.TARGET) == 3

    def test_default_target_constant(self):
        assert DEFAULT_TARGET_FILE_SIZE_BYTES == 128 * 1024 * 1024

    def test_uses_configurable_target_size(self):
        custom_target = 64 * 1024 * 1024
        total = 200 * 1024 * 1024
        assert estimate_output_file_count(total, custom_target) == 4

    def test_rejects_invalid_target(self):
        with pytest.raises(ValueError):
            estimate_output_file_count(1000, 0)
