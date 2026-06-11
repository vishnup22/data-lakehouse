from __future__ import annotations

import math

DEFAULT_TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024


def estimate_output_file_count(
    total_bytes: int, target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES
) -> int:
    if total_bytes <= 0:
        return 1
    if target_file_size_bytes <= 0:
        raise ValueError("target_file_size_bytes must be positive")
    return max(1, math.ceil(total_bytes / target_file_size_bytes))
