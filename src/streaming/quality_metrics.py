from __future__ import annotations

import json
from pathlib import Path

from src.streaming.data_quality import QualityMetrics


def write_quality_metrics(metrics: QualityMetrics, path: str) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(metrics.to_dict(), default=str) + "\n")
