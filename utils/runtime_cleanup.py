from __future__ import annotations

from pathlib import Path


def clear_runtime_artifacts(artifact_dir: Path) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    for sqlite_file in artifact_dir.glob("*.sqlite"):
        sqlite_file.unlink(missing_ok=True)
