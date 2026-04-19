from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    dataset_sizes: tuple[int, ...] = (300_000, 600_000, 900_000, 1_200_000, 1_500_000)
    dataset_labels: dict[int, str] = field(
        default_factory=lambda: {
            300_000: "3L",
            600_000: "6L",
            900_000: "9L",
            1_200_000: "12L",
            1_500_000: "15L",
        }
    )
    chunk_sizes_mb: tuple[int, ...] = (1, 2, 5, 6)
    random_seed: int = 20260419
    source_table: str = "source_students"
    destination_table: str = "destination_students"
    source_insert_batch_size: int = 5_000
    stage_load_batch_size: int = 10_000
    extract_fetch_size: int = 5_000
    queue_maxsize: int = 4
    runtime_dir: Path = field(default=PROJECT_ROOT / "runtime")
    runtime_dataset_dir: Path = field(default=PROJECT_ROOT / "runtime" / "datasets")
    runtime_artifact_dir: Path = field(default=PROJECT_ROOT / "runtime" / "artifacts")
    results_dir: Path = field(default=PROJECT_ROOT / "results")

    def label_for(self, dataset_size: int) -> str:
        return self.dataset_labels.get(dataset_size, f"{dataset_size:,}")

    def source_db_path(self, dataset_size: int) -> Path:
        return self.runtime_dataset_dir / f"source_{self.label_for(dataset_size)}.sqlite"

    def destination_db_path(self, case_name: str, dataset_size: int, chunk_size_mb: int | None = None) -> Path:
        chunk_suffix = "" if chunk_size_mb is None else f"_{chunk_size_mb}mb"
        return self.runtime_artifact_dir / f"{case_name}_{self.label_for(dataset_size)}{chunk_suffix}.sqlite"


settings = Settings()
