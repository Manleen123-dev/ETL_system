from __future__ import annotations

from pathlib import Path

from config.config import settings
from data_generator.generate_data import SourceDatasetBuilder


def ensure_source_database(dataset_size: int, force_rebuild: bool = False) -> Path:
    builder = SourceDatasetBuilder(settings=settings)
    return builder.build(dataset_size=dataset_size, force_rebuild=force_rebuild)
