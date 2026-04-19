from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from pathlib import Path

from config.config import Settings, settings
from db.db_connection import get_connection


RowTuple = tuple[str, str, str, str]

BASE_RECORDS = (
    ("Aarav Sharma", "aarav.sharma", "9812300001"),
    ("Ananya Patel", "ananya.patel", "9812300002"),
    ("Diya Mehta", "diya.mehta", "9812300003"),
    ("Ishaan Gupta", "ishaan.gupta", "9812300004"),
    ("Kabir Das", "kabir.das", "9812300005"),
    ("Kiara Kapoor", "kiara.kapoor", "9812300006"),
    ("Meera Nair", "meera.nair", "9812300007"),
    ("Neha Verma", "neha.verma", "9812300008"),
    ("Priya Bhatia", "priya.bhatia", "9812300009"),
    ("Rohan Agarwal", "rohan.agarwal", "9812300010"),
    ("Saanvi Reddy", "saanvi.reddy", "9812300011"),
    ("Vivaan Chopra", "vivaan.chopra", "9812300012"),
)


@dataclass
class SourceDatasetBuilder:
    settings: Settings

    def build(self, dataset_size: int, force_rebuild: bool = False) -> Path:
        database_path = self.settings.source_db_path(dataset_size)
        if database_path.exists() and not force_rebuild and self._row_count_matches(database_path, dataset_size):
            return database_path

        with get_connection(database_path) as connection:
            connection.execute(f"DROP TABLE IF EXISTS {self.settings.source_table}")
            connection.execute(
                f"""
                CREATE TABLE {self.settings.source_table} (
                    name TEXT NOT NULL,
                    roll_no TEXT NOT NULL,
                    email TEXT NOT NULL,
                    phone_number TEXT NOT NULL
                )
                """
            )

            randomizer = random.Random(self.settings.random_seed + dataset_size)
            batch: list[RowTuple] = []

            for index in range(dataset_size):
                batch.append(self._make_row(index=index, randomizer=randomizer))
                if len(batch) >= self.settings.source_insert_batch_size:
                    connection.executemany(
                        f"""
                        INSERT INTO {self.settings.source_table} (name, roll_no, email, phone_number)
                        VALUES (?, ?, ?, ?)
                        """,
                        batch,
                    )
                    batch = []

            if batch:
                connection.executemany(
                    f"""
                    INSERT INTO {self.settings.source_table} (name, roll_no, email, phone_number)
                    VALUES (?, ?, ?, ?)
                    """,
                    batch,
                )

            connection.commit()

        return database_path

    def _row_count_matches(self, database_path: Path, dataset_size: int) -> bool:
        try:
            with get_connection(database_path) as connection:
                value = connection.execute(f"SELECT COUNT(*) FROM {self.settings.source_table}").fetchone()[0]
                return int(value) == dataset_size
        except Exception:
            return False

    def _make_row(self, index: int, randomizer: random.Random) -> RowTuple:
        base_name, base_email_prefix, base_phone = BASE_RECORDS[index % len(BASE_RECORDS)]
        suffix = index // len(BASE_RECORDS)
        shard = randomizer.randint(100, 999)
        name = base_name
        roll_no = f"{suffix:04d}{index + 1:07d}"
        email = f"{base_email_prefix}{suffix:04d}{shard}@college.edu"
        phone_number = f"{int(base_phone) + (index % 997):010d}"
        return (name, roll_no, email, phone_number)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a benchmark source SQLite dataset.")
    parser.add_argument("--records", type=int, required=True, help="Dataset size to generate.")
    parser.add_argument("--force-rebuild", action="store_true", help="Rebuild the source DB even if it already exists.")
    return parser


def main() -> None:
    arguments = build_parser().parse_args()
    path = SourceDatasetBuilder(settings=settings).build(arguments.records, force_rebuild=arguments.force_rebuild)
    print(f"Source database ready: {path}")


if __name__ == "__main__":
    main()
