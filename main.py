from __future__ import annotations

import argparse
import csv
from pathlib import Path

from config.config import settings
from db.create_db import ensure_source_database
from db.db_connection import get_connection
from etl.case1_direct import run_case1_direct
from etl.case2_staged import run_case2_staged
from etl.case3_parallel import ParallelCaseConfig, run_case3_parallel
from results.plot_results import render_benchmark_plot, render_results_table
from utils.file_utils import ensure_directory, file_size_mb
from utils.runtime_cleanup import clear_runtime_artifacts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the ETL benchmarking system.")
    parser.add_argument("--sizes", nargs="*", type=int, default=list(settings.dataset_sizes), help="Dataset sizes to run.")
    parser.add_argument(
        "--chunk-sizes-mb",
        nargs="*",
        type=int,
        default=list(settings.chunk_sizes_mb),
        help="Chunk sizes for Case 3.",
    )
    parser.add_argument(
        "--cases",
        nargs="*",
        default=["case1", "case2", "case3"],
        choices=["case1", "case2", "case3"],
        help="Cases to execute.",
    )
    parser.add_argument("--keep-destinations", action="store_true", help="Keep generated destination DBs in runtime/artifacts.")
    parser.add_argument("--force-rebuild", action="store_true", help="Regenerate source datasets before benchmarking.")
    return parser


def count_rows(database_path: Path, table_name: str) -> int:
    with get_connection(database_path) as connection:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def validate_transform_sample(database_path: Path, table_name: str) -> tuple[str, str, str, str]:
    with get_connection(database_path) as connection:
        row = connection.execute(
            f"SELECT name, roll_no, email, phone_number FROM {table_name} ORDER BY rowid LIMIT 1"
        ).fetchone()
        return tuple(row)


def write_results_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    ensure_directory(output_path.parent)
    columns = [
        "DATASET",
        "RECORDS",
        "SOURCE_DB_MB",
        "CASE1_DIRECT_SEC",
        "CASE2_STAGED_SEC",
        "1_MB_SEC",
        "2_MB_SEC",
        "5_MB_SEC",
        "6_MB_SEC",
        "CASE3_OPTIMAL_SEC",
        "CASE3_OPTIMAL_CHUNK_MB",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def benchmark() -> list[dict[str, str]]:
    arguments = build_parser().parse_args()
    ensure_directory(settings.runtime_dataset_dir)
    ensure_directory(settings.runtime_artifact_dir)
    ensure_directory(settings.results_dir)
    clear_runtime_artifacts(settings.runtime_artifact_dir)

    results_rows: list[dict[str, str]] = []

    for dataset_size in arguments.sizes:
        source_path = ensure_source_database(dataset_size, force_rebuild=arguments.force_rebuild)
        result_row = {
            "DATASET": settings.label_for(dataset_size),
            "RECORDS": str(dataset_size),
            "SOURCE_DB_MB": f"{file_size_mb(source_path):.2f}",
            "CASE1_DIRECT_SEC": "",
            "CASE2_STAGED_SEC": "",
            "1_MB_SEC": "",
            "2_MB_SEC": "",
            "5_MB_SEC": "",
            "6_MB_SEC": "",
            "CASE3_OPTIMAL_SEC": "",
            "CASE3_OPTIMAL_CHUNK_MB": "",
        }

        source_count = count_rows(source_path, settings.source_table)
        if source_count != dataset_size:
            raise RuntimeError(f"Source dataset {source_path} expected {dataset_size} rows, found {source_count}.")

        if "case1" in arguments.cases:
            destination = settings.destination_db_path("case1_direct", dataset_size)
            elapsed = run_case1_direct(source_path, destination, settings.source_table, settings.destination_table)
            result_row["CASE1_DIRECT_SEC"] = f"{elapsed:.4f}"
            _assert_destination_valid(destination, dataset_size)

        if "case2" in arguments.cases:
            destination = settings.destination_db_path("case2_staged", dataset_size)
            elapsed = run_case2_staged(
                source_path,
                destination,
                settings.source_table,
                settings.destination_table,
                settings.stage_load_batch_size,
            )
            result_row["CASE2_STAGED_SEC"] = f"{elapsed:.4f}"
            _assert_destination_valid(destination, dataset_size)

        case3_results: list[tuple[int, float]] = []
        if "case3" in arguments.cases:
            for chunk_size_mb in arguments.chunk_sizes_mb:
                destination = settings.destination_db_path("case3_parallel", dataset_size, chunk_size_mb)
                elapsed = run_case3_parallel(
                    source_path,
                    destination,
                    settings.source_table,
                    settings.destination_table,
                    ParallelCaseConfig(
                        chunk_size_mb=chunk_size_mb,
                        extract_fetch_size=settings.extract_fetch_size,
                        queue_maxsize=settings.queue_maxsize,
                    ),
                )
                case3_results.append((chunk_size_mb, elapsed))
                key = f"{chunk_size_mb}_MB_SEC"
                if key in result_row:
                    result_row[key] = f"{elapsed:.4f}"
                _assert_destination_valid(destination, dataset_size)

        if case3_results:
            optimal_chunk, optimal_sec = min(case3_results, key=lambda item: item[1])
            result_row["CASE3_OPTIMAL_SEC"] = f"{optimal_sec:.4f}"
            result_row["CASE3_OPTIMAL_CHUNK_MB"] = str(optimal_chunk)

        results_rows.append(result_row)

    write_results_csv(results_rows, settings.results_dir / "results.csv")
    render_results_table(settings.results_dir / "results.csv", settings.results_dir / "results_table.png")
    render_benchmark_plot(settings.results_dir / "results.csv", settings.results_dir / "benchmark_plot.png")

    if not arguments.keep_destinations:
        clear_runtime_artifacts(settings.runtime_artifact_dir)

    return results_rows


def _assert_destination_valid(destination_path: Path, expected_count: int) -> None:
    destination_count = count_rows(destination_path, settings.destination_table)
    if destination_count != expected_count:
        raise RuntimeError(
            f"Destination dataset {destination_path} expected {expected_count} rows, found {destination_count}."
        )

    sample = validate_transform_sample(destination_path, settings.destination_table)
    if not sample[0].isupper() or not sample[1].startswith("RN_") or not sample[2].isupper() or not sample[3].startswith("+91"):
        raise RuntimeError(f"Transformation validation failed for {destination_path}.")


if __name__ == "__main__":
    rows = benchmark()
    print("Benchmark complete.")
    for row in rows:
        print(row)
    print(f"CSV: {settings.results_dir / 'results.csv'}")
    print(f"Table PNG: {settings.results_dir / 'results_table.png'}")
    print(f"Plot PNG: {settings.results_dir / 'benchmark_plot.png'}")
