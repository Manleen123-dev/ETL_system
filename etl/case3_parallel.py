from __future__ import annotations

from dataclasses import dataclass
from queue import Queue
from threading import Thread

from db.db_connection import get_connection
from etl.transformations import transform_rows
from utils.chunk_utils import chunk_by_target_size
from utils.timer import measure_seconds


SENTINEL = object()


@dataclass
class ParallelCaseConfig:
    chunk_size_mb: int
    extract_fetch_size: int
    queue_maxsize: int


def run_case3_parallel(
    source_db_path,
    destination_db_path,
    source_table: str,
    destination_table: str,
    config: ParallelCaseConfig,
) -> float:
    with get_connection(destination_db_path) as destination_connection:
        destination_connection.execute(f"DROP TABLE IF EXISTS {destination_table}")
        destination_connection.execute(
            f"""
            CREATE TABLE {destination_table} (
                name TEXT NOT NULL,
                roll_no TEXT NOT NULL,
                email TEXT NOT NULL,
                phone_number TEXT NOT NULL
            )
            """
        )
        destination_connection.commit()

    extract_queue: Queue[object] = Queue(maxsize=config.queue_maxsize)
    transform_queue: Queue[object] = Queue(maxsize=config.queue_maxsize)

    def extract_worker() -> None:
        with get_connection(source_db_path) as source_connection:
            cursor = source_connection.execute(
                f"SELECT name, roll_no, email, phone_number FROM {source_table}"
            )
            while True:
                rows = cursor.fetchmany(config.extract_fetch_size)
                if not rows:
                    break
                for chunk in chunk_by_target_size([tuple(row) for row in rows], config.chunk_size_mb):
                    extract_queue.put(chunk)
        extract_queue.put(SENTINEL)

    def transform_worker() -> None:
        while True:
            chunk = extract_queue.get()
            if chunk is SENTINEL:
                transform_queue.put(SENTINEL)
                extract_queue.task_done()
                break
            transform_queue.put(transform_rows(chunk))
            extract_queue.task_done()

    def load_worker() -> None:
        with get_connection(destination_db_path) as destination_connection:
            insert_sql = (
                f"INSERT INTO {destination_table} (name, roll_no, email, phone_number) VALUES (?, ?, ?, ?)"
            )
            while True:
                chunk = transform_queue.get()
                if chunk is SENTINEL:
                    transform_queue.task_done()
                    break
                destination_connection.executemany(insert_sql, chunk)
                destination_connection.commit()
                transform_queue.task_done()

    extractor = Thread(target=extract_worker, name="extract-thread", daemon=True)
    transformer = Thread(target=transform_worker, name="transform-thread", daemon=True)
    loader = Thread(target=load_worker, name="load-thread", daemon=True)

    with measure_seconds() as elapsed:
        extractor.start()
        transformer.start()
        loader.start()

        extractor.join()
        transformer.join()
        loader.join()

    return elapsed()
