from __future__ import annotations

from db.db_connection import get_connection
from etl.transformations import transform_rows
from utils.timer import measure_seconds


def run_case2_staged(
    source_db_path,
    destination_db_path,
    source_table: str,
    destination_table: str,
    batch_size: int,
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

        with get_connection(source_db_path) as source_connection, measure_seconds() as elapsed:
            extracted_rows = [
                tuple(row)
                for row in source_connection.execute(
                    f"SELECT name, roll_no, email, phone_number FROM {source_table}"
                ).fetchall()
            ]
            transformed_rows = transform_rows(extracted_rows)
            insert_sql = (
                f"INSERT INTO {destination_table} (name, roll_no, email, phone_number) VALUES (?, ?, ?, ?)"
            )
            for start in range(0, len(transformed_rows), batch_size):
                destination_connection.executemany(insert_sql, transformed_rows[start : start + batch_size])
            destination_connection.commit()

    return elapsed()
