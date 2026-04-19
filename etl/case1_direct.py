from __future__ import annotations

from db.db_connection import get_connection
from etl.transformations import transform_row
from utils.timer import measure_seconds


def run_case1_direct(source_db_path, destination_db_path, source_table: str, destination_table: str) -> float:
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
            cursor = source_connection.execute(
                f"SELECT name, roll_no, email, phone_number FROM {source_table}"
            )
            insert_sql = (
                f"INSERT INTO {destination_table} (name, roll_no, email, phone_number) VALUES (?, ?, ?, ?)"
            )
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                destination_connection.execute(insert_sql, transform_row(tuple(row)))
            destination_connection.commit()

    return elapsed()
