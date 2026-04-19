from __future__ import annotations


ETLRow = tuple[str, str, str, str]


def transform_row(row: ETLRow) -> ETLRow:
    name, roll_no, email, phone_number = row
    return (
        name.upper(),
        f"RN_{roll_no}",
        email.upper(),
        f"+91{phone_number}",
    )


def transform_rows(rows: list[ETLRow]) -> list[ETLRow]:
    return [transform_row(row) for row in rows]
