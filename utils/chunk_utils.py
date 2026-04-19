from __future__ import annotations

from etl.transformations import ETLRow


def estimate_row_bytes(row: ETLRow) -> int:
    return sum(len(value.encode("utf-8")) for value in row)


def chunk_by_target_size(rows: list[ETLRow], chunk_size_mb: int) -> list[list[ETLRow]]:
    target_bytes = chunk_size_mb * 1024 * 1024
    chunks: list[list[ETLRow]] = []
    current_chunk: list[ETLRow] = []
    current_bytes = 0

    for row in rows:
        current_chunk.append(row)
        current_bytes += estimate_row_bytes(row)
        if current_bytes >= target_bytes:
            chunks.append(current_chunk)
            current_chunk = []
            current_bytes = 0

    if current_chunk:
        chunks.append(current_chunk)

    return chunks
