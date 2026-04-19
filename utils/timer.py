from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter


@contextmanager
def measure_seconds():
    started_at = perf_counter()
    value: dict[str, float] = {"elapsed": 0.0}
    yield lambda: value["elapsed"]
    value["elapsed"] = perf_counter() - started_at
