"""Balanced currency shards with ordered, all-or-nothing execution."""

from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import threading
from typing import TypeVar


SHARD_COUNT = 9
T = TypeVar("T")


def balanced_shards(
    currencies: Sequence[str], count: int = SHARD_COUNT,
) -> tuple[tuple[str, ...], ...]:
    """Split unique uppercase three-letter codes into contiguous balanced groups."""
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("count must be a positive integer")
    if isinstance(currencies, (str, bytes)) or not isinstance(currencies, Sequence):
        raise ValueError("currencies must be a sequence of currency codes")
    codes = tuple(currencies)
    if any(not isinstance(code, str) or re.fullmatch(r"[A-Z]{3}", code) is None
           for code in codes):
        raise ValueError("currency codes must contain exactly three uppercase ASCII letters")
    if len(set(codes)) != len(codes):
        raise ValueError("currency codes must be unique")
    size, extra = divmod(len(codes), count)
    shards = []
    start = 0
    for index in range(count):
        end = start + size + (index < extra)
        shards.append(codes[start:end])
        start = end
    return tuple(shards)


def run_currency_shards(
    currencies: Sequence[str],
    worker: Callable[[int, tuple[str, ...], threading.Event], T],
    count: int = SHARD_COUNT,
) -> list[T]:
    """Run every shard, including empty ones, and return results in shard order.

    Workers must check the shared cancel event before each request and own
    independent clients. Failure signals cancellation and waits for workers to
    exit before re-raising the original exception; no partial results escape.
    """
    shards = balanced_shards(currencies, count)
    cancel_event = threading.Event()
    with ThreadPoolExecutor(max_workers=count, thread_name_prefix="fx-shard") as executor:
        futures = {}
        results: dict[int, T] = {}
        try:
            for index, shard in enumerate(shards):
                futures[executor.submit(worker, index, shard, cancel_event)] = index
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        except BaseException:
            cancel_event.set()
            for future in futures:
                future.cancel()
            raise
    return [results[index] for index in range(count)]
