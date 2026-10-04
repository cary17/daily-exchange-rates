"""Collector-local attempt pacing and shared forbidden-response state."""

from concurrent.futures import CancelledError
import math
from threading import Event, Lock
import time
from typing import Any, Callable


def finite_seconds(name: str, value: float, *, positive: bool = False) -> float:
    try:
        valid = (not isinstance(value, bool) and isinstance(value, (int, float))
                 and math.isfinite(value) and (value > 0 if positive else value >= 0))
    except OverflowError:
        valid = False
    if not valid:
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be finite and {qualifier}")
    return value


class GateBlockedError(RuntimeError):
    def __init__(self, response: Any):
        super().__init__("HTTP 403 circuit is open")
        self.response = response


def _wait(seconds: float, cancel_event: Event | None) -> bool:
    return (cancel_event if cancel_event is not None else Event()).wait(seconds)


class RateGate:
    """Forks share this gate, but each retains its own transport session."""

    WAIT_SLICE = 0.25

    def __init__(self, global_interval: float = 0,
                 forbidden_cooldown: float = 0, forbidden_threshold: int = 0, *,
                 clock: Callable[[], float] | None = None,
                 wait: Callable[[float, Event | None], bool] | None = None):
        self.global_interval = finite_seconds("global_interval", global_interval)
        self.forbidden_cooldown = finite_seconds("forbidden_cooldown", forbidden_cooldown)
        if (isinstance(forbidden_threshold, bool)
                or not isinstance(forbidden_threshold, int) or forbidden_threshold < 0):
            raise ValueError("forbidden_threshold must be a non-negative integer")
        self.forbidden_threshold = forbidden_threshold
        self.enabled = bool(global_interval or forbidden_cooldown or forbidden_threshold)
        self._clock = clock if clock is not None else time.monotonic
        self._wait = wait if wait is not None else _wait
        self._lock = Lock()
        self._next_started = 0.0
        self._cooldown_until = 0.0
        self.consecutive_forbidden = 0
        self.broken = False
        self.last_response: Any = None

    def acquire(self, cancel_event: Event | None = None) -> None:
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise CancelledError("Currency shard cancelled after another shard failed")
            with self._lock:
                if self.broken:
                    raise GateBlockedError(self.last_response)
                now = self._clock()
                remaining = max(self._next_started, self._cooldown_until) - now
                if remaining <= 0:
                    self._next_started = now + self.global_interval
                    return
            # No future reservations: every wake rechecks cooldown and broken.
            self._wait(min(remaining, self.WAIT_SLICE), cancel_event)

    def record_response(self, status: int, response: Any) -> bool:
        with self._lock:
            if self.broken:
                return True
            if status == 200:
                self.consecutive_forbidden = 0
            elif status == 403:
                self.last_response = response
                self.consecutive_forbidden += 1
                if self.forbidden_cooldown:
                    self._cooldown_until = max(
                        self._cooldown_until, self._clock() + self.forbidden_cooldown,
                    )
                if (self.forbidden_threshold
                        and self.consecutive_forbidden >= self.forbidden_threshold):
                    self.broken = True
            return self.broken
