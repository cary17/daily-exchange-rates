"""Reusable browser-fingerprinted HTTP transport with bounded retries."""

from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from datetime import datetime, timezone
import time
from threading import Event
from typing import Any, Mapping

from curl_cffi import requests
import simplejson

from .rate_control import GateBlockedError, RateGate, finite_seconds


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class JsonResponse:
    data: Any
    body_text: str
    body: bytes
    url: str
    params: Mapping[str, Any] | None = None
    request_headers: Mapping[str, str] | None = None
    status_code: int = 200
    response_headers: Mapping[str, str] = field(default_factory=dict)
    fetched_at_utc: str = field(default_factory=utc_now)
    records: list[dict[str, Any]] = field(default_factory=list)

    def as_record(self) -> dict[str, Any]:
        return {
            "fetched_at_utc": self.fetched_at_utc,
            "request": {
                "method": "GET", "url": self.url,
                "params": dict(self.params or {}),
                "headers": dict(self.request_headers or {}),
            },
            "response": {
                "url": self.url, "status_code": self.status_code,
                "headers": dict(self.response_headers), "body_text": self.body_text,
            },
        }


class HttpError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None,
                 response: JsonResponse | None = None,
                 records: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response
        self.records = records or []


class ResponseDecodeError(HttpError):
    pass


class AccessBlockedError(HttpError):
    def __init__(self, response: JsonResponse | None = None,
                 records: list[dict[str, Any]] | None = None, *, request_sent: bool = False):
        super().__init__("HTTP 403 circuit is open", 403, response,
                         records if records is not None else
                         (response.records if response is not None else []))
        self.last_status = 403
        self.request_sent = request_sent


class HttpClient:
    def __init__(self, timeout: float = 30, retries: int = 3,
                 interval: float = 0.3, cancel_event: Event | None = None,
                 global_interval: float = 0, forbidden_cooldown: float = 0,
                 forbidden_threshold: int = 0):
        finite_seconds("timeout", timeout, positive=True)
        if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
            raise ValueError("retries must be a non-negative integer")
        finite_seconds("interval", interval)
        self.timeout = timeout
        self.retries = retries
        self.interval = interval
        self.cancel_event = cancel_event
        self.global_interval = global_interval
        self.forbidden_cooldown = forbidden_cooldown
        self.forbidden_threshold = forbidden_threshold
        self._gate = RateGate(global_interval, forbidden_cooldown, forbidden_threshold)
        self._session: requests.Session | None = None
        self._last_started: float | None = None
        self.records: list[dict[str, Any]] = []

    def __enter__(self) -> "HttpClient":
        self._get_session()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session(impersonate="chrome")
        return self._session

    def fork(self, cancel_event: Event | None = None) -> "HttpClient":
        child = HttpClient(self.timeout, self.retries, self.interval, cancel_event,
                           self.global_interval, self.forbidden_cooldown,
                           self.forbidden_threshold)
        child._gate = self._gate
        return child

    def _check_cancelled(self) -> None:
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise CancelledError("Currency shard cancelled after another shard failed")

    def _sleep(self, seconds: float) -> None:
        if self.cancel_event is not None:
            if self.cancel_event.wait(seconds):
                self._check_cancelled()
        else:
            time.sleep(seconds)

    def _wait(self) -> None:
        self._check_cancelled()
        if self._last_started is not None:
            remaining = self.interval - (time.monotonic() - self._last_started)
            if remaining > 0:
                self._sleep(remaining)
        self._last_started = time.monotonic()

    def request_json(self, url: str, params: Mapping[str, Any] | None = None,
                     headers: Mapping[str, str] | None = None) -> JsonResponse:
        return self._request(url, params, headers, decode_json=True)

    def request_text(self, url: str, params: Mapping[str, Any] | None = None,
                     headers: Mapping[str, str] | None = None) -> JsonResponse:
        return self._request(url, params, headers, decode_json=False)

    def _request(self, url: str, params: Mapping[str, Any] | None,
                 headers: Mapping[str, str] | None, *, decode_json: bool) -> JsonResponse:
        session = self._get_session()
        records: list[dict[str, Any]] = []
        for attempt in range(self.retries + 1):
            self._wait()
            if self._gate.enabled:
                try:
                    self._gate.acquire(self.cancel_event)
                except GateBlockedError as exc:
                    raise AccessBlockedError(exc.response) from exc
            try:
                response = session.get(
                    url, params=params, headers=headers, timeout=self.timeout,
                )
            except requests.RequestsError as exc:
                record = {
                    "attempt": attempt + 1, "fetched_at_utc": utc_now(),
                    "request": {"method": "GET", "url": url,
                                "params": dict(params or {}),
                                "headers": dict(headers or {})},
                    "transport_error": str(exc),
                }
                records.append(record)
                self.records.append(record)
                if attempt == self.retries:
                    raise HttpError(f"Transport failure for {url}: {exc}",
                                    records=records) from exc
            else:
                result = JsonResponse(
                    data=None, body_text=response.text, body=response.content,
                    url=str(response.url or url), params=dict(params or {}),
                    request_headers=dict(headers or {}),
                    status_code=response.status_code,
                    response_headers=dict(response.headers),
                )
                record = result.as_record()
                record["request"]["url"] = url
                record["attempt"] = attempt + 1
                records.append(record)
                self.records.append(record)
                result.records = records
                status = response.status_code
                if self._gate.enabled:
                    if self._gate.record_response(status, result) and status == 403:
                        raise AccessBlockedError(result, records, request_sent=True)
                if 200 <= status < 300:
                    if not decode_json:
                        return result
                    try:
                        result.data = simplejson.loads(
                            result.body_text, use_decimal=True, allow_nan=False,
                        )
                    except (ValueError, UnicodeError) as exc:
                        raise ResponseDecodeError(
                            f"Invalid JSON from {url}: {exc}", status, result,
                            records,
                        ) from exc
                    return result
                if status != 429 and not 500 <= status < 600:
                    raise HttpError(f"HTTP {status} for {url}", status, result,
                                    records)
                if attempt == self.retries:
                    raise HttpError(f"HTTP {status} for {url} after retries",
                                    status, result, records)
            self._sleep(max(self.interval, 0.5) * (2 ** attempt))
        raise AssertionError("unreachable retry state")
