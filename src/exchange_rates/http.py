"""Reusable browser-fingerprinted HTTP transport with bounded retries."""

from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from datetime import datetime, timezone
from ipaddress import ip_address
import time
from threading import Event, Lock
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from curl_cffi import CurlOpt, requests
import simplejson

from .edge_dns import EdgeResolution, resolve_edges
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
    edge_ip: str | None = None
    primary_ip: str | None = None

    def as_record(self) -> dict[str, Any]:
        record = {
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
        if self.edge_ip is not None:
            record["request"]["edge_ip"] = self.edge_ip
        if self.primary_ip is not None:
            record["response"]["primary_ip"] = self.primary_ip
        return record


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
                 forbidden_threshold: int = 0,
                 edge_dns_servers: Sequence[str] = ()):
        finite_seconds("timeout", timeout, positive=True)
        if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
            raise ValueError("retries must be a non-negative integer")
        finite_seconds("interval", interval)
        if not isinstance(edge_dns_servers, (list, tuple)):
            raise ValueError("edge_dns_servers must be a list of public IP addresses")
        servers = []
        for server in edge_dns_servers:
            if not isinstance(server, str):
                raise ValueError("edge_dns_servers must contain public IP addresses")
            address = ip_address(server)
            if not address.is_global or address.is_multicast:
                raise ValueError("edge_dns_servers must contain public IP addresses")
            servers.append(str(address))
        self.edge_dns_servers = tuple(dict.fromkeys(servers))
        self._edge_cache: dict[str, EdgeResolution] = {}
        self._edge_lock = Lock()
        self._edge_sessions: dict[tuple, requests.Session] = {}
        self._preferred_edges: dict[str, str] = {}
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
        for session in self._edge_sessions.values():
            session.close()
        self._edge_sessions.clear()

    def edge_candidates(self, url: str) -> EdgeResolution | None:
        if not self.edge_dns_servers:
            return None
        self._check_cancelled()
        host = urlsplit(url).hostname
        if not host:
            raise ValueError("Regional DNS requires a URL hostname")
        with self._edge_lock:
            resolution = self._edge_cache.get(host)
            if resolution is None or resolution.expires_at <= time.time():
                resolution = resolve_edges(host, self.edge_dns_servers)
                self._edge_cache[host] = resolution
                self.records.append({"fetched_at_utc": utc_now(), "dns": {
                    "hostname": host, "queries": resolution.queries,
                    "addresses": list(resolution.addresses),
                    "complete": resolution.complete,
                }})
        self._check_cancelled()
        for key in list(self._edge_sessions):
            if key[0] == host and key[2] not in resolution.addresses:
                self._edge_sessions.pop(key).close()
        preferred = self._preferred_edges.get(host)
        addresses = resolution.addresses
        if preferred in addresses:
            addresses = (preferred,) + tuple(ip for ip in addresses if ip != preferred)
        return EdgeResolution(addresses, resolution.queries, resolution.expires_at)

    def _get_session(self, url: str = "", edge_ip: str | None = None) -> requests.Session:
        if edge_ip is None:
            if self._session is None:
                self._session = requests.Session(impersonate="chrome")
            return self._session
        address = ip_address(edge_ip)
        if not address.is_global or address.is_multicast:
            raise ValueError("Edge address must be a public IP")
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("Pinned edge requests require an HTTPS hostname")
        port = parsed.port or 443
        key = (parsed.hostname, port, str(address))
        if key not in self._edge_sessions:
            value = f"[{address}]" if address.version == 6 else str(address)
            self._edge_sessions[key] = requests.Session(
                impersonate="chrome", curl_options={
                    CurlOpt.RESOLVE: [f"{parsed.hostname}:{port}:{value}"],
                    CurlOpt.PROXY: "",
                })
        return self._edge_sessions[key]

    def fork(self, cancel_event: Event | None = None) -> "HttpClient":
        child = HttpClient(self.timeout, self.retries, self.interval, cancel_event,
                           self.global_interval, self.forbidden_cooldown,
                           self.forbidden_threshold, self.edge_dns_servers)
        child._gate = self._gate
        child._edge_cache = self._edge_cache
        child._edge_lock = self._edge_lock
        child._preferred_edges = dict(self._preferred_edges)
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

    def request_json(self, url: str, params: Mapping[str, Any] | None = None,
                     headers: Mapping[str, str] | None = None, *,
                     edge_ip: str | None = None) -> JsonResponse:
        return self._request(url, params, headers, decode_json=True, edge_ip=edge_ip)

    def request_text(self, url: str, params: Mapping[str, Any] | None = None,
                     headers: Mapping[str, str] | None = None) -> JsonResponse:
        return self._request(url, params, headers, decode_json=False)

    def _request(self, url: str, params: Mapping[str, Any] | None,
                 headers: Mapping[str, str] | None, *, decode_json: bool,
                 edge_ip: str | None = None) -> JsonResponse:
        if edge_ip is not None or not self.edge_dns_servers:
            return self._request_at(url, params, headers, decode_json=decode_json,
                                    edge_ip=edge_ip)
        start = len(self.records)
        resolution = self.edge_candidates(url)
        records = list(self.records[start:])
        if not resolution.addresses:
            raise HttpError(f"Regional DNS returned no public edge addresses for {url}",
                            records=records)
        for address in resolution.addresses:
            try:
                result = self._request_at(url, params, headers, decode_json=decode_json,
                                          edge_ip=address)
            except HttpError as exc:
                records.extend(exc.records)
                exc.records = records
                if exc.status_code is not None or address == resolution.addresses[-1]:
                    raise
            else:
                result.records = records + result.records
                return result
        raise AssertionError("unreachable edge state")

    def _request_at(self, url: str, params: Mapping[str, Any] | None,
                    headers: Mapping[str, str] | None, *, decode_json: bool,
                    edge_ip: str | None) -> JsonResponse:
        session = self._get_session(url, edge_ip)
        records: list[dict[str, Any]] = []
        for attempt in range(self.retries + 1):
            self._wait()
            epoch = None
            if self._gate.enabled:
                try:
                    epoch = self._gate.acquire(self.cancel_event)
                except GateBlockedError as exc:
                    raise AccessBlockedError(exc.response) from exc
            response_recorded = False
            try:
                self._last_started = time.monotonic()
                response = session.get(
                    url, params=params, headers=headers, timeout=self.timeout,
                    **({"allow_redirects": False} if edge_ip is not None else {}),
                )
            except requests.RequestsError as exc:
                record = {
                    "attempt": attempt + 1, "fetched_at_utc": utc_now(),
                    "request": {"method": "GET", "url": url,
                                "params": dict(params or {}),
                                "headers": dict(headers or {})},
                    "transport_error": str(exc),
                }
                if edge_ip is not None:
                    record["request"]["edge_ip"] = edge_ip
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
                    response_headers=dict(response.headers), edge_ip=edge_ip,
                    primary_ip=getattr(response, "primary_ip", None),
                )
                record = result.as_record()
                record["request"]["url"] = url
                record["attempt"] = attempt + 1
                records.append(record)
                self.records.append(record)
                result.records = records
                status = response.status_code
                if self._gate.enabled:
                    blocked = self._gate.record_response(status, result, epoch=epoch)
                    response_recorded = True
                    if blocked and status == 403:
                        raise AccessBlockedError(result, records, request_sent=True)
                if 200 <= status < 300:
                    if edge_ip is not None:
                        if result.primary_ip and ip_address(result.primary_ip) != ip_address(edge_ip):
                            raise HttpError("Response did not use the pinned edge IP", status,
                                            result, records)
                        self._preferred_edges[urlsplit(url).hostname] = edge_ip
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
            finally:
                if self._gate.enabled and not response_recorded:
                    self._gate.record_response(None, None, epoch=epoch)
            self._sleep(max(self.interval, 0.5) * (2 ** attempt))
        raise AssertionError("unreachable retry state")
