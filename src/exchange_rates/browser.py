"""Single-page browser transport for official sites behind an interactive challenge.

Some providers (Visa) now answer every plain HTTP client with a Cloudflare
managed challenge. A real browser clears it, but cookies or TLS impersonation
alone do not, because Cloudflare binds the verdict to the browser session that
solved it. Requests therefore have to leave the page itself, so this transport
owns one headed browser and serialises every request onto it.
"""

from __future__ import annotations

import atexit
import os
import queue
import threading
import time
from typing import Any, Mapping
from urllib.parse import urlencode, urlsplit

_USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
_LAUNCH_ARGS = ["--no-sandbox", "--disable-blink-features=AutomationControlled"]
# Headers a page may not set; the browser derives them from the session instead.
_FORBIDDEN_HEADERS = frozenset({
    "host", "cookie", "referer", "origin", "user-agent", "content-length",
    "connection", "accept-encoding",
})
_CHALLENGE_TIMEOUT = 90
_FETCH_JS = """async ([url, headers, timeoutMs]) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {credentials: 'include', headers,
                                       signal: controller.signal, redirect: 'follow'});
    return [response.status, response.url, await response.text(),
            Array.from(response.headers.entries())];
  } finally {
    clearTimeout(timer);
  }
}"""


class BrowserTransportError(RuntimeError):
    """The browser could not answer, so the caller should retry or fail the request."""


class BrowserResponse:
    def __init__(self, status_code: int, url: str, text: str,
                 headers: Mapping[str, str]):
        self.status_code = status_code
        self.url = url
        self.text = text
        self.headers = dict(headers)
        self.primary_ip = None

    @property
    def content(self) -> bytes:
        return self.text.encode("utf-8")


def _origin(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise BrowserTransportError(f"browser transport requires an absolute URL: {url}")
    return f"{parsed.scheme}://{parsed.netloc}"


def request_target(url: str, params: Mapping[str, Any] | None = None) -> str:
    """Absolute URL including query parameters, as the page should request it."""
    if not params:
        return url
    parsed = urlsplit(url)
    extra = urlencode({key: str(value) for key, value in params.items()})
    query = f"{parsed.query}&{extra}" if parsed.query else extra
    return parsed._replace(query=query).geturl()


def fetch_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    return {key: value for key, value in (headers or {}).items()
            if key.lower() not in _FORBIDDEN_HEADERS}


def is_challenge(body: str) -> bool:
    return "just a moment" in body[:4096].lower()


class BrowserTransport:
    """Owns one headed Chromium and serves requests from any calling thread."""

    def __init__(self, timeout: float = 30, challenge_timeout: float = _CHALLENGE_TIMEOUT):
        if not os.environ.get("DISPLAY") and os.name != "nt":
            raise BrowserTransportError(
                "browser mode needs a display; run the collector under xvfb-run")
        self.timeout = timeout
        self.challenge_timeout = challenge_timeout
        self._jobs: "queue.Queue" = queue.Queue()
        self._origin: str | None = None
        self._error: BaseException | None = None
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._serve, name="fx-browser", daemon=True)
        self._thread.start()
        self._ready.wait()
        if self._error is not None:
            raise BrowserTransportError(f"browser failed to start: {self._error}") from self._error
        atexit.register(self.close)

    def get(self, url: str, params: Mapping[str, Any] | None = None,
            headers: Mapping[str, str] | None = None, **_ignored: Any) -> BrowserResponse:
        return self._call(self._get, url, params, headers)

    def close(self) -> None:
        if not self._thread.is_alive():
            return
        self._jobs.put(None)
        self._thread.join(timeout=30)

    # ---- worker thread ----

    def _serve(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(
                headless=False, args=_LAUNCH_ARGS)
            self._context = self._browser.new_context(
                locale="en-US", user_agent=_USER_AGENT)
            self._page = self._context.new_page()
            self._page.set_default_timeout(self.timeout * 1000)
        except BaseException as exc:  # surfaced to the constructing thread
            self._error = exc
            self._ready.set()
            return
        self._ready.set()
        while True:
            job = self._jobs.get()
            if job is None:
                break
            func, args, box, done = job
            try:
                box.append(func(*args))
            except BaseException as exc:
                box.append(exc)
            done.set()
        try:
            self._browser.close()
            self._playwright.stop()
        except Exception:
            pass

    def _call(self, func, *args):
        if not self._thread.is_alive():
            raise BrowserTransportError("browser transport is closed")
        box: list = []
        done = threading.Event()
        self._jobs.put((func, args, box, done))
        done.wait()
        result = box[0]
        if isinstance(result, BaseException):
            raise result
        return result

    def _get(self, url: str, params: Mapping[str, Any] | None,
             headers: Mapping[str, str] | None) -> BrowserResponse:
        origin = _origin(url)
        referer = next((value for key, value in (headers or {}).items()
                        if key.lower() == "referer"), None)
        self._solve(origin, referer)
        target = request_target(url, params)
        status, final_url, body, raw_headers = self._fetch(target, headers)
        if is_challenge(body):
            self._solve(origin, referer, refresh=True)
            status, final_url, body, raw_headers = self._fetch(target, headers)
        return BrowserResponse(status, final_url or target, body, dict(raw_headers))

    def _fetch(self, target: str, headers: Mapping[str, str] | None):
        try:
            return self._page.evaluate(
                _FETCH_JS, [target, fetch_headers(headers), int(self.timeout * 1000)])
        except Exception as exc:
            raise BrowserTransportError(f"page fetch failed for {target}: {exc}") from exc

    def _solve(self, origin: str, referer: str | None, *, refresh: bool = False) -> None:
        if origin == self._origin and not refresh:
            return
        target = referer if isinstance(referer, str) and _origin(referer) == origin \
            else origin + "/"
        try:
            self._page.goto(target, wait_until="domcontentloaded",
                            timeout=int(self.challenge_timeout * 1000))
        except Exception:
            pass  # the challenge page often abandons the navigation; the title decides
        deadline = time.time() + self.challenge_timeout
        while time.time() < deadline:
            try:
                if "just a moment" not in self._page.title().lower():
                    self._origin = origin
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise BrowserTransportError(f"browser challenge was not cleared for {origin}")


_transport: BrowserTransport | None = None
_transport_lock = threading.Lock()


def browser_transport(timeout: float = 30) -> BrowserTransport:
    """Process-wide transport: every client and fork shares one browser page."""
    global _transport
    with _transport_lock:
        if _transport is None:
            _transport = BrowserTransport(timeout)
        return _transport
