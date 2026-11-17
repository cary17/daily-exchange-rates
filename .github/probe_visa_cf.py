import os
import time
from urllib.request import urlopen

from curl_cffi import requests as creq
from playwright.sync_api import sync_playwright

PAGE = os.environ["VISA_PAGE"]
API = os.environ["VISA_API"]
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def step(label, value):
    print(f"PROBE {label}: {value}", flush=True)


try:
    step("runner_ip", urlopen("https://api.ipify.org", timeout=20).read().decode())
except Exception as exc:
    step("runner_ip", f"unavailable ({type(exc).__name__})")

try:
    r = creq.Session(impersonate="chrome").get(API, headers={"Accept": "application/json"}, timeout=30)
    step("curl_cffi_no_cookies", f"{r.status_code} {r.text[:80]}")
except Exception as exc:
    step("curl_cffi_no_cookies", f"error {type(exc).__name__}: {exc}")

jar = {}
with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=False,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
    ctx = browser.new_context(locale="en-US", user_agent=UA)
    page = ctx.new_page()
    started = time.time()
    page.goto(PAGE, wait_until="domcontentloaded", timeout=90000)
    while "just a moment" in page.title().lower() and time.time() - started < 90:
        time.sleep(1)
    step("browser_title", page.title())
    step("browser_challenge_seconds", f"{time.time() - started:.1f}")
    step("browser_has_currency_list", "currencyList" in page.content())

    jar = {c["name"]: c["value"] for c in ctx.cookies()}
    step("browser_cookies", ",".join(sorted(jar)))

    def in_page_fetch(url, accept):
        return page.evaluate(
            """async ([u, a]) => {
                 const r = await fetch(u, {credentials: 'include', headers: {'Accept': a}});
                 const body = await r.text();
                 return [r.status, body.slice(0, 120)];
               }""",
            [url, accept])

    status, body = in_page_fetch(API, "application/json")
    step("page_fetch_api", f"{status} {body}")
    step("page_fetch_api_has_rate", '"fxRateVisa"' in body)

    for n in (1, 2, 3, 4, 5):
        s, b = in_page_fetch(API, "application/json")
        step(f"page_fetch_repeat_{n}", f"{s} {'fxRateVisa' in b}")
        time.sleep(2)

    s, b = in_page_fetch(PAGE, "text/html")
    step("page_fetch_html", f"{s} len={len(b)}")
    step("context_request_api", str(ctx.request.get(API).status))

    browser.close()

header = {"Accept": "application/json", "Referer": PAGE,
          "Cookie": "; ".join(f"{k}={v}" for k, v in jar.items()), "User-Agent": UA}
try:
    session = creq.Session(impersonate="chrome")
    reply = session.get(API, headers=header, timeout=30)
    step("replay_with_cookies", f"{reply.status_code}")
    session.close()
except Exception as exc:
    step("replay_with_cookies", f"error {type(exc).__name__}: {exc}")
