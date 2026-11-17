from exchange_rates.browser import fetch_headers, is_challenge, request_target


def test_request_target_keeps_existing_query():
    assert request_target("https://www.visa.com.sg/cmsapi/fx/rates") == \
        "https://www.visa.com.sg/cmsapi/fx/rates"
    assert request_target("https://www.visa.com.sg/cmsapi/fx/rates",
                          {"fromCurr": "USD", "toCurr": "CNY"}) == \
        "https://www.visa.com.sg/cmsapi/fx/rates?fromCurr=USD&toCurr=CNY"
    assert request_target("https://www.visa.com.sg/p?a=1", {"b": 2}) == \
        "https://www.visa.com.sg/p?a=1&b=2"


def test_fetch_headers_drops_browser_owned_headers():
    assert fetch_headers({"Accept": "application/json", "Referer": "https://x/",
                          "Cookie": "a=b", "User-Agent": "ua", "X-Test": "1"}) == \
        {"Accept": "application/json", "X-Test": "1"}
    assert fetch_headers(None) == {}


def test_is_challenge_matches_cloudflare_interstitial():
    assert is_challenge("<html><title>Just a moment...</title></html>")
    assert not is_challenge('{"originalValues": {"fxRateVisa": "0.15"}}')
