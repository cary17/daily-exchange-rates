"""Offline provider and HTTP contracts, including precision and atomic days."""

from concurrent.futures import CancelledError
from datetime import date, datetime, timezone
from decimal import Decimal
from email.utils import formatdate
import hashlib
from html import escape
from itertools import permutations
from urllib.parse import urlsplit
from threading import Event, Lock
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from curl_cffi import requests
import simplejson

from exchange_rates.catalog import CurrencyCatalog, discover_currencies
from exchange_rates.cli import http_settings, load_config
from exchange_rates.http import HttpClient, HttpError, JsonResponse, ResponseDecodeError
from exchange_rates.models import DayResult, json_bytes
from exchange_rates.providers import (
    DateMismatchError, NoDataError, ProviderError, RecoveryError, _query_pair,
    fetch_day, provider_ids,
)


DAY = date(2026, 3, 27)
CURRENCIES = ("CNY", "USD", "EUR", "JPY", "HKD", "GBP", "AUD", "CAD", "CHF", "SGD", "KRW")


def response(data, url="https://example.test/rates", params=None, headers=None,
             body=None):
    if body is None:
        body = json_bytes(data)
    return JsonResponse(data=data, body_text=body.decode("utf-8"), body=body,
                        url=url, params=params, request_headers=headers)


class FakeClient:
    def __init__(self, provider, mutate=None, fail_at=None):
        self.provider = provider
        self.mutate = mutate
        self.fail_at = fail_at
        self.calls = []
        self.records = []
        self.forks = []
        self.lock = Lock()
        self.failed = Event()

    def fork(self, cancel_event=None):
        child = FakeFork(self, cancel_event)
        with self.lock:
            self.forks.append(child)
        return child

    def edge_candidates(self, url):
        return None

    def request_json(self, url, params=None, headers=None, *, edge_fallback=True):
        with self.lock:
            self.calls.append((url, dict(params or {}), dict(headers or {})))
            call_index = len(self.calls)
            if call_index == self.fail_at:
                self.failed.set()
        if call_index == self.fail_at:
            raise HttpError("HTTP 503", 503)
        if self.provider == "visa":
            spend, home = params["toCurr"], params["fromCurr"]
            rate = Decimal("1.23456789012345678901234567890123456789")
            if spend > home:
                rate = Decimal("0.987654321098765432109876543210987654321")
            data = {"status": "success", "originalValues": {
                "fxRateVisa": rate, "toAmountWithVisaRate": "1234.57" if spend < home else "987.65",
                "exchangedate": DAY.strftime("%m/%d/%Y"),
            }}
        else:
            spend, home = params["transaction_currency"], params["cardholder_billing_currency"]
            rate = Decimal("1.23456789012345678901234567890123456789")
            if spend > home:
                rate = Decimal("0.987654321098765432109876543210987654321")
            data = {"data": {
                "errorCode": "0", "conversionRate": rate,
                "crdhldBillAmt": "1234.57" if spend < home else "987.65",
                "exchange_date": DAY.isoformat(),
            }}
        if self.mutate:
            self.mutate(data)
        return response(data, url, params, headers)


class FakeFork:
    def __init__(self, owner, cancel_event):
        self.owner = owner
        self.cancel_event = cancel_event
        self.records = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def edge_candidates(self, url):
        return self.owner.edge_candidates(url)

    def request_json(self, *args, **kwargs):
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise CancelledError()
        kwargs.pop("edge_fallback", None)
        result = self.owner.request_json(*args, **kwargs)
        self.records.extend(result.records or [result.as_record()])
        return result


def forkable_mock():
    client = Mock()
    client.edge_candidates.return_value = None
    client.fork.side_effect = lambda cancelled: FakeFork(client, cancelled)
    return client


def call_pair(provider, params):
    if provider == "visa":
        return params["toCurr"], params["fromCurr"]
    return params["transaction_currency"], params["cardholder_billing_currency"]


class ProviderTests(unittest.TestCase):
    def test_registry(self):
        self.assertEqual(provider_ids(), ("unionpay", "visa", "mastercard", "jcb"))
        with self.assertRaises(ValueError):
            fetch_day("unknown", DAY, CURRENCIES, {}, FakeClient("visa"))

    def test_decimal_json_is_number_and_exact(self):
        number = Decimal("0.123456789012345678901234567890123456789")
        encoded = json_bytes({"rate": number, "label": "人民币", "aaa": 1,
                              "curDate": DAY.isoformat()})
        self.assertEqual(list(simplejson.loads(encoded)), ["curDate", "aaa", "label", "rate"])
        self.assertIn(str(number).encode(), encoded)
        self.assertNotIn(('"' + str(number) + '"').encode(), encoded)
        self.assertIn("人民币".encode(), encoded)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertEqual(simplejson.loads(encoded, use_decimal=True)["rate"], number)

    def test_direction_parameters_and_all_ordered_pairs(self):
        for provider in ("visa", "mastercard"):
            with self.subTest(provider=provider):
                client = FakeClient(provider)
                result = fetch_day(provider, DAY, CURRENCIES, {}, client)
                self.assertIsInstance(result, DayResult)
                data = simplejson.loads(result.normalized, use_decimal=True)
                self.assertEqual(list(data), ["curDate", "exchangeRateJson"])
                self.assertEqual(data["curDate"], DAY.isoformat())
                rows = data["exchangeRateJson"]
                self.assertEqual(len(rows), 110)
                self.assertEqual(len(client.calls), 110)
                self.assertEqual({(r["transCur"], r["baseCur"]) for r in rows},
                                 set(permutations(CURRENCIES, 2)))
                calls = {call_pair(provider, params): (params, headers)
                         for _, params, headers in client.calls}
                for row in rows:
                    spend, home = row["transCur"], row["baseCur"]
                    params, headers = calls[spend, home]
                    self.assertIn("Referer", headers)
                    if provider == "visa":
                        self.assertEqual(params, {
                            "amount": "1000", "fee": "0", "utcConvertedDate": "03/27/2026",
                            "exchangedate": "03/27/2026", "fromCurr": home, "toCurr": spend,
                        })
                    else:
                        self.assertEqual(params, {
                            "exchange_date": "2026-03-27", "transaction_currency": spend,
                            "cardholder_billing_currency": home, "bank_fee": "0",
                            "transaction_amount": "1000",
                        })
                forward = next(r["rateData"] for r in rows if (r["transCur"], r["baseCur"]) == ("CNY", "USD"))
                reverse = next(r["rateData"] for r in rows if (r["transCur"], r["baseCur"]) == ("USD", "CNY"))
                self.assertEqual(forward, Decimal("1.23456789012345678901234567890123456789"))
                self.assertEqual(reverse, Decimal("0.987654321098765432109876543210987654321"))
                self.assertEqual(result.metadata["pair_count"], 110)
                records = [record for name in sorted(result.raw_parts)
                           for record in simplejson.loads(result.raw_parts[name])["requests"]]
                self.assertEqual(len(records), 110)
                self.assertEqual([call_pair(provider, r["request"]["params"]) for r in records],
                                 [(r["transCur"], r["baseCur"]) for r in rows])
                for record in records:
                    expected = FakeClient(provider).request_json("ignored", record["request"]["params"])
                    self.assertEqual(record["response"]["body_text"], expected.body_text)

    def test_one_failure_recovers_after_complete_scan(self):
        for provider in ("visa", "mastercard"):
            client = FakeClient(provider, fail_at=7)
            with self.subTest(provider=provider):
                result = fetch_day(provider, DAY, CURRENCIES, {}, client)
            self.assertEqual(len(client.calls), 111)
            self.assertEqual(result.metadata["recovery"],
                             {"initial_failed": 1, "rounds": 1, "repaired": 1})
            self.assertTrue(all(child.closed for child in client.forks))
            self.assertTrue(all(not child.cancel_event.is_set() for child in client.forks))

    def test_missing_required_fields_fail(self):
        for provider, container, keys in (
            ("visa", "originalValues", ("fxRateVisa", "toAmountWithVisaRate")),
            ("mastercard", "data", ("conversionRate", "crdhldBillAmt")),
        ):
            for key in keys:
                client = FakeClient(provider, mutate=lambda obj, c=container, k=key: obj[c].pop(k))
                with self.subTest(provider=provider, missing=key), self.assertRaises(ProviderError):
                    fetch_day(provider, DAY, CURRENCIES, {}, client)
                self.assertEqual(len(client.calls), 330)
                self.assertEqual(len(client.forks), 27)

    def test_card_error_status_and_bill_mismatch(self):
        mutations = (
            ("visa", lambda d: d.update(status="error")),
            ("mastercard", lambda d: d["data"].update(errorCode="NO_RATE")),
            ("visa", lambda d: d["originalValues"].update(toAmountWithVisaRate="9000")),
            ("mastercard", lambda d: d["data"].update(crdhldBillAmt="9000")),
        )
        for provider, mutate in mutations:
            client = FakeClient(provider, mutate=mutate)
            with self.subTest(provider=provider), self.assertRaises(ProviderError):
                fetch_day(provider, DAY, CURRENCIES, {}, client)
            self.assertEqual(len(client.calls), 330)

    def test_rounding_is_allowed(self):
        for provider, container, field in (
            ("visa", "originalValues", "toAmountWithVisaRate"),
            ("mastercard", "data", "crdhldBillAmt"),
        ):
            client = FakeClient(provider, mutate=lambda d, c=container, f=field: d[c].update({f: "1235" if Decimal(str(d[c].get("fxRateVisa", d[c].get("conversionRate")))) > 1 else "988"}))
            result = fetch_day(provider, DAY, ("CNY", "USD"), {}, client)
            self.assertEqual(result.metadata["pair_count"], 2)

    def test_unionpay_date_first_preserves_fields_precision_and_raw_bytes(self):
        body = (b'{ "date":"20260327", "extra":{"source":"complete"}, "exchangeRateJson":['
                b'{"transCur":"USD","baseCur":"CNY","rateData":7.1234567890123456789012345678901},'
                b'{"transCur":"EUR","baseCur":"JPY","rateData":161.987654321}]}\n')
        data = simplejson.loads(body, use_decimal=True)
        client = Mock()
        client.request_json.return_value = response(data, body=body)
        result = fetch_day("unionpay", DAY, ("USD", "CNY"), {}, client)
        self.assertEqual(result.normalized, json_bytes({**data, "curDate": DAY.isoformat()}))
        self.assertEqual(next(iter(simplejson.loads(result.normalized))), "curDate")
        self.assertEqual(len(simplejson.loads(result.normalized)["exchangeRateJson"]), 2)
        client.request_json.assert_called_once_with(
            "https://www.unionpayintl.com/upload/jfimg/20260327.json",
            headers={"Accept": "application/json, text/plain, */*"},
        )
        raw = simplejson.loads(result.raw)
        self.assertEqual(raw["requests"][0]["response"]["body_text"], body.decode())
        self.assertNotIn("metadata", simplejson.loads(result.normalized))

    def test_unionpay_no_data_date_and_structure(self):
        client = Mock()
        client.request_json.side_effect = HttpError("not found", 404)
        with self.assertRaises(NoDataError):
            fetch_day("unionpay", DAY, (), {}, client)
        client.request_json.side_effect = HttpError("forbidden", 403)
        with self.assertRaises(HttpError):
            fetch_day("unionpay", DAY, (), {}, client)
        client.request_json.side_effect = None
        samples = (
            ({"exchangeRateJson": []}, NoDataError),
            ({"exchangeRateJson": {}}, ProviderError),
            ({"exchangeRateJson": [{"transCur": "USD", "baseCur": "CNY"}]}, ProviderError),
            ({"exchangeRateJson": [{"transCur": "USD", "baseCur": "CNY", "rateData": "7.1"}]}, ProviderError),
            ({"date": "20260326", "exchangeRateJson": [{"transCur": "USD", "baseCur": "CNY", "rateData": Decimal("7.1")}]}, DateMismatchError),
        )
        for data, exception in samples:
            client.request_json.return_value = response(data)
            with self.subTest(data=data), self.assertRaises(exception):
                fetch_day("unionpay", DAY, (), {}, client)

    def test_absent_response_date_does_not_trigger_fallback(self):
        for provider, container, field in (
            ("visa", "originalValues", "exchangedate"),
            ("mastercard", "data", "exchange_date"),
        ):
            client = FakeClient(provider, mutate=lambda d, c=container, f=field: d[c].pop(f))
            result = fetch_day(provider, DAY, ("USD", "CNY"), {}, client)
            self.assertEqual(result.requested_date, DAY)
            self.assertEqual(len(client.calls), 2)

    def test_display_precision_limits_rounding_tolerance(self):
        client = FakeClient("visa", mutate=lambda d: d["originalValues"].update(toAmountWithVisaRate="1234.60"))
        with self.assertRaises(ProviderError):
            fetch_day("visa", DAY, ("CNY", "USD"), {}, client)

    def test_official_response_date_and_currency_fields(self):
        target = date(2026, 9, 30)
        for provider in ("unionpay", "visa", "mastercard"):
            client = forkable_mock()
            client.request_json.side_effect = lambda url, params=None, headers=None, p=provider: response(
                official_data(p, params), url, params, headers,
            )
            result = fetch_day(provider, target, ("USD", "CNY"), {}, client)
            self.assertEqual(result.requested_date, target)
            normalized = simplejson.loads(result.normalized)
            self.assertEqual(list(normalized), ["curDate", "exchangeRateJson"])
            self.assertEqual(normalized["curDate"], "2026-09-30")

    def test_official_effective_dates_must_match(self):
        samples = (
            ("unionpay", None, "curDate", "2026-09-29"),
            ("visa", None, "conversionInputDate", "09/29/2026"),
            ("visa", "originalValues", "asOfDate", 1790640000),
            ("mastercard", "data", "fxDate", "2026-09-29"),
        )
        for provider, container, field, wrong in samples:
            def altered(url, params=None, headers=None):
                data = official_data(provider, params)
                (data[container] if container else data)[field] = wrong
                return response(data, url, params, headers)
            client = forkable_mock()
            client.request_json.side_effect = altered
            expected_error = DateMismatchError if provider == "unionpay" else RecoveryError
            with self.subTest(provider=provider, field=field), self.assertRaises(expected_error):
                fetch_day(provider, date(2026, 9, 30), ("USD", "CNY"), {}, client)
            self.assertEqual(client.request_json.call_count, 3 if provider == "unionpay" else 6)

    def test_official_returned_currency_direction_must_match(self):
        for provider, container, fields in (
            ("visa", "originalValues", ("fromCurrency", "toCurrency")),
            ("mastercard", "data", ("transCurr", "crdhldBillCurr")),
        ):
            for field in fields:
                def altered(url, params=None, headers=None):
                    data = official_data(provider, params)
                    data[container][field] = "EUR"
                    return response(data, url, params, headers)
                client = forkable_mock()
                client.request_json.side_effect = altered
                with self.subTest(provider=provider, field=field), self.assertRaises(ProviderError):
                    fetch_day(provider, date(2026, 9, 30), ("USD", "CNY"), {}, client)
                self.assertEqual(client.request_json.call_count, 6)

    def test_publication_and_current_dates_are_not_rate_dates(self):
        for provider in ("unionpay", "visa", "mastercard"):
            def actual_with_metadata(url, params=None, headers=None):
                data = official_data(provider, params)
                data.update(currentDate="2026-10-02", publicationDate="2026-10-01")
                container = "originalValues" if provider == "visa" else "data"
                if provider != "unionpay":
                    data[container].update(currentDate="2026-10-02", publicationDate="2026-10-01")
                return response(data, url, params, headers)
            client = forkable_mock()
            client.request_json.side_effect = actual_with_metadata
            result = fetch_day(provider, date(2026, 9, 30), ("USD", "CNY"), {}, client)
            self.assertEqual(result.requested_date, date(2026, 9, 30))

    def test_visa_unix_date_uses_utc_and_rejects_invalid_types(self):
        for timestamp in (True, "1790726400", None, 10 ** 30):
            def altered(url, params=None, headers=None):
                data = official_data("visa", params)
                data["originalValues"]["asOfDate"] = timestamp
                return response(data, url, params, headers)
            client = forkable_mock()
            client.request_json.side_effect = altered
            with self.subTest(timestamp=timestamp), self.assertRaises(ProviderError):
                fetch_day("visa", date(2026, 9, 30), ("USD", "CNY"), {}, client)

    def test_custom_institution_config(self):
        client = FakeClient("visa")
        fetch_day("visa", DAY, ("USD", "CNY"),
                  {"endpoint": "https://local.test/fx", "referer": "https://local.test/calc"}, client)
        self.assertEqual(client.calls[0][0], "https://local.test/fx")
        self.assertEqual(client.calls[0][2]["Referer"], "https://local.test/calc")

    def test_directional_catalog_shards_and_raw_index(self):
        catalog = CurrencyCatalog(CURRENCIES, ("USD", "SKK", "XXX"))
        for provider in ("visa", "mastercard"):
            with self.subTest(provider=provider):
                client = FakeClient(provider)
                result = fetch_day(provider, DAY, catalog, {}, client)
                expected = list(catalog.pairs())
                rows = simplejson.loads(result.normalized, use_decimal=True)["exchangeRateJson"]
                pairs = [(row["transCur"], row["baseCur"]) for row in rows]
                self.assertEqual(pairs, expected)
                self.assertEqual(len(set(pairs)), catalog.pair_count)
                self.assertEqual(len(client.calls), catalog.pair_count)
                self.assertNotIn(("USD", "USD"), pairs)
                self.assertIn(("CNY", "SKK"), pairs)
                self.assertIn(call_pair(provider, client.calls[0][1]), expected)
                self.assertEqual(len(client.forks), 9)
                self.assertEqual(len({id(child) for child in client.forks}), 9)
                self.assertEqual(len({id(child.records) for child in client.forks}), 9)
                self.assertTrue(all(child.closed for child in client.forks))
                self.assertEqual(len({id(child.cancel_event) for child in client.forks}), 1)
                self.assertEqual(result.metadata["shard_count"], 9)
                self.assertEqual(result.metadata["transaction_currencies"], list(catalog.transaction))
                self.assertEqual(result.metadata["billing_currencies"], list(catalog.billing))
                names = [f"shard-{index:02d}.json" for index in range(1, 10)]
                self.assertEqual(list(result.raw_parts), names)
                index = simplejson.loads(result.raw)
                self.assertNotIn("requests", index)
                self.assertEqual(index["format"], "exchange-rates-raw-shards-v1")
                self.assertEqual(index["parts"], [
                    {"name": name, "size": len(result.raw_parts[name]),
                     "sha256": hashlib.sha256(result.raw_parts[name]).hexdigest()}
                    for name in names])
                parts = [simplejson.loads(result.raw_parts[name]) for name in names]
                lengths = [len(part["transaction_currencies"]) for part in parts]
                self.assertLessEqual(max(lengths) - min(lengths), 1)
                self.assertEqual([part["shard_index"] for part in parts], list(range(9)))
                self.assertEqual([code for part in parts for code in part["transaction_currencies"]],
                                 list(catalog.transaction))
                for shard, part in zip(result.metadata["shards"], parts):
                    actual = [call_pair(provider, r["request"]["params"]) for r in part["requests"]]
                    self.assertEqual(actual, [(spend, home) for spend in part["transaction_currencies"]
                                              for home in catalog.billing if spend != home])
                    self.assertEqual(shard["pair_count"], len(actual))

    def test_empty_shards_still_have_raw_files(self):
        for provider in ("visa", "mastercard"):
            result = fetch_day(provider, DAY, ("USD", "CNY"), {}, FakeClient(provider))
            self.assertEqual(len(result.raw_parts), 9)
            for index, name in enumerate(sorted(result.raw_parts)):
                part = simplejson.loads(result.raw_parts[name])
                self.assertEqual(part["shard_index"], index)
                if index >= 2:
                    self.assertEqual(part["transaction_currencies"], [])
                    self.assertEqual(part["requests"], [])

    def test_none_discovers_catalog_but_explicit_catalog_does_not(self):
        for provider in ("visa", "mastercard"):
            catalog = CurrencyCatalog(("USD", "SKK"), ("CNY",))
            with patch("exchange_rates.providers.discover_currencies", return_value=catalog) as discover:
                result = fetch_day(provider, DAY, None, {}, FakeClient(provider))
                discover.assert_called_once()
                self.assertEqual(result.metadata["pair_count"], 2)
                discover.reset_mock()
                fetch_day(provider, DAY, catalog, {}, FakeClient(provider))
                discover.assert_not_called()

    def test_target_requires_date_not_datetime(self):
        client = FakeClient("visa")
        with self.assertRaises(TypeError):
            fetch_day("visa", datetime(2026, 3, 27), CURRENCIES, {}, client)
        self.assertEqual(client.calls, [])


def official_data(provider, params):
    if provider == "unionpay":
        return {"curDate": "2026-09-30", "exchangeRateJson": [
            {"transCur": "USD", "baseCur": "CNY", "rateData": Decimal("6.7")},
        ]}
    if provider == "visa":
        return {"conversionInputDate": "09/30/2026", "status": "success", "originalValues": {
            "asOfDate": 1790726400, "fromCurrency": params["toCurr"],
            "toCurrency": params["fromCurr"], "fxRateVisa": "6.707775687",
            "toAmountWithVisaRate": "6707.775687",
        }}
    return {"data": {
        "fxDate": "2026-09-30", "transCurr": params["transaction_currency"],
        "crdhldBillCurr": params["cardholder_billing_currency"],
        "errorCode": 0, "conversionRate": Decimal("6.707775687"),
        "crdhldBillAmt": Decimal("6707.775687"),
    }}


def http_response(status=200, body=b'{"rate":1.234567890123456789012345678901}',
                  headers=None, url="https://example.test/rates"):
    return SimpleNamespace(status_code=status, content=body, text=body.decode(),
                           url=url,
                           headers={"Content-Type": "application/json", **(headers or {})})


VISA_SITES = ("https://usa.visa.com", "https://www.visa.com.sg", "https://www.visa.co.in")
VISA_RATES = "https://usa.visa.com/cmsapi/fx/rates"
VISA_CATALOG = ("https://usa.visa.com/support/consumer/travel-support/"
                "exchange-rate-calculator.html")


def visa_catalog_html(codes):
    content = simplejson.dumps({"currencyList": [{"key": code} for code in codes]})
    return '<dm-calculator content="' + escape(content, quote=True) + '"></dm-calculator>'


def visa_rate_text(params):
    return simplejson.dumps({"status": "success", "originalValues": {
        "fxRateVisa": "6.707775687", "toAmountWithVisaRate": "6707.775687",
        "exchangedate": DAY.strftime("%m/%d/%Y"),
        "fromCurrency": params["toCurr"], "toCurrency": params["fromCurr"],
    }})


def site_get(log, *, failed=(), retry_after=None, on_request=None):
    def get(url, params=None, headers=None, **kwargs):
        if on_request is not None:
            on_request(url)
        log.append((url, dict(params or {}), dict(headers or {})))
        host = urlsplit(url).hostname
        if host in failed:
            headers = {} if retry_after is None else {"Retry-After": retry_after}
            return http_response(520, b"down", headers, url=url)
        if urlsplit(url).path.endswith("calculator.html"):
            return http_response(200, visa_catalog_html(("USD", "CNY")).encode(), url=url)
        return http_response(200, visa_rate_text(params).encode(), url=url)
    return get


class HttpTests(unittest.TestCase):
    def test_session_reuse_and_decimal_body(self):
        with patch("exchange_rates.http.requests.Session") as factory:
            session = factory.return_value
            session.get.return_value = http_response()
            with HttpClient(interval=0) as client:
                first = client.request_json("https://example.test/rates")
                client.request_json("https://example.test/next")
            factory.assert_called_once_with(impersonate="chrome")
            session.close.assert_called_once()
            self.assertEqual(first.data["rate"], Decimal("1.234567890123456789012345678901"))
            self.assertEqual(first.body_text, http_response().text)
            self.assertEqual(first.body, http_response().content)
            self.assertEqual(len(client.records), 2)

    def test_4xx_and_bad_json_are_not_retried(self):
        for status in (400, 401, 403, 404):
            with self.subTest(status=status), patch("exchange_rates.http.requests.Session") as factory:
                factory.return_value.get.return_value = http_response(status)
                with HttpClient(interval=0) as client, self.assertRaises(HttpError) as caught:
                    client.request_json("https://example.test/rates")
                self.assertEqual(caught.exception.status_code, status)
                self.assertEqual(factory.return_value.get.call_count, 1)
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.return_value = http_response(body=b'{bad json')
            with HttpClient(interval=0) as client, self.assertRaises(ResponseDecodeError):
                client.request_json("https://example.test/rates")
            self.assertEqual(factory.return_value.get.call_count, 1)

    def test_bounded_retries_exponential_and_raw_failure(self):
        with patch("exchange_rates.http.requests.Session") as factory, patch("exchange_rates.http.time.sleep") as sleep:
            factory.return_value.get.return_value = http_response(500, b'server broke')
            with HttpClient(interval=0, retries=3) as client, self.assertRaises(HttpError) as caught:
                client.request_json("https://example.test/rates")
            self.assertEqual(factory.return_value.get.call_count, 4)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.5, 1.0, 2.0])
            self.assertEqual(len(caught.exception.records), 4)
            self.assertEqual(caught.exception.response.body_text, "server broke")

    def test_retry_after_seconds_overrides_exponential_for_retryable_statuses(self):
        cases = ((520, "text", "retry-after"), (429, "json", "Retry-After"),
                 (503, "json", "RETRY-AFTER"), (502, "text", "rEtRy-AfTeR"))
        for status, kind, header in cases:
            with self.subTest(status=status, header=header), patch(
                    "exchange_rates.http.requests.Session") as factory, patch(
                    "exchange_rates.http.time.sleep") as sleep:
                factory.return_value.get.return_value = http_response(
                    status, b"server broke", {header: "60"})
                with HttpClient(interval=0, retries=3) as client, self.assertRaises(HttpError) as caught:
                    call = client.request_text if kind == "text" else client.request_json
                    call("https://example.test/rates")
                self.assertEqual(factory.return_value.get.call_count, 4)
                self.assertEqual([c.args[0] for c in sleep.call_args_list], [60.0] * 3)
                self.assertEqual(caught.exception.status_code, status)
                self.assertEqual(len(caught.exception.records), 4)
                self.assertEqual(caught.exception.response.body_text, "server broke")

    def test_retry_after_http_date_uses_frozen_clock(self):
        fixed = datetime(2026, 11, 4, 12, 0, 0, tzinfo=timezone.utc)

        class FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed if tz is not None else fixed.replace(tzinfo=None)

        with patch("exchange_rates.http.datetime", FrozenDatetime), patch(
                "exchange_rates.http.requests.Session") as factory, patch(
                "exchange_rates.http.time.sleep") as sleep:
            factory.return_value.get.return_value = http_response(
                503, headers={"Retry-After": formatdate(fixed.timestamp() + 60, usegmt=True)})
            with HttpClient(interval=0, retries=1) as client, self.assertRaises(HttpError):
                client.request_text("https://example.test/rates")
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [60.0])

    def test_invalid_or_missing_retry_after_falls_back_to_exponential(self):
        cases = (None, "", "abc", "-5", "nan", "inf", "1e400", "Wed, 32 Oct 2015 07:28:00 GMT")
        for value in cases:
            with self.subTest(value=value), patch(
                    "exchange_rates.http.requests.Session") as factory, patch(
                    "exchange_rates.http.time.sleep") as sleep:
                headers = None if value is None else {"Retry-After": value}
                factory.return_value.get.return_value = http_response(503, headers=headers)
                with HttpClient(interval=0, retries=3) as client, self.assertRaises(HttpError):
                    client.request_text("https://example.test/rates")
                self.assertEqual([c.args[0] for c in sleep.call_args_list], [0.5, 1.0, 2.0])

    def test_retry_after_over_bound_fails_without_retrying_early(self):
        for value, retries, sleeps in (("300", 1, [300.0]), ("301", 3, []), ("3600", 3, [])):
            with self.subTest(value=value), patch(
                    "exchange_rates.http.requests.Session") as factory, patch(
                    "exchange_rates.http.time.sleep") as sleep:
                factory.return_value.get.return_value = http_response(
                    503, b"server broke", {"Retry-After": value})
                with HttpClient(interval=0, retries=retries) as client, self.assertRaises(HttpError) as caught:
                    client.request_text("https://example.test/rates")
                self.assertEqual([c.args[0] for c in sleep.call_args_list], sleeps)
                if sleeps:
                    self.assertEqual(factory.return_value.get.call_count, 2)
                else:
                    self.assertEqual(factory.return_value.get.call_count, 1)
                    self.assertEqual(caught.exception.status_code, 503)
                    self.assertEqual(caught.exception.response.body_text, "server broke")
                    self.assertEqual(len(caught.exception.records), 1)

    def test_retry_after_over_bound_is_terminal_on_default_path(self):
        with patch("exchange_rates.http.requests.Session") as factory, patch(
                "exchange_rates.http.time.sleep") as sleep:
            factory.return_value.get.side_effect = [
                http_response(503, b"limited", {"Retry-After": "3600"}),
                http_response(200),
            ]
            with HttpClient(interval=0, edge_dns_servers=["1.1.1.1"]) as client:
                client.edge_rounds = lambda url: iter(
                    [SimpleNamespace(addresses=("8.8.8.8",))])
                with self.assertRaises(HttpError) as caught:
                    client.request_text("https://example.test/rates")
            self.assertEqual(factory.return_value.get.call_count, 1)
            sleep.assert_not_called()
            self.assertEqual(caught.exception.status_code, 503)
            self.assertEqual(caught.exception.response.body_text, "limited")
            self.assertEqual(len(caught.exception.records), 1)

    def test_retry_after_over_bound_is_terminal_after_edge_selection(self):
        with patch("exchange_rates.http.requests.Session") as factory, patch(
                "exchange_rates.http.time.sleep") as sleep:
            factory.return_value.get.side_effect = [
                requests.RequestsError("network"), requests.RequestsError("network"),
                http_response(503, b"limited", {"Retry-After": "3600"}),
                http_response(200),
            ]
            with HttpClient(interval=0, retries=1, edge_dns_servers=["1.1.1.1"]) as client:
                client.edge_rounds = lambda url: iter(
                    [SimpleNamespace(addresses=("8.8.8.8", "9.9.9.9"))])
                with self.assertRaises(HttpError) as caught:
                    client.request_text("https://example.test/rates")
            self.assertEqual(factory.return_value.get.call_count, 3)
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [0.5])
            self.assertEqual(caught.exception.status_code, 503)
            self.assertEqual(caught.exception.response.body_text, "limited")
            self.assertEqual(len(caught.exception.records), 3)

    def test_cancellation_interrupts_retry_after_wait(self):
        cancelled = Event()

        def cancel_on_wait(seconds):
            cancelled.set()
            return True

        with patch("exchange_rates.http.requests.Session") as factory, patch.object(
                cancelled, "wait", side_effect=cancel_on_wait) as wait, patch(
                "exchange_rates.http.time.sleep") as sleep:
            factory.return_value.get.return_value = http_response(
                520, b"down", {"Retry-After": "60"})
            with HttpClient(interval=0, retries=3, cancel_event=cancelled) as client, self.assertRaises(CancelledError):
                client.request_text("https://example.test/rates")
            wait.assert_called_once_with(60.0)
            factory.return_value.get.assert_called_once()
            sleep.assert_not_called()

    def test_transport_error_after_retry_after_does_not_reuse_header(self):
        with patch("exchange_rates.http.requests.Session") as factory, patch(
                "exchange_rates.http.time.sleep") as sleep:
            factory.return_value.get.side_effect = [
                http_response(503, headers={"Retry-After": "60"}),
                *[requests.RequestsError("network")] * 3,
            ]
            with HttpClient(interval=0, retries=3) as client, self.assertRaises(HttpError) as caught:
                client.request_text("https://example.test/rates")
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [60.0, 1.0, 2.0])
            self.assertEqual(factory.return_value.get.call_count, 4)
            self.assertIsNone(caught.exception.status_code)

    def test_text_response_is_complete_utf8_and_never_json_decoded(self):
        body = '<dm-calculator content="&quot;人民币&quot;"></dm-calculator>'.encode("utf-8")
        with patch("exchange_rates.http.requests.Session") as factory, patch("exchange_rates.http.simplejson.loads") as decode:
            factory.return_value.get.return_value = http_response(body=body)
            with HttpClient(interval=0) as client:
                result = client.request_text("https://example.test/catalog", params={"all": "1"},
                                             headers={"Accept": "text/html"})
            decode.assert_not_called()
            self.assertIsNone(result.data)
            self.assertEqual(result.body, body)
            self.assertEqual(result.body_text, body.decode("utf-8"))
            self.assertEqual(result.records[0]["response"]["body_text"], body.decode("utf-8"))
            self.assertEqual(result.records[0]["request"]["params"], {"all": "1"})
            self.assertEqual(len(client.records), 1)

    def test_already_cancelled_client_does_not_send_request(self):
        cancelled = Event()
        cancelled.set()
        with patch("exchange_rates.http.requests.Session") as factory:
            with HttpClient(interval=0, cancel_event=cancelled) as client, self.assertRaises(CancelledError):
                client.request_json("https://example.test/rates")
            factory.return_value.get.assert_not_called()
            self.assertEqual(client.records, [])

    def test_cancellation_interrupts_request_interval(self):
        cancelled = Event()
        def cancel_on_wait(seconds):
            cancelled.set()
            return True
        with patch("exchange_rates.http.requests.Session") as factory, patch.object(cancelled, "wait", side_effect=cancel_on_wait) as wait, patch("exchange_rates.http.time.monotonic", return_value=10.1), patch("exchange_rates.http.time.sleep") as sleep:
            with HttpClient(interval=0.3, cancel_event=cancelled) as client:
                client._last_started = 10.0
                with self.assertRaises(CancelledError):
                    client.request_text("https://example.test/catalog")
            self.assertAlmostEqual(wait.call_args.args[0], 0.2)
            factory.return_value.get.assert_not_called()
            sleep.assert_not_called()

    def test_cancellation_interrupts_transport_and_status_retry_backoff(self):
        for error in (requests.RequestsError("network"), http_response(429), http_response(503)):
            cancelled = Event()
            def cancel_on_wait(seconds):
                cancelled.set()
                return True
            with self.subTest(error=error), patch("exchange_rates.http.requests.Session") as factory, patch.object(cancelled, "wait", side_effect=cancel_on_wait) as wait, patch("exchange_rates.http.time.sleep") as sleep:
                factory.return_value.get.side_effect = [error, http_response()]
                with HttpClient(interval=0, retries=3, cancel_event=cancelled) as client, self.assertRaises(CancelledError):
                    client.request_json("https://example.test/rates")
                self.assertEqual(factory.return_value.get.call_count, 1)
                self.assertEqual(len(client.records), 1)
                wait.assert_called_once_with(0.5)
                sleep.assert_not_called()


class VisaSiteFallbackTests(unittest.TestCase):
    """Configured Visa mirrors: USA first, then Singapore, then India."""

    def client(self, sites=VISA_SITES, **kwargs):
        kwargs.setdefault("retries", 0)
        return HttpClient(interval=0, sites=sites, **kwargs)

    def fetch(self, log, *, sites=VISA_SITES, failed=(), retry_after=None):
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = site_get(
                log, failed=failed, retry_after=retry_after)
            with self.client(sites) as client:
                catalog = discover_currencies("visa", {"referer": VISA_CATALOG}, client)
                row, response = _query_pair(
                    "visa", DAY, "CNY", "USD", VISA_RATES,
                    {"Referer": VISA_CATALOG}, client)
            return catalog, row, response, factory.return_value.get.call_count

    def test_healthy_usa_serves_catalog_and_rates_without_fallback(self):
        log = []
        catalog, row, response, count = self.fetch(log)
        self.assertEqual(count, 2)
        self.assertEqual(catalog.transaction, ("CNY", "USD"))
        self.assertEqual(row["rateData"], Decimal("6.707775687"))
        self.assertEqual([url for url, _, _ in log], [VISA_CATALOG, VISA_RATES])
        self.assertEqual(response.url, VISA_RATES)

    def test_default_config_starts_at_the_lowest_latency_site(self):
        config = load_config(None)["providers"]["visa"]
        first = config["http"]["sites"][0]
        self.assertEqual(first, "https://www.visa.com.sg")
        # Endpoints must follow the first site, or the catalog request starts elsewhere.
        self.assertEqual(config["endpoint"], f"{first}/cmsapi/fx/rates")
        self.assertEqual(config["currency_endpoint"],
                         f"{first}/support/consumer/travel-support/exchange-rate-calculator.html")
        self.assertEqual(config["referer"], config["currency_endpoint"])
        log = []
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = site_get(log)
            settings = http_settings({"http": {}, "providers": {"visa": config}},
                                     SimpleNamespace(provider="visa", interval=None))
            with HttpClient(interval=0, **settings) as client:
                discover_currencies("visa", config, client)
                _query_pair("visa", DAY, "CNY", "USD", config["endpoint"], {}, client)
        self.assertEqual([url for url, _, _ in log], [config["currency_endpoint"], config["endpoint"]])

    def test_fallback_switches_origin_and_keeps_paths_params_and_referer(self):
        log = []
        catalog, row, response, count = self.fetch(log, failed={"usa.visa.com"})
        self.assertEqual(count, 3)
        # Only the first US request fails; the switch is then sticky.
        self.assertEqual([urlsplit(url).hostname for url, _, _ in log],
                         ["usa.visa.com"] + ["www.visa.com.sg"] * 2)
        self.assertEqual(log[1][0], "https://www.visa.com.sg/support/consumer/"
                                    "travel-support/exchange-rate-calculator.html")
        self.assertEqual(log[2][0], "https://www.visa.com.sg/cmsapi/fx/rates")
        self.assertEqual(log[2][1]["fromCurr"], "USD")
        self.assertEqual(log[2][1]["toCurr"], "CNY")
        self.assertTrue(log[1][2]["Referer"].startswith("https://www.visa.com.sg/"))
        self.assertTrue(log[2][2]["Referer"].startswith("https://www.visa.com.sg/"))
        self.assertEqual(row["rateData"], Decimal("6.707775687"))
        self.assertEqual(response.url, "https://www.visa.com.sg/cmsapi/fx/rates")
        self.assertEqual(catalog.transaction, ("CNY", "USD"))

    def test_transport_failure_also_switches_site(self):
        log = []
        with patch("exchange_rates.http.requests.Session") as factory, patch(
                "exchange_rates.http.time.sleep"):
            healthy = site_get(log)
            failures = [requests.RequestsError("network"), requests.RequestsError("network")]

            def get(url, *args, **kwargs):
                if failures:
                    raise failures.pop(0)
                return healthy(url, *args, **kwargs)

            factory.return_value.get.side_effect = get
            with self.client(retries=1) as client:
                discover_currencies("visa", {}, client)
                row, response = _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, {}, client)
        self.assertEqual([urlsplit(url).hostname for url, _, _ in log],
                         ["www.visa.com.sg"] * 2)
        self.assertEqual(response.url, "https://www.visa.com.sg/cmsapi/fx/rates")

    def test_validation_errors_4xx_and_cancellation_never_switch_sites(self):
        payload = simplejson.loads(visa_rate_text({"toCurr": "CNY", "fromCurr": "USD"}))
        payload["originalValues"]["exchangedate"] = "09/29/2026"
        cases = (
            ("403", http_response(403, b"blocked", url=VISA_RATES)),
            ("429", http_response(429, b"blocked", url=VISA_RATES)),
            ("404", http_response(404, b"missing", url=VISA_RATES)),
            ("bad-json", http_response(200, b"{bad", url=VISA_RATES)),
            ("date-mismatch", http_response(200, simplejson.dumps(payload).encode(),
                                            url=VISA_RATES)),
        )
        for label, bad in cases:
            with self.subTest(label=label), patch(
                    "exchange_rates.http.requests.Session") as factory:
                log = []

                def get(url, params=None, headers=None, **kwargs):
                    log.append((url, dict(params or {}), dict(headers or {})))
                    return bad

                factory.return_value.get.side_effect = get
                with self.client() as client:
                    with self.assertRaises((HttpError, ProviderError)):
                        _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, {}, client)
                self.assertEqual([urlsplit(url).hostname for url, _, _ in log],
                                 ["usa.visa.com"])
        cancelled = Event()
        cancelled.set()
        with patch("exchange_rates.http.requests.Session") as factory:
            with self.client(cancel_event=cancelled) as client, \
                    self.assertRaises(CancelledError):
                _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, {}, client)
            factory.return_value.get.assert_not_called()

    def test_terminal_retry_after_cap_stops_fallback(self):
        # With retries=0, a 5xx is already terminal; both cap and last-response
        # limits live in _request_at. Reuse the cap regression shape here.
        with patch("exchange_rates.http.requests.Session") as factory:
            log = []
            factory.return_value.get.side_effect = site_get(
                log, failed={"usa.visa.com"}, retry_after="3600")
            with self.client() as client:
                with self.assertRaises(HttpError) as caught:
                    _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, {}, client)
            self.assertEqual(caught.exception.status_code, 520)
            self.assertEqual([urlsplit(url).hostname for url, _, _ in log], ["usa.visa.com"])

    def test_last_response_retry_after_is_honored_before_next_site(self):
        with patch("exchange_rates.http.requests.Session") as factory, patch(
                "exchange_rates.http.time.sleep") as sleep:
            log = []
            factory.return_value.get.side_effect = site_get(
                log, failed={"usa.visa.com"}, retry_after="60")
            with self.client(retries=1) as client:
                discover_currencies("visa", {}, client)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [60.0, 60.0])
            self.assertEqual([urlsplit(url).hostname for url, _, _ in log],
                             ["usa.visa.com", "usa.visa.com", "www.visa.com.sg"])

    def test_all_sites_failing_keeps_every_record(self):
        log = []
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = site_get(
                log, failed={urlsplit(site).hostname for site in VISA_SITES})
            with self.client() as client:
                with self.assertRaises(HttpError) as caught:
                    _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, {}, client)
        self.assertEqual([url for url, _, _ in log],
                         [f"{site}/cmsapi/fx/rates" for site in VISA_SITES])
        self.assertEqual(len(caught.exception.records), 3)
        self.assertEqual(caught.exception.status_code, 520)

    def test_selected_site_is_sticky_within_run_and_shared_across_forks(self):
        log = []
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = site_get(log, failed={"usa.visa.com"})
            with self.client() as client:
                discover_currencies("visa", {}, client)
                with client.fork() as child:
                    for spend, home in (("CNY", "USD"), ("USD", "CNY")):
                        _query_pair("visa", DAY, spend, home, VISA_RATES, {}, child)
                _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, {}, client)
        self.assertEqual(factory.return_value.get.call_count, 5)
        self.assertEqual([urlsplit(url).hostname for url, _, _ in log],
                         ["usa.visa.com"] + ["www.visa.com.sg"] * 4)

    def test_fresh_client_starts_again_at_usa(self):
        for _ in range(2):
            log = []
            with patch("exchange_rates.http.requests.Session") as factory:
                factory.return_value.get.side_effect = site_get(log)
                with self.client() as client:
                    _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, {}, client)
            self.assertEqual([urlsplit(url).hostname for url, _, _ in log], ["usa.visa.com"])

    def test_invalid_site_configuration_is_rejected_before_any_request(self):
        invalid = (
            ["https://usa.visa.com", "http://www.visa.com.sg"],
            ["https://user:pw@usa.visa.com"],
            ["https://usa.visa.com/cmsapi"],
            ["https://usa.visa.com?x=1"],
            ["https://usa.visa.com#fragment"],
            "https://usa.visa.com",
            [],
            ["https://usa.visa.com", "https://usa.visa.com"],
            ["usa.visa.com"],
        )
        for sites in invalid:
            with self.subTest(sites=sites), patch(
                    "exchange_rates.http.requests.Session") as factory:
                factory.return_value.get.side_effect = site_get([])
                with self.assertRaises(ValueError):
                    self.client(sites)
                factory.return_value.get.assert_not_called()

    def test_unconfigured_endpoint_is_never_rewritten(self):
        log = []
        foreign = "https://elsewhere.test/cmsapi/fx/rates"
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = site_get(log, failed={"elsewhere.test"})
            with self.client() as client, self.assertRaises(HttpError):
                _query_pair("visa", DAY, "CNY", "USD", foreign, {}, client)
        self.assertEqual([urlsplit(url).hostname for url, _, _ in log], ["elsewhere.test"])

    def test_credentials_are_not_forwarded_across_origins(self):
        log = []
        headers = {"Referer": VISA_CATALOG, "Authorization": "Bearer secret",
                   "Cookie": "session=secret", "Accept": "text/html"}
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = site_get(log, failed={"usa.visa.com"})
            with self.client() as client:
                _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, headers,
                            client)
        self.assertEqual(factory.return_value.get.call_count, 2)
        self.assertEqual(log[0][1], log[1][1])
        self.assertEqual(log[1][0], "https://www.visa.com.sg/cmsapi/fx/rates")
        self.assertNotIn("Authorization", log[1][2])
        self.assertNotIn("Cookie", log[1][2])
        self.assertEqual(log[1][2]["Referer"],
                         "https://www.visa.com.sg/support/consumer/"
                         "travel-support/exchange-rate-calculator.html")
        self.assertEqual(log[0][2]["Authorization"], "Bearer secret")
        self.assertEqual(log[0][2]["Cookie"], "session=secret")

    def test_concurrent_selection_is_monotonic_and_not_duplicated(self):
        log = []
        with patch("exchange_rates.http.requests.Session") as factory:
            worker = None
            responses = iter([http_response(520, b"down", url=VISA_RATES),
                              http_response(200, visa_rate_text(
                                  {"toCurr": "CNY", "fromCurr": "USD"}).encode(),
                                  url="https://www.visa.co.in/cmsapi/fx/rates")])

            def get(url, *args, **kwargs):
                log.append((url, dict(kwargs.get("headers") or {})))
                if len(log) == 1:
                    # A sibling worker advances the shared site mid-request.
                    worker._select_site(VISA_SITES[2])
                    return next(responses)
                return next(responses)

            factory.return_value.get.side_effect = get
            with self.client() as client:
                worker = client.fork()
                row, response = _query_pair("visa", DAY, "CNY", "USD", VISA_RATES,
                                            {"Referer": VISA_CATALOG}, worker)
        self.assertEqual([urlsplit(url).hostname for url, _ in log],
                         ["usa.visa.com", "www.visa.co.in"])
        self.assertEqual(row["rateData"], Decimal("6.707775687"))
        self.assertEqual(response.url, "https://www.visa.co.in/cmsapi/fx/rates")

    def test_stale_pending_does_not_retry_india_when_it_was_skipped(self):
        log = []
        with patch("exchange_rates.http.requests.Session") as factory:
            responses = iter([http_response(520, b"down", url=VISA_RATES),
                              http_response(520, b"down",
                                            url="https://www.visa.co.in/cmsapi/fx/rates")])

            def get(url, *args, **kwargs):
                log.append((url, dict(kwargs.get("headers") or {})))
                if len(log) == 1:
                    # Shared selection jumps to India; Singapore must be skipped.
                    worker._select_site(VISA_SITES[2])
                return next(responses)

            factory.return_value.get.side_effect = get
            with self.client() as client:
                worker = client.fork()
                with self.assertRaises(HttpError):
                    _query_pair("visa", DAY, "CNY", "USD", VISA_RATES,
                                {"Referer": VISA_CATALOG}, worker)
        self.assertEqual([urlsplit(url).hostname for url, _ in log],
                         ["usa.visa.com", "www.visa.co.in"])

    def test_cross_origin_switch_strips_explicit_host(self):
        log = []
        headers = {"Referer": VISA_CATALOG, "Host": "usa.visa.com", "Accept": "text/html"}
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = site_get(log, failed={"usa.visa.com"})
            with self.client() as client:
                _query_pair("visa", DAY, "CNY", "USD", VISA_RATES, headers,
                            client)
        self.assertEqual(factory.return_value.get.call_count, 2)
        self.assertEqual(log[0][2]["Host"], "usa.visa.com")
        self.assertNotIn("Host", log[1][2])
        self.assertEqual(log[1][0], "https://www.visa.com.sg/cmsapi/fx/rates")
        self.assertTrue(log[1][2]["Referer"].startswith("https://www.visa.com.sg/"))


if __name__ == "__main__":
    unittest.main()
