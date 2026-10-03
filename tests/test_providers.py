"""Offline provider and HTTP contracts, including precision and atomic days."""

from datetime import date
from decimal import Decimal
from itertools import permutations
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from curl_cffi import requests
import simplejson

from exchange_rates.http import HttpClient, HttpError, JsonResponse, ResponseDecodeError
from exchange_rates.models import DayResult, json_bytes
from exchange_rates.providers import (
    DateMismatchError, NoDataError, ProviderError, fetch_day, provider_ids,
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

    def request_json(self, url, params=None, headers=None):
        self.calls.append((url, dict(params or {}), dict(headers or {})))
        if len(self.calls) == self.fail_at:
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


class ProviderTests(unittest.TestCase):
    def test_registry(self):
        self.assertEqual(provider_ids(), ("unionpay", "visa", "mastercard"))
        with self.assertRaises(ValueError):
            fetch_day("unknown", DAY, CURRENCIES, {}, FakeClient("visa"))

    def test_decimal_json_is_number_and_exact(self):
        number = Decimal("0.123456789012345678901234567890123456789")
        encoded = json_bytes({"rate": number, "label": "人民币"})
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
                self.assertEqual(set(data), {"exchangeRateJson"})
                rows = data["exchangeRateJson"]
                self.assertEqual(len(rows), 110)
                self.assertEqual(len(client.calls), 110)
                self.assertEqual({(r["transCur"], r["baseCur"]) for r in rows},
                                 set(permutations(CURRENCIES, 2)))
                for row, (_, params, headers) in zip(rows, client.calls):
                    spend, home = row["transCur"], row["baseCur"]
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
                raw = simplejson.loads(result.raw, use_decimal=True)
                self.assertEqual(len(raw["requests"]), 110)
                self.assertEqual(raw["requests"][0]["response"]["body_text"],
                                 response(client_response(provider)).body_text)

    def test_one_failure_aborts_day(self):
        for provider in ("visa", "mastercard"):
            client = FakeClient(provider, fail_at=7)
            with self.subTest(provider=provider), self.assertRaises(HttpError):
                fetch_day(provider, DAY, CURRENCIES, {}, client)
            self.assertEqual(len(client.calls), 7)

    def test_missing_required_fields_fail(self):
        for provider, container, keys in (
            ("visa", "originalValues", ("fxRateVisa", "toAmountWithVisaRate")),
            ("mastercard", "data", ("conversionRate", "crdhldBillAmt")),
        ):
            for key in keys:
                client = FakeClient(provider, mutate=lambda obj, c=container, k=key: obj[c].pop(k))
                with self.subTest(provider=provider, missing=key), self.assertRaises(ProviderError):
                    fetch_day(provider, DAY, CURRENCIES, {}, client)
                self.assertEqual(len(client.calls), 1)

    def test_date_mismatch_and_invalid_date(self):
        for provider, container, field in (
            ("visa", "originalValues", "exchangedate"),
            ("mastercard", "data", "exchange_date"),
        ):
            for returned, exception in (("2026-03-26", DateMismatchError), ("not-a-date", ProviderError)):
                client = FakeClient(provider, mutate=lambda obj, c=container, f=field, v=returned: obj[c].update({f: v}))
                with self.subTest(provider=provider, returned=returned), self.assertRaises(exception):
                    fetch_day(provider, DAY, CURRENCIES, {}, client)
                self.assertEqual(len(client.calls), 1)

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
            self.assertEqual(len(client.calls), 1)

    def test_rounding_is_allowed(self):
        for provider, container, field in (
            ("visa", "originalValues", "toAmountWithVisaRate"),
            ("mastercard", "data", "crdhldBillAmt"),
        ):
            client = FakeClient(provider, mutate=lambda d, c=container, f=field: d[c].update({f: "1235" if Decimal(str(d[c].get("fxRateVisa", d[c].get("conversionRate")))) > 1 else "988"}))
            result = fetch_day(provider, DAY, ("CNY", "USD"), {}, client)
            self.assertEqual(result.metadata["pair_count"], 2)

    def test_unionpay_full_original_bytes_and_raw_envelope(self):
        body = (b'{ "date":"20260327", "extra":{"source":"complete"}, "exchangeRateJson":['
                b'{"transCur":"USD","baseCur":"CNY","rateData":7.1234567890123456789012345678901},'
                b'{"transCur":"EUR","baseCur":"JPY","rateData":161.987654321}]}\n')
        data = simplejson.loads(body, use_decimal=True)
        client = Mock()
        client.request_json.return_value = response(data, body=body)
        result = fetch_day("unionpay", DAY, ("USD", "CNY"), {}, client)
        self.assertEqual(result.normalized, body)
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
        for provider in provider_ids():
            client = Mock()
            client.request_json.side_effect = lambda url, params=None, headers=None, p=provider: response(
                official_data(p, params), url, params, headers,
            )
            result = fetch_day(provider, target, ("USD", "CNY"), {}, client)
            self.assertEqual(result.requested_date, target)
            if provider != "unionpay":
                self.assertEqual(set(simplejson.loads(result.normalized)), {"exchangeRateJson"})
            else:
                self.assertEqual(simplejson.loads(result.normalized)["curDate"], "2026-09-30")

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
            client = Mock()
            client.request_json.side_effect = altered
            with self.subTest(provider=provider, field=field), self.assertRaises(DateMismatchError):
                fetch_day(provider, date(2026, 9, 30), ("USD", "CNY"), {}, client)
            self.assertEqual(client.request_json.call_count, 1)

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
                client = Mock()
                client.request_json.side_effect = altered
                with self.subTest(provider=provider, field=field), self.assertRaises(ProviderError):
                    fetch_day(provider, date(2026, 9, 30), ("USD", "CNY"), {}, client)
                self.assertEqual(client.request_json.call_count, 1)

    def test_publication_and_current_dates_are_not_rate_dates(self):
        for provider in provider_ids():
            def actual_with_metadata(url, params=None, headers=None):
                data = official_data(provider, params)
                data.update(currentDate="2026-10-02", publicationDate="2026-10-01")
                container = "originalValues" if provider == "visa" else "data"
                if provider != "unionpay":
                    data[container].update(currentDate="2026-10-02", publicationDate="2026-10-01")
                return response(data, url, params, headers)
            client = Mock()
            client.request_json.side_effect = actual_with_metadata
            result = fetch_day(provider, date(2026, 9, 30), ("USD", "CNY"), {}, client)
            self.assertEqual(result.requested_date, date(2026, 9, 30))

    def test_visa_unix_date_uses_utc_and_rejects_invalid_types(self):
        for timestamp in (True, "1790726400", None, 10 ** 30):
            def altered(url, params=None, headers=None):
                data = official_data("visa", params)
                data["originalValues"]["asOfDate"] = timestamp
                return response(data, url, params, headers)
            client = Mock()
            client.request_json.side_effect = altered
            with self.subTest(timestamp=timestamp), self.assertRaises(ProviderError):
                fetch_day("visa", date(2026, 9, 30), ("USD", "CNY"), {}, client)

    def test_custom_institution_config(self):
        client = FakeClient("visa")
        fetch_day("visa", DAY, ("USD", "CNY"),
                  {"endpoint": "https://local.test/fx", "referer": "https://local.test/calc"}, client)
        self.assertEqual(client.calls[0][0], "https://local.test/fx")
        self.assertEqual(client.calls[0][2]["Referer"], "https://local.test/calc")


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


def client_response(provider):
    client = FakeClient(provider)
    if provider == "visa":
        return client.request_json("ignored", {"toCurr": "CNY", "fromCurr": "USD"}).data
    return client.request_json("ignored", {"transaction_currency": "CNY", "cardholder_billing_currency": "USD"}).data


def http_response(status=200, body=b'{"rate":1.234567890123456789012345678901}'):
    return SimpleNamespace(status_code=status, content=body, text=body.decode(),
                           url="https://example.test/rates", headers={"Content-Type": "application/json"})


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

    def test_retry_transport_429_and_5xx_only(self):
        for error in (requests.RequestsError("network"), http_response(429), http_response(503)):
            with self.subTest(error=error), patch("exchange_rates.http.requests.Session") as factory, patch("exchange_rates.http.time.sleep") as sleep:
                factory.return_value.get.side_effect = [error, http_response()]
                with HttpClient(interval=0, retries=1) as client:
                    result = client.request_json("https://example.test/rates")
                self.assertEqual(factory.return_value.get.call_count, 2)
                self.assertEqual(len(result.records), 2)
                sleep.assert_called_once_with(0.5)

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

    def test_request_interval_applies_between_attempts(self):
        with patch("exchange_rates.http.requests.Session") as factory, patch("exchange_rates.http.time.sleep") as sleep, patch("exchange_rates.http.time.monotonic", side_effect=[10.0, 10.1, 10.3]):
            factory.return_value.get.return_value = http_response()
            with HttpClient(interval=0.3) as client:
                client.request_json("https://example.test/a")
                client.request_json("https://example.test/b")
            self.assertAlmostEqual(sleep.call_args.args[0], 0.2)


if __name__ == "__main__":
    unittest.main()
