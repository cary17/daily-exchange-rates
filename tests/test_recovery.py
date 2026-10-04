"""Bounded, barrier-separated recovery contracts using offline transports."""

from collections import Counter
from concurrent.futures import CancelledError
import unittest
from unittest.mock import patch

import simplejson

from exchange_rates.catalog import CurrencyCatalog
from exchange_rates.http import HttpClient, HttpError
from exchange_rates.models import json_bytes
from exchange_rates.providers import RecoveryError, fetch_day
from tests.test_providers import (
    CURRENCIES, DAY, FakeClient, call_pair, http_response, response,
)


class RecoveryTests(unittest.TestCase):
    def test_transient_first_and_middle_failures_only_retry_after_full_round(self):
        for provider in ("visa", "mastercard"):
            for fail_at in (1, 57):
                client = FakeClient(provider, fail_at=fail_at)
                original = client.request_json

                def checked(*args, **kwargs):
                    if len(client.calls) >= 110:
                        self.assertEqual(len(client.calls), 110)
                        self.assertTrue(all(child.closed for child in client.forks[:9]))
                    return original(*args, **kwargs)

                client.request_json = checked
                with self.subTest(provider=provider, fail_at=fail_at):
                    result = fetch_day(provider, DAY, CURRENCIES, {}, client)
                    pairs = [call_pair(provider, p) for _, p, _ in client.calls]
                    self.assertEqual(len(set(pairs[:110])), 110)
                    self.assertEqual(pairs[-1], pairs[fail_at - 1])
                    self.assertEqual(Counter(Counter(pairs).values()), {1: 109, 2: 1})
                    self.assertEqual(len(client.forks), 18)
                    rows = simplejson.loads(result.normalized)["exchangeRateJson"]
                    self.assertEqual([(r["transCur"], r["baseCur"]) for r in rows],
                                     list(CurrencyCatalog(CURRENCIES, CURRENCIES).pairs()))

    def test_persistent_precise_missing_pairs_and_no_result(self):
        client = FakeClient("visa")
        original = client.request_json
        missing = ("CNY", "USD")

        def broken(url, params=None, headers=None):
            result = original(url, params, headers)
            if call_pair("visa", params) == missing:
                raise HttpError("persistent 503", 503)
            return result

        client.request_json = broken
        with self.assertRaises(RecoveryError) as caught:
            fetch_day("visa", DAY, CURRENCIES, {}, client)
        error = caught.exception
        self.assertEqual(error.missing_pairs, [missing])
        self.assertEqual(error.errors, {missing: "HttpError: persistent 503"})
        self.assertIn("CNY/USD", str(error))
        self.assertEqual(error.recovery, {"initial_failed": 1, "rounds": 2, "repaired": 0})
        self.assertEqual(len(client.calls), 112)
        self.assertEqual(len(error.raw_parts), 9)

    def test_real_http_failed_validation_and_duplicate_attempt_evidence(self):
        for provider, container, date_field in (
            ("visa", "originalValues", "exchangedate"),
            ("mastercard", "data", "exchange_date"),
        ):
            for defect in ("date", "rate", "direction", "json", "http"):
                calls = Counter()
                fixture = FakeClient(provider)
                catalog = CurrencyCatalog(("USD",), ("CNY",))

                def request(url, params=None, headers=None, **kwargs):
                    pair = call_pair(provider, params)
                    calls[pair] += 1
                    data = fixture.request_json(url, params, headers).data
                    if calls[pair] == 1:
                        if defect == "date":
                            data[container][date_field] = "2026-03-26"
                        elif defect == "rate":
                            data[container].pop("fxRateVisa" if provider == "visa" else "conversionRate")
                        elif defect == "direction":
                            data[container]["fromCurrency" if provider == "visa" else "transCurr"] = "EUR"
                        elif defect == "json":
                            return http_response(body=b"not json")
                        else:
                            return http_response(503, b"failed HTTP evidence")
                    return http_response(body=json_bytes(data))

                with self.subTest(provider=provider, defect=defect), patch("exchange_rates.http.requests.Session") as factory:
                    factory.return_value.get.side_effect = request
                    result = fetch_day(provider, DAY, catalog, {}, HttpClient(retries=0, interval=0))
                raw = simplejson.loads(result.raw_parts["shard-01.json"])
                self.assertEqual(len(raw["requests"]), 2)
                self.assertIsNotNone(raw["attempts"][0]["error"])
                self.assertIsNone(raw["attempts"][1]["error"])
                self.assertEqual([a["round"] for a in raw["attempts"]], [0, 1])
                self.assertEqual(result.metadata["recovery"]["repaired"], 1)
                if defect == "date":
                    self.assertIn("2026-03-26", raw["requests"][0]["response"]["body_text"])
                    self.assertNotIn("2026-03-26", str(result.metadata["response_dates"]))
                if defect == "http":
                    self.assertEqual(raw["requests"][0]["response"]["status_code"], 503)

        good = fixture.request_json("x", {"transaction_currency": "USD", "cardholder_billing_currency": "CNY"}).data
        with patch("exchange_rates.http.requests.Session") as factory, patch("exchange_rates.http.time.sleep"), patch("exchange_rates.http.utc_now", return_value="fixed"):
            factory.return_value.get.side_effect = [http_response(503, b"same"), http_response(503, b"same"), http_response(body=json_bytes(good))]
            result = fetch_day("mastercard", DAY, catalog, {}, HttpClient(retries=1, interval=0))
        records = simplejson.loads(result.raw_parts["shard-01.json"])["requests"]
        self.assertEqual(len(records), 3)
        self.assertEqual([r["response"]["status_code"] for r in records], [503, 503, 200])
        with patch("exchange_rates.http.requests.Session") as factory, patch("exchange_rates.http.time.sleep"):
            factory.return_value.get.side_effect = [http_response(503, b"retry within successful query"), http_response(body=json_bytes(good))]
            result = fetch_day("mastercard", DAY, catalog, {}, HttpClient(retries=1, interval=0))
        records = simplejson.loads(result.raw_parts["shard-01.json"])["requests"]
        self.assertEqual([r["response"]["status_code"] for r in records], [503, 200])
        self.assertEqual(result.metadata["recovery"]["rounds"], 0)

    def test_round_limit_zero_and_invalid_configs(self):
        client = FakeClient("visa", fail_at=1)
        with self.assertRaises(RecoveryError) as caught:
            fetch_day("visa", DAY, CURRENCIES, {"recovery_rounds": 0}, client)
        self.assertEqual(len(client.calls), 110)
        self.assertEqual(caught.exception.recovery["rounds"], 0)
        for value in (-1, True, 1.5, "2", None):
            for provider in ("visa", "mastercard", "unionpay"):
                with self.subTest(provider=provider, value=value), self.assertRaises(ValueError):
                    fetch_day(provider, DAY, CURRENCIES, {"recovery_rounds": value}, FakeClient(provider))

    def test_interrupts_and_programming_errors_propagate(self):
        for error in (KeyboardInterrupt(), BaseException("stop"), CancelledError(), TypeError("bug"), RuntimeError("bug")):
            client = FakeClient("visa")
            with patch.object(client, "request_json", side_effect=error):
                with self.subTest(error=type(error).__name__), self.assertRaises(type(error)):
                    fetch_day("visa", DAY, CURRENCIES, {}, client)
            self.assertLessEqual(len(client.forks), 9)
            self.assertTrue(all(child.closed for child in client.forks))

    def test_unionpay_whole_file_recovery_and_strict_dates(self):
        from unittest.mock import Mock
        good = {"date": "20260327", "exchangeRateJson": [
            {"transCur": "USD", "baseCur": "CNY", "rateData": 7}]}
        wrong = dict(good, date="20260326")
        client = Mock(records=[])
        client.request_json.side_effect = [response(wrong), response(good)]
        result = fetch_day("unionpay", DAY, (), {}, client)
        self.assertEqual(result.normalized, json_bytes(good))
        self.assertEqual(client.request_json.call_count, 2)
        self.assertEqual(len(simplejson.loads(result.raw)["requests"]), 2)
        self.assertEqual(result.metadata["recovery"],
                         {"initial_failed": 1, "rounds": 1, "repaired": 1, "unit": "file"})


if __name__ == "__main__":
    unittest.main()
