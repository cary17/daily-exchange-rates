"""Offline contracts for the complete official calculator currency catalogs."""

from html import escape
from itertools import product
import unittest
from unittest.mock import Mock

import simplejson

from exchange_rates.catalog import (
    CatalogError, CurrencyCatalog, discover_currencies,
    parse_mastercard_currencies, parse_visa_currencies,
)
from exchange_rates.http import JsonResponse
from exchange_rates.models import json_bytes


VISA_URL = "https://www.visa.co.in/support/consumer/travel-support/exchange-rate-calculator.html"
MC_URL = "https://www.mastercard.com/marketingservices/public/mccom-services/currency-conversions/currencies"


def visa_html(codes):
    content = {
        "currencyList": [{"key": code, "value": f"Currency {code} & historical"} for code in codes],
        "mostUsedCurrencyList": [{"key": "ZZZ", "value": "Not the complete catalog"}],
    }
    return '<dm-calculator content="' + escape(simplejson.dumps(content), quote=True) + '"></dm-calculator>'


def mc_data(codes):
    return {"data": {"currencies": [{"alphaCd": code, "currNam": f"Currency {code}"} for code in codes]}}


def source_response(data=None, text=None):
    body = text.encode("utf-8") if text is not None else json_bytes(data)
    return JsonResponse(data=data, body_text=body.decode("utf-8"), body=body,
                        url="https://official.test/catalog")


class CatalogTests(unittest.TestCase):
    def test_direction_sets_are_independent_and_identity_is_excluded(self):
        catalog = CurrencyCatalog(["USD", "CNY", "SKK"], ["USD", "EUR"])
        expected = [(spend, home) for spend, home in product(catalog.transaction, catalog.billing)
                    if spend != home]
        self.assertEqual(list(catalog.pairs()), expected)
        self.assertEqual(catalog.pair_count, 5)
        self.assertEqual(catalog.transaction, ("USD", "CNY", "SKK"))
        self.assertEqual(catalog.billing, ("USD", "EUR"))
        self.assertIn(("SKK", "EUR"), expected)

    def test_invalid_empty_duplicate_or_identity_only_catalog_fails(self):
        for transaction, billing in (
            ((), ("USD",)), (("USD",), ()), (("USD",), ("USD",)),
            (("USD", "USD"), ("EUR",)), (("EUR",), ("USD", "USD")),
            ("USD", ("EUR",)), (("usd",), ("EUR",)), ((None,), ("EUR",)),
            (("USD\n",), ("EUR",)), (("USD",), (True,)),
        ):
            with self.subTest(transaction=transaction, billing=billing), self.assertRaises(CatalogError):
                CurrencyCatalog(transaction, billing)

    def test_visa_entity_encoded_content_uses_full_list_and_preserves_history(self):
        html = visa_html(("USD", "None", "SKK", "CNY", "VEF"))
        self.assertIn("&quot;currencyList&quot;", html)
        self.assertEqual(parse_visa_currencies(html), ("CNY", "SKK", "USD", "VEF"))
        single_quoted = html.replace('content="', "content='").replace('"></dm-calculator>', "'></dm-calculator>")
        self.assertEqual(parse_visa_currencies(single_quoted), ("CNY", "SKK", "USD", "VEF"))

    def test_visa_malformed_duplicate_empty_and_ambiguous_lists_fail(self):
        for html in (
            "<p>No calculator</p>", '<dm-calculator content="{broken}"></dm-calculator>',
            visa_html(("None",)), visa_html(("USD", "USD")), visa_html(("usd",)),
            visa_html(("USD",)) + visa_html(("EUR",)),
            '<dm-calculator content="' + escape('{"mostUsedCurrencyList": [{"key": "USD"}]}') + '"></dm-calculator>',
            '<dm-calculator content="' + escape('{"currencyList": [{"value": "USD"}]}') + '"></dm-calculator>',
            '<dm-calculator content="' + escape('{"currencyList": {"key": "USD"}}') + '"></dm-calculator>',
        ):
            with self.subTest(html=html), self.assertRaises(CatalogError):
                parse_visa_currencies(html)

    def test_mastercard_alpha_codes_succeed_without_error_code(self):
        data = mc_data(("USD", "SKK", "CNY", "VEF"))
        self.assertNotIn("errorCode", data["data"])
        for code in (None, "", 0, "0"):
            with self.subTest(errorCode=code):
                if code is not None:
                    data["data"]["errorCode"] = code
                self.assertEqual(parse_mastercard_currencies(data), ("CNY", "SKK", "USD", "VEF"))

    def test_mastercard_malformed_duplicate_empty_or_error_fails(self):
        for data in (
            None, [], {}, {"data": []}, {"data": {}}, {"data": {"currencies": {}}},
            mc_data(()), mc_data(("USD", "USD")), mc_data(("usd",)),
            {"data": {"currencies": [{"currNam": "USD"}]}},
            {"data": {"currencies": [{"alphaCd": None}]}},
            {"data": {"currencies": ["USD"]}},
            {"data": {"currencies": [{"alphaCd": "USD"}], "errorCode": "NO_DATA"}},
        ):
            with self.subTest(data=data), self.assertRaises(CatalogError):
                parse_mastercard_currencies(data)

    def test_discovery_uses_official_endpoints_and_each_own_full_catalog(self):
        client = Mock(spec=["request_text", "request_json"])
        visa_source = source_response(text=visa_html(("None", "USD", "SKK", "CNY")))
        mc_source = source_response(mc_data(("USD", "EUR")))
        client.request_text.return_value = visa_source
        client.request_json.return_value = mc_source
        visa = discover_currencies("visa", {}, client)
        mc = discover_currencies("mastercard", {}, client)
        headers = {"Accept": "application/json, text/plain, */*"}
        client.request_text.assert_called_once_with(VISA_URL, headers=headers)
        client.request_json.assert_called_once_with(MC_URL, headers=headers)
        self.assertEqual(visa.transaction, ("CNY", "SKK", "USD"))
        self.assertEqual(mc.transaction, ("EUR", "USD"))
        self.assertEqual(visa.billing, visa.transaction)
        self.assertEqual(mc.billing, mc.transaction)
        self.assertEqual(visa.responses, (visa_source,))
        self.assertEqual(mc.responses, (mc_source,))
        self.assertIsNot(visa, mc)
        self.assertNotIn("SKK", mc.transaction)
        self.assertNotIn("EUR", visa.transaction)

    def test_custom_discovery_endpoint_and_referer(self):
        for provider in ("visa", "mastercard"):
            client = Mock(spec=["request_text", "request_json"])
            client.request_text.return_value = source_response(text=visa_html(("USD", "CNY")))
            client.request_json.return_value = source_response(mc_data(("USD", "CNY")))
            discover_currencies(provider, {"currency_endpoint": "https://fixture.test/currencies",
                                          "referer": "https://fixture.test/calculator"}, client)
            method = client.request_text if provider == "visa" else client.request_json
            method.assert_called_once_with("https://fixture.test/currencies", headers={
                "Accept": "application/json, text/plain, */*", "Referer": "https://fixture.test/calculator"})
            (client.request_json if provider == "visa" else client.request_text).assert_not_called()

    def test_unknown_provider_and_invalid_endpoint_do_not_make_requests(self):
        client = Mock(spec=["request_text", "request_json"])
        for provider, config in (("unionpay", {}), ("unknown", {}),
                                 ("visa", {"currency_endpoint": ""}),
                                 ("mastercard", {"currency_endpoint": None})):
            with self.subTest(provider=provider, config=config), self.assertRaises(CatalogError):
                discover_currencies(provider, config, client)
        client.request_json.assert_not_called()
        client.request_text.assert_not_called()


if __name__ == "__main__":
    unittest.main()
