"""JCB HTML parsing: four normalized feeds, exact reciprocals, strict dates."""

from datetime import date, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
import json
from math import gcd
import unittest
from unittest.mock import Mock

import simplejson

from exchange_rates.http import HttpClient, HttpError, JsonResponse
from exchange_rates.models import DayResult
from exchange_rates.providers import (
    DateMismatchError, NoDataError, ProviderError, fetch_day,
)

DAY = date(2026, 10, 29)
JPY_URL = "https://www.jcb.jp/rate/jpy.html"
USD_URL = "https://www.jcb.jp/rate/usd10292026.html"
USD_ROWS = (
    ("AED", "3.672500000", "3.672600000", "3.672700000"),
    ("ARS", "1541.500000000", "1541.750000000", "1542.000000000"),
    ("BND", "1.280000000", "1.300000000", "1.320000000"),
    ("USD", "1.000000000", "1.000000000", "1.000000000"),
)


def jpy_body(day=DAY, rows=None):
    rows = rows if rows is not None else (
        ("USD", "（米ドル）", "158.085"),
        ("EUR", "（欧州ユーロ）", "176.39"),
        ("KRW", "（韓国ウォン）", "0.118"),
    )
    cells = "\n".join(
        f"<tr><td><b>{code}</b></td><td>{label}</td><td>&#61;</td>"
        f"<td><span class=\"rate\">{value}</span></td><td>JPY（日本円）</td></tr>"
        for code, label, value in rows)
    return (f"<html><body><p>{day:%Y年%m月%d日}換算日の基準レート</p>\n"
            f"<table>\n{cells}\n</table></body></html>")


def usd_body(day=DAY, rows=USD_ROWS):
    header = ('<tr class="odd"><td></td><td></td><td>Buy</td><td>Mid</td>'
              '<td>Sell</td><td></td></tr>')
    cells = "\n".join(
        f'<tr><td class="">USD</td><td class="">=</td>'
        f'<td class=""><span>{buy}</span></td><td class="">{mid}</td>'
        f'<td class="">{sell}</td><td class="">{code}</td></tr>'
        for code, buy, mid, sell in rows)
    return (f"<html><body><p>Base rate for {day:%m/%d/%Y}</p>\n"
            f'<div id="CSVTable"><table class="CSVTable"><tbody>\n{header}\n'
            f"{cells}\n</tbody></table></div></body></html>")


def collected(jpy=None, usd=None):
    jpy = jpy_body() if jpy is None else jpy
    usd = usd_body() if usd is None else usd
    client = Mock(spec=HttpClient)
    client.request_text.side_effect = [
        JsonResponse(None, jpy, jpy.encode(), JPY_URL),
        JsonResponse(None, usd, usd.encode(), USD_URL),
    ]
    return fetch_day("jcb", DAY, None, {}, client), client


def rows_by_provider(results):
    return {result.provider: simplejson.loads(result.normalized, use_decimal=True)
            for result in results}


def reciprocal(value, rounding=ROUND_DOWN, precision=100):
    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        return Decimal(1) / Decimal(value)


class JcbTests(unittest.TestCase):
    def test_four_feeds_from_exactly_two_source_requests(self):
        results, client = collected()
        self.assertIsInstance(results, tuple)
        self.assertEqual(len(results), 4)
        self.assertTrue(all(isinstance(result, DayResult) for result in results))
        self.assertEqual([result.provider for result in results],
                         ["jcb-jpy", "jcb-usd-buy", "jcb-usd-mid", "jcb-usd-sell"])
        self.assertEqual([call.args[0] for call in client.request_text.call_args_list],
                         [JPY_URL, USD_URL])
        client.request_json.assert_not_called()
        self.assertTrue(all(result.requested_date == DAY for result in results))
        self.assertTrue(all(result.raw_parts == {} for result in results))
        feeds = rows_by_provider(results)
        for provider, feed in feeds.items():
            with self.subTest(provider=provider):
                self.assertEqual(set(feed), {"curDate", "exchangeRateJson"})
                self.assertEqual(feed["curDate"], "2026-10-29")
        jpy = feeds["jcb-jpy"]["exchangeRateJson"]
        self.assertEqual([row["transCur"] for row in jpy], ["USD", "EUR", "KRW"])
        self.assertTrue(all(row["baseCur"] == "JPY" for row in jpy))
        self.assertEqual(jpy[0]["rateData"], Decimal("158.085"))
        self.assertNotIn("rateNumerator", jpy[0])
        for provider, column in (("jcb-usd-buy", 0), ("jcb-usd-mid", 1), ("jcb-usd-sell", 2)):
            feed = feeds[provider]["exchangeRateJson"]
            self.assertEqual([row["transCur"] for row in feed],
                             [code for code, *_ in USD_ROWS])
            self.assertTrue(all(row["baseCur"] == "USD" for row in feed))
            for row, (_, buy, mid, sell) in zip(feed, USD_ROWS):
                self.assertEqual(row["rateData"],
                                 reciprocal((buy, mid, sell)[column]))
        self.assertEqual(feeds["jcb-usd-buy"]["exchangeRateJson"][3]["rateData"], Decimal(1))
        self.assertEqual(results[0].metadata["pair_count"], 3)
        self.assertEqual(results[1].metadata["pair_count"], len(USD_ROWS))
        for result, column in zip(results[1:], ("Buy", "Mid", "Sell")):
            self.assertEqual(result.metadata["quote_column"], column)

    def test_live_full_width_separator_and_entities_are_accepted(self):
        jpy = jpy_body().replace("<td>&#61;</td>", "<td>＝</td>", 1)
        self.assertIn("＝", jpy)
        results, _ = collected(jpy)
        self.assertEqual(len(results), 4)

    def test_original_html_is_preserved_per_result_envelope(self):
        jpy = jpy_body()
        usd = usd_body()
        results, _ = collected(jpy, usd)
        expected = {"jcb-jpy": jpy, "jcb-usd-buy": usd,
                    "jcb-usd-mid": usd, "jcb-usd-sell": usd}
        for result in results:
            with self.subTest(provider=result.provider):
                raw = json.loads(result.raw)
                self.assertEqual(raw["provider"], result.provider)
                self.assertEqual(raw["requested_date"], "2026-10-29")
                self.assertEqual(len(raw["requests"]), 1)
                self.assertEqual(raw["requests"][0]["response"]["body_text"],
                                 expected[result.provider])
                self.assertNotIn("parts", raw)
                self.assertEqual(result.metadata["source_urls"],
                                 [JPY_URL if result.provider == "jcb-jpy" else USD_URL])
                self.assertEqual(result.metadata["response_dates"],
                                 [{"file": "jpy.html" if result.provider == "jcb-jpy"
                                   else "usd10292026.html", "date": "2026-10-29"}])

    def test_reciprocal_is_truncated_at_100_digits_not_rounded(self):
        results, _ = collected()
        sell = rows_by_provider(results)["jcb-usd-sell"]["exchangeRateJson"][0]
        original = Decimal("3.672700000")
        truncated = reciprocal(original)
        nearest = reciprocal(original, ROUND_HALF_EVEN)
        self.assertEqual(sell["rateData"], truncated)
        self.assertEqual(len(sell["rateData"].as_tuple().digits), 100)
        # The truncated product stays below 1 and strictly below nearest rounding.
        self.assertLess(Fraction(sell["rateData"]) * Fraction(original), 1)
        self.assertGreater(nearest, sell["rateData"])
        # rateNumerator/rateDenominator is the reduced exact reciprocal of the decimal.
        exact = Fraction(1) / Fraction(original)
        self.assertEqual(Fraction(sell["rateNumerator"],
                                  sell["rateDenominator"]), exact)
        self.assertEqual(Fraction(sell["rateNumerator"], sell["rateDenominator"])
                         * Fraction(original), 1)
        self.assertNotEqual(Fraction(sell["rateData"]), exact)
        self.assertEqual(gcd(sell["rateNumerator"], sell["rateDenominator"]), 1)
        self.assertEqual((sell["rateNumerator"], sell["rateDenominator"]), (10000, 36727))

    def test_terminating_reciprocal_is_exact(self):
        results, _ = collected()
        feed = rows_by_provider(results)["jcb-usd-buy"]["exchangeRateJson"]
        bnd = next(row for row in feed if row["transCur"] == "BND")
        self.assertEqual(bnd["rateData"], Decimal("0.78125"))
        self.assertEqual(Fraction(bnd["rateNumerator"], bnd["rateDenominator"]),
                         Fraction(25, 32))
        self.assertEqual(Fraction(bnd["rateData"]) * Fraction(Decimal("1.280000000")), 1)
        self.assertTrue(all(isinstance(row["rateNumerator"], int)
                            and isinstance(row["rateDenominator"], int) for row in feed))

    def test_both_page_dates_must_match_requested_day(self):
        cases = (
            (jpy_body(DAY - timedelta(days=1)), usd_body(), 1, DateMismatchError),
            (jpy_body(), usd_body(DAY - timedelta(days=1)), 2, DateMismatchError),
            (jpy_body(), usd_body().replace("10/29/2026", "02/30/2026"), 2, ProviderError),
            (jpy_body().replace("2026年10月29日", ""), usd_body(), 1, ProviderError),
            (jpy_body(), usd_body().replace("Base rate for 10/29/2026", ""), 2, ProviderError),
        )
        for jpy, usd, calls, error in cases:
            with self.subTest(error=error, calls=calls):
                client = Mock(spec=HttpClient)
                client.request_text.side_effect = [
                    JsonResponse(None, jpy, jpy.encode(), JPY_URL),
                    JsonResponse(None, usd, usd.encode(), USD_URL),
                ]
                with self.assertRaises(error):
                    fetch_day("jcb", DAY, None, {}, client)
                self.assertEqual(client.request_text.call_count, calls)

    def test_http_errors(self):
        client = Mock(spec=HttpClient)
        client.request_text.side_effect = HttpError("HTTP 404", 404)
        with self.assertRaises(NoDataError):
            fetch_day("jcb", DAY, None, {}, client)
        client.reset_mock()
        client.request_text.side_effect = HttpError("HTTP 503", 503)
        with self.assertRaises(HttpError):
            fetch_day("jcb", DAY, None, {}, client)
        self.assertEqual(client.request_text.call_count, 1)

    def test_jpy_malformed_rows_are_rejected(self):
        bad_rows = (
            (("USD", "（米ドル）", "158.085"), ("USD", "dup", "1.0")),       # duplicate
            (("usd", "（米ドル）", "158.085"),),                               # currency code
            (("USD", "", "158.085"),),                                        # empty label
            (("USD", "（米ドル）", "abc"),),                                   # not a number
            (("USD", "（米ドル）", "0"),),                                     # nonpositive
            (("USD", "（米ドル）", "NaN"),),                                   # nonfinite
            (("USD", "（米ドル）", "-1.5"),),                                  # negative
        )
        for rows in bad_rows:
            with self.subTest(rows=rows):
                jpy = jpy_body(rows=rows)
                client = Mock(spec=HttpClient)
                client.request_text.return_value = JsonResponse(
                    None, jpy, jpy.encode(), JPY_URL)
                with self.assertRaises(ProviderError):
                    fetch_day("jcb", DAY, None, {}, client)

    def test_jpy_malformed_structure_is_rejected(self):
        cases = (
            jpy_body().replace("<td>&#61;</td>", "<td>:</td>", 1),  # wrong separator
            jpy_body().replace("<td>JPY（日本円）</td>", "<td>USD</td>", 1),
            jpy_body().replace("<td>JPY（日本円）</td>", "<td>JPYINVALID</td>", 1),
            jpy_body().replace("</span></td>", "</span></td><td>extra</td>", 1),
            "<html><body><p>2026年10月29日換算日の基準レート</p><table></table></body></html>",
            "<html><body><p>2026年10月29日換算日の基準レート</p></body></html>",
        )
        for jpy in cases:
            with self.subTest(jpy=jpy[-40:]):
                client = Mock(spec=HttpClient)
                client.request_text.return_value = JsonResponse(
                    None, jpy, jpy.encode(), JPY_URL)
                with self.assertRaises(ProviderError):
                    fetch_day("jcb", DAY, None, {}, client)

    def test_usd_malformed_rows_and_index_pages_are_rejected(self):
        header = ("", "", "Buy", "Mid", "Sell", "")
        bad_rows = (
            (header, ("USD", "=", "1.0", "1.0", "1.0", "USD"),
             ("USD", "=", "2.0", "2.0", "2.0", "USD")),                # duplicate
            (header, ("EUR", "=", "1.0", "1.0", "1.0", "EUR")),         # wrong source
            (header, ("USD", "=", "1.0", "1.0", "1.0", "eur")),         # code
            (header, ("USD", "=", "1.0", "1.0", "1.0")),                # short row
            (header, ("USD", "=", "1.0", "0", "1.0", "AED")),           # nonpositive
        )
        for rows in bad_rows:
            with self.subTest(rows=rows):
                cells = "\n".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row)
                                  + "</tr>" for row in rows)
                usd = (f"<html><body><p>Base rate for {DAY:%m/%d/%Y}</p>"
                       f"<table>{cells}</table></body></html>")
                client = Mock(spec=HttpClient)
                client.request_text.side_effect = [
                    JsonResponse(None, jpy_body(), jpy_body().encode(), JPY_URL),
                    JsonResponse(None, usd, usd.encode(), USD_URL),
                ]
                with self.assertRaises(ProviderError):
                    fetch_day("jcb", DAY, None, {}, client)
        for usd in (f"<html><ul><li>{DAY:%m/%d/%Y}</li></ul></html>",
                    f"<html><p>Base rate for {DAY:%m/%d/%Y}</p><table></table></html>"):
            with self.subTest(usd=usd):
                client = Mock(spec=HttpClient)
                client.request_text.side_effect = [
                    JsonResponse(None, jpy_body(), jpy_body().encode(), JPY_URL),
                    JsonResponse(None, usd, usd.encode(), "https://www.jcb.jp/rate/usd.html"),
                ]
                with self.assertRaises(ProviderError):
                    fetch_day("jcb", DAY, None, {}, client)


if __name__ == "__main__":
    unittest.main()
