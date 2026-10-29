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
    DateMismatchError, JcbUnavailableError, PublicationUnknownError, ProviderError, fetch_day,
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
RECONSTRUCTION_USD_ROWS = (
    ("USD", "1.000000000", "1.000000000", "1.000000000"),
    ("EUR", "0.896224385", "0.896498367", "0.896772515"),
    ("GBP", "0.750336713", "0.750421173", "0.750505653"),
    ("HKD", "7.825254000", "7.844848000", "7.864441000"),
    ("CNY", "6.632670000", "6.649385000", "6.666100000"),
    ("SGD", "1.278891000", "1.281242000", "1.283594000"),
    ("AUD", "1.416362384", "1.417382208", "1.418401489"),
    ("KRW", "1334.295000000", "1337.747500000", "1341.200000000"),
    ("THB", "33.555240000", "33.637620000", "33.720000000"),
    ("TWD", "32.221085000", "32.317042500", "32.413000000"),
    ("MYR", "4.058998000", "4.077093000", "4.095189000"),
    ("IDR", "18120.475000000", "18152.737500000", "18185.000000000"),
    ("PHP", "63.783178000", "63.904089000", "64.025000000"),
    ("VND", "25852.000000000", "25856.500000000", "25861.000000000"),
    ("CAD", "1.396955000", "1.398538000", "1.400122000"),
    ("JPY", "157.604200000", "157.845000000", "158.085800000"),
)
RECONSTRUCTED_JPY = (
    ("USD", "158.085"), ("EUR", "176.390"), ("GBP", "210.686"),
    ("HKD", "20.202"), ("CNY", "23.834"), ("SGD", "123.611"),
    ("AUD", "111.613"), ("KRW", "0.118"), ("THB", "4.711"),
    ("TWD", "4.906"), ("MYR", "38.947"), ("IDR", "0.008"),
    ("PHP", "2.478"), ("VND", "0.006"), ("CAD", "113.164"),
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
                self.assertEqual(list(feed), ["curDate", "exchangeRateJson"])
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
        self.assertFalse(results[0].metadata["derived"])
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
            (jpy_body(), usd_body(DAY - timedelta(days=1)), 2, DateMismatchError),
            (jpy_body(DAY + timedelta(days=1)),
             usd_body(DAY - timedelta(days=1), RECONSTRUCTION_USD_ROWS), 2, DateMismatchError),
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
        with self.assertRaises(HttpError):
            fetch_day("jcb", DAY, None, {}, client)
        client.reset_mock()
        client.request_text.side_effect = HttpError("HTTP 503", 503)
        with self.assertRaises(HttpError):
            fetch_day("jcb", DAY, None, {}, client)
        self.assertEqual(client.request_text.call_count, 1)

    def test_missing_jpy_is_reconstructed_from_same_day_usd(self):
        cases = (
            (DAY + timedelta(days=1), "historical_source_unavailable"),
            (DAY - timedelta(days=1), "not_yet_published"),
        )
        usd = usd_body(rows=RECONSTRUCTION_USD_ROWS)
        for current, expected in cases:
            with self.subTest(current=current):
                results, client = collected(jpy=jpy_body(current), usd=usd)
                self.assertEqual([result.provider for result in results],
                                 ["jcb-jpy", "jcb-usd-buy", "jcb-usd-mid", "jcb-usd-sell"])
                jpy = rows_by_provider(results)["jcb-jpy"]["exchangeRateJson"]
                self.assertEqual(jpy, [
                    {"transCur": code, "baseCur": "JPY", "rateData": Decimal(value)}
                    for code, value in RECONSTRUCTED_JPY
                ])
                self.assertTrue(results[0].metadata["derived"])
                self.assertEqual(results[0].metadata["original_source"], {
                    "availability": expected, "requested_date": str(DAY),
                    "response_date": str(current), "source_url": JPY_URL,
                })
                self.assertIn("JPY Sell / transCur Buy", results[0].metadata["rate_formula"])
                self.assertEqual(results[0].metadata["pair_count"], 15)
                for result in results:
                    self.assertEqual(result.requested_date, DAY)
                    self.assertEqual(json.loads(result.normalized)["curDate"], str(DAY))
                    self.assertEqual(result.metadata["source_urls"], [USD_URL])
                    self.assertEqual(result.metadata["response_dates"],
                                     [{"file": "usd10292026.html", "date": str(DAY)}])
                    self.assertEqual(json.loads(result.raw)["requests"][0]["response"]["body_text"], usd)
                self.assertEqual(client.request_text.call_count, 2)

    def test_jpy_reconstruction_truncates_without_decimal_context_rounding(self):
        rows = tuple((code, "1." + "0" * 120 + "1", "2", "3") if code == "EUR"
                     else (code, "998", "999", "1000") if code == "JPY"
                     else (code, buy, mid, sell)
                     for code, buy, mid, sell in RECONSTRUCTION_USD_ROWS)
        with localcontext() as context:
            context.prec = 6
            results, _ = collected(jpy=jpy_body(DAY + timedelta(days=1)), usd=usd_body(rows=rows))
        jpy = rows_by_provider(results)["jcb-jpy"]["exchangeRateJson"]
        self.assertEqual(next(row["rateData"] for row in jpy if row["transCur"] == "EUR"),
                         Decimal("999.999"))

    def test_jpy_reconstruction_requires_every_source_quote(self):
        for missing in ("JPY", "EUR"):
            rows = tuple(row for row in RECONSTRUCTION_USD_ROWS if row[0] != missing)
            with self.subTest(missing=missing), self.assertRaisesRegex(ProviderError, missing):
                collected(jpy=jpy_body(DAY + timedelta(days=1)), usd=usd_body(rows=rows))

    def test_usd_404_is_not_proof_that_official_data_was_unpublished(self):
        cases = (
            ((DAY - timedelta(days=1), DAY, DAY + timedelta(days=1)), HttpError, None),
            ((DAY - timedelta(days=1), DAY + timedelta(days=1)), JcbUnavailableError, "not_published"),
            ((DAY + timedelta(days=1), DAY + timedelta(days=2)), PublicationUnknownError, None),
            ((DAY - timedelta(days=3), DAY - timedelta(days=2)), PublicationUnknownError, None),
        )
        for published, error, availability in cases:
            with self.subTest(published=published, error=error):
                index = "<html>" + "".join(f'<a href="usd{day:%m%d%Y}.html">{day}</a>'
                                           for day in published) + "</html>"
                html = jpy_body()
                missing = HttpError("HTTP 404", 404)
                client = Mock(spec=HttpClient)
                client.request_text.side_effect = [
                    JsonResponse(None, html, html.encode(), JPY_URL), missing,
                    JsonResponse(None, index, index.encode(), "https://www.jcb.jp/rate/usd.html"),
                ]
                with self.assertRaises(error) as caught:
                    fetch_day("jcb", DAY, None, {}, client)
                if availability:
                    self.assertEqual(caught.exception.availability, availability)
                elif error is HttpError:
                    self.assertIs(caught.exception, missing)
                    self.assertEqual(missing.context["availability"], "download_failed")
                else:
                    self.assertEqual(caught.exception.context["availability"], "unknown")
                self.assertEqual(client.request_text.call_count, 3)

    def test_unreadable_or_untrusted_index_is_not_treated_as_unpublished(self):
        bad_indices = (
            HttpError("index unavailable", 503),
            "<html>challenge page</html>",
            '<a href="https://other.test/rate/usd10292026.html">date</a>',
            '<a href="/other/usd10292026.html">date</a>',
            '<a href="usd02302026.html">invalid date</a>',
        )
        for bad in bad_indices:
            with self.subTest(index=bad):
                html = jpy_body(DAY - timedelta(days=1))
                index = (bad if isinstance(bad, HttpError) else
                         JsonResponse(None, bad, bad.encode(), "https://www.jcb.jp/rate/usd.html"))
                client = Mock(spec=HttpClient)
                client.request_text.side_effect = [
                    JsonResponse(None, html, html.encode(), JPY_URL), HttpError("HTTP 404", 404), index,
                ]
                with self.assertRaises((HttpError, ProviderError)) as caught:
                    fetch_day("jcb", DAY, None, {}, client)
                self.assertNotIsInstance(caught.exception, JcbUnavailableError)

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
