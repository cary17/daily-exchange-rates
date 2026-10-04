"""Persistent access denials stop collection without publishing partial days."""
from datetime import date
from threading import Lock
import unittest
from unittest.mock import Mock, patch

import simplejson

from exchange_rates.catalog import CurrencyCatalog
from exchange_rates.http import AccessBlockedError, HttpClient
from exchange_rates.providers import fetch_day
from tests.test_providers import http_response


class AccessRecoveryTests(unittest.TestCase):
    def test_shared_block_stops_before_full_scan_and_preserves_diagnostics(self):
        lock = Lock()
        calls = []

        def deny(url, params=None, headers=None, **kwargs):
            with lock:
                calls.append(dict(params))
            return http_response(403, b'<html>Access Denied fixture</html>')

        def session_factory(**kwargs):
            session = Mock()
            session.get.side_effect = deny
            return session

        catalog = CurrencyCatalog(('USD', 'CNY', 'EUR', 'JPY', 'GBP'),
                                  ('USD', 'CNY', 'EUR', 'JPY', 'GBP'))
        with patch('exchange_rates.http.requests.Session', side_effect=session_factory):
            with HttpClient(retries=0, interval=0, forbidden_threshold=3) as client:
                with self.assertRaises(AccessBlockedError) as caught:
                    fetch_day('visa', date(2026, 10, 4), catalog, {'recovery_rounds': 2}, client)
        error = caught.exception
        self.assertLess(len(calls), catalog.pair_count)
        self.assertEqual(error.context['round'], 0)
        self.assertEqual(error.context['successful_pairs'], 0)
        self.assertTrue(error.context['stopped_for_access_control'])
        self.assertEqual(len(error.raw_parts), 9)
        bodies = [record['response']['body_text']
                  for payload in error.raw_parts.values()
                  for record in simplejson.loads(payload)['requests']]
        self.assertTrue(bodies)
        self.assertTrue(all('Access Denied fixture' in body for body in bodies))
        self.assertLess(len(str(error)), 1000)

    def test_unionpay_block_does_not_start_business_recovery(self):
        with patch('exchange_rates.http.requests.Session') as factory:
            factory.return_value.get.return_value = http_response(403, b'access denied')
            with HttpClient(retries=0, interval=0, forbidden_threshold=1) as client:
                with self.assertRaises(AccessBlockedError):
                    fetch_day('unionpay', date(2026, 10, 4), None,
                              {'recovery_rounds': 10}, client)
            self.assertEqual(factory.return_value.get.call_count, 1)


if __name__ == '__main__':
    unittest.main()
