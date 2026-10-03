"""Deterministic contracts for balanced, all-or-nothing currency concurrency."""

from itertools import product
import threading
import unittest
from unittest.mock import Mock

from exchange_rates.concurrency import SHARD_COUNT, balanced_shards, run_currency_shards


CURRENCIES = ("USD", "EUR", "JPY", "CNY", "GBP", "AUD", "CAD", "CHF", "HKD", "SGD", "KRW")
TIMEOUT = 5


class BalancedShardTests(unittest.TestCase):
    def test_balanced_contiguous_complete_shards(self):
        codes = tuple(f"A{chr(65 + index // 26)}{chr(65 + index % 26)}"
                      for index in range(31))
        for size in range(len(codes) + 1):
            for count in (1, SHARD_COUNT, 12):
                with self.subTest(size=size, count=count):
                    expected = codes[:size]
                    shards = balanced_shards(expected, count)
                    lengths = [len(shard) for shard in shards]
                    self.assertEqual(len(shards), count)
                    self.assertTrue(all(isinstance(shard, tuple) for shard in shards))
                    self.assertEqual(tuple(code for shard in shards for code in shard), expected)
                    self.assertLessEqual(max(lengths) - min(lengths), 1)
                    self.assertEqual(lengths, sorted(lengths, reverse=True))

    def test_fewer_currencies_than_default_shards(self):
        self.assertEqual(SHARD_COUNT, 9)
        self.assertEqual(balanced_shards(["USD", "EUR"]),
                         (("USD",), ("EUR",)) + ((),) * 7)
        self.assertEqual(balanced_shards([]), ((),) * 9)

    def test_invalid_and_duplicate_currency_codes(self):
        invalid = ("usd", "US", "USDD", "U1D", " USD", "USD\n", "", "\u00dcSD", 1, None, True)
        for code in invalid:
            with self.subTest(code=code), self.assertRaises(ValueError):
                balanced_shards((code,))
        with self.assertRaises(ValueError):
            balanced_shards(("USD", "EUR", "USD"))
        for codes in ("USD", b"USD", {"USD"}, {"USD": 1}, None, iter(("USD",))):
            with self.subTest(codes=codes), self.assertRaises(ValueError):
                balanced_shards(codes)

    def test_invalid_counts_are_rejected_before_worker_execution(self):
        for count in (True, False, 0, -1, 1.0, "9", None):
            worker = Mock()
            with self.subTest(count=count):
                with self.assertRaises(ValueError):
                    balanced_shards(CURRENCIES, count)
                with self.assertRaises(ValueError):
                    run_currency_shards(CURRENCIES, worker, count)
                worker.assert_not_called()
        worker = Mock()
        with self.assertRaises(ValueError):
            run_currency_shards(("USD", "USD"), worker)
        worker.assert_not_called()


class CurrencyConcurrencyTests(unittest.TestCase):
    def test_nine_workers_really_overlap_including_empty_shards(self):
        barrier = threading.Barrier(SHARD_COUNT, timeout=TIMEOUT)
        lock = threading.Lock()
        threads = set()
        events = []

        def worker(index, shard, cancel_event):
            with lock:
                threads.add((threading.get_ident(), threading.current_thread().name))
                events.append(cancel_event)
            barrier.wait()
            self.assertFalse(cancel_event.is_set())
            return index, shard

        results = run_currency_shards(("USD", "EUR"), worker)
        self.assertEqual(results, list(enumerate(balanced_shards(("USD", "EUR")))))
        self.assertEqual(len(threads), 9)
        self.assertTrue(all(name.startswith("fx-shard") for _, name in threads))
        self.assertEqual(len(events), 9)
        self.assertTrue(all(event is events[0] for event in events))

    def test_reverse_completion_preserves_shard_result_order(self):
        barrier = threading.Barrier(SHARD_COUNT, timeout=TIMEOUT)
        turns = [threading.Event() for _ in range(SHARD_COUNT)]
        turns[-1].set()
        completion_order = []
        lock = threading.Lock()

        def worker(index, shard, cancel_event):
            barrier.wait()
            self.assertTrue(turns[index].wait(TIMEOUT))
            with lock:
                completion_order.append(index)
            if index:
                turns[index - 1].set()
            return index, shard

        results = run_currency_shards(CURRENCIES, worker)
        self.assertEqual(completion_order, list(reversed(range(SHARD_COUNT))))
        self.assertEqual(results, list(enumerate(balanced_shards(CURRENCIES))))

    def test_failure_cancels_and_waits_without_partial_return(self):
        barrier = threading.Barrier(SHARD_COUNT, timeout=TIMEOUT)
        early_success = threading.Event()
        cancel_seen = threading.Event()
        release_cleanup = threading.Event()
        finished = threading.Event()
        lock = threading.Lock()
        exited = set()
        errors = []
        returned = []
        failure = RuntimeError("one shard failed")

        def worker(index, shard, cancel_event):
            try:
                barrier.wait()
                if index == 0:
                    early_success.set()
                    return "early result"
                if index == 4:
                    self.assertTrue(early_success.wait(TIMEOUT))
                    raise failure
                self.assertTrue(cancel_event.wait(TIMEOUT))
                cancel_seen.set()
                self.assertTrue(release_cleanup.wait(TIMEOUT))
                return "cancelled"
            finally:
                with lock:
                    exited.add(index)

        def run():
            try:
                returned.extend(run_currency_shards(CURRENCIES, worker))
            except BaseException as error:
                errors.append(error)
            finally:
                finished.set()

        runner = threading.Thread(target=run)
        runner.start()
        try:
            self.assertTrue(cancel_seen.wait(TIMEOUT))
            self.assertFalse(finished.is_set())
            release_cleanup.set()
            self.assertTrue(finished.wait(TIMEOUT))
        finally:
            release_cleanup.set()
            runner.join(TIMEOUT)
        self.assertFalse(runner.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIs(errors[0], failure)
        self.assertEqual(returned, [])
        self.assertEqual(exited, set(range(SHARD_COUNT)))

    def test_worker_queries_all_billing_currencies_not_only_its_shard(self):
        def worker(index, shard, cancel_event):
            pairs = []
            for transaction_currency in shard:
                for billing_currency in CURRENCIES:
                    self.assertFalse(cancel_event.is_set())
                    pairs.append((transaction_currency, billing_currency))
            return pairs

        results = run_currency_shards(CURRENCIES, worker)
        pairs = [pair for shard_pairs in results for pair in shard_pairs]
        self.assertEqual(pairs, list(product(CURRENCIES, repeat=2)))
        self.assertEqual(len(set(pairs)), len(CURRENCIES) ** 2)
        self.assertIn((CURRENCIES[0], CURRENCIES[-1]), pairs)


if __name__ == "__main__":
    unittest.main()
