"""Deterministic shared pacing and HTTP 403 circuit tests; no network or sleeps."""

from concurrent.futures import CancelledError, ThreadPoolExecutor
from threading import Event, Lock
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from curl_cffi import requests

from exchange_rates.http import (
    AccessBlockedError, HttpClient, HttpError, JsonResponse, ResponseDecodeError,
)
from exchange_rates.rate_control import GateBlockedError, RateGate


URL = "https://example.test/rates"


def response(status=200, body=b'{"rate":1}'):
    return SimpleNamespace(status_code=status, content=body, text=body.decode(),
                           url=URL, headers={"Content-Type": "application/json"})


class FakeClock:
    def __init__(self):
        self.now = 10.0
        self.waits = []
        self.lock = Lock()

    def __call__(self):
        with self.lock:
            return self.now

    def advance(self, seconds):
        with self.lock:
            self.now += seconds

    def wait(self, seconds, cancelled):
        self.waits.append((seconds, cancelled))
        self.advance(seconds)
        return cancelled is not None and cancelled.is_set()


def controlled_client(clock, **settings):
    client = HttpClient(interval=0, **settings)
    client._gate = RateGate(client.global_interval, client.forbidden_cooldown,
                            client.forbidden_threshold, clock=clock, wait=clock.wait)
    return client


class RateControlTests(unittest.TestCase):
    def test_numeric_validation_is_strict_and_finite(self):
        invalid = (True, False, None, "0.1", complex(1, 0), float("nan"),
                   float("inf"), float("-inf"), -0.1, 10 ** 1000)
        for name in ("timeout", "interval", "global_interval", "forbidden_cooldown"):
            for value in invalid:
                with self.subTest(name=name, value=repr(value)[:40]):
                    with self.assertRaises(ValueError):
                        HttpClient(**{name: value})
        with self.assertRaises(ValueError):
            HttpClient(timeout=0)
        for name in ("retries", "forbidden_threshold"):
            for value in (True, False, -1, 1.0, None, "3", float("nan"), float("inf")):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    HttpClient(**{name: value})
        client = HttpClient(timeout=1, interval=0, global_interval=0,
                            forbidden_cooldown=0, forbidden_threshold=0)
        self.assertFalse(client._gate.enabled)
        self.assertIsNone(client._session)

    def test_nine_forks_share_gate_but_own_sessions_and_records(self):
        clock = FakeClock()
        parent = controlled_client(clock, global_interval=0.1,
                                   forbidden_cooldown=60, forbidden_threshold=3)
        cancelled = Event()
        children = [parent.fork(cancelled) for _ in range(9)]
        sessions = [Mock() for _ in children]
        started = []
        for session in sessions:
            session.get.side_effect = lambda *args, **kwargs: (
                started.append(clock()) or response())
        with patch("exchange_rates.http.requests.Session", side_effect=sessions):
            for child in children:
                with child:
                    child.request_json(URL)
        self.assertEqual(len({id(child._gate) for child in children}), 1)
        self.assertIs(children[0]._gate, parent._gate)
        self.assertEqual(len({id(child.records) for child in children}), 9)
        self.assertEqual(len({id(session) for session in sessions}), 9)
        for index, value in enumerate(started):
            self.assertAlmostEqual(value, 10 + index * 0.1)
        for child, session in zip(children, sessions):
            self.assertEqual((child.global_interval, child.forbidden_cooldown,
                              child.forbidden_threshold), (0.1, 60, 3))
            self.assertIs(child.cancel_event, cancelled)
            session.close.assert_called_once()
        other = controlled_client(clock, global_interval=0.1)
        self.assertIsNot(other._gate, parent._gate)
        self.assertEqual(other._gate._next_started, 0)

    def test_every_transport_and_status_retry_attempt_passes_gate(self):
        for failure in (requests.RequestsError("network"), response(429), response(503)):
            clock = FakeClock()
            client = controlled_client(clock, retries=1, global_interval=1)
            started = []
            outcomes = iter((failure, response()))
            def get(*args, **kwargs):
                started.append(clock())
                outcome = next(outcomes)
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome
            session = Mock()
            session.get.side_effect = get
            with self.subTest(failure=failure), patch(
                    "exchange_rates.http.requests.Session", return_value=session), patch.object(
                    client, "_sleep", side_effect=clock.advance):
                result = client.request_json(URL)
            self.assertEqual(started, [10, 11])
            self.assertEqual([record["attempt"] for record in result.records], [1, 2])
            self.assertTrue(clock.waits)

    def test_single_403_cools_all_forks_without_immediate_retry(self):
        clock = FakeClock()
        parent = controlled_client(clock, retries=3, global_interval=0.1,
                                   forbidden_cooldown=60, forbidden_threshold=3)
        first, sibling = parent.fork(), parent.fork()
        sessions = [Mock(), Mock()]
        sessions[0].get.return_value = response(403, b"forbidden")
        sessions[1].get.return_value = response()
        with patch("exchange_rates.http.requests.Session", side_effect=sessions), patch(
                "exchange_rates.http.time.sleep", side_effect=AssertionError("real sleep")):
            with self.assertRaises(HttpError) as caught:
                first.request_json(URL)
            self.assertNotIsInstance(caught.exception, AccessBlockedError)
            self.assertEqual(clock.waits, [])
            sessions[0].get.assert_called_once()
            sibling.request_json(URL)
        self.assertEqual(clock(), 70)
        self.assertTrue(all(0 < seconds <= RateGate.WAIT_SLICE
                            for seconds, _ in clock.waits))
        self.assertEqual(parent._gate.consecutive_forbidden, 0)
        self.assertFalse(parent._gate.broken)

    def test_200_resets_count_then_threshold_breaks_whole_group(self):
        clock = FakeClock()
        parent = controlled_client(clock, forbidden_cooldown=60, forbidden_threshold=3)
        children = [parent.fork() for _ in range(5)]
        statuses = (403, 200, 403, 403, 403)
        sessions = [Mock() for _ in children]
        for session, status in zip(sessions, statuses):
            session.get.return_value = response(status)
        caught = None
        with patch("exchange_rates.http.requests.Session", side_effect=sessions):
            for child, status, expected in zip(children, statuses, (1, 0, 1, 2, 3)):
                if status == 200:
                    child.request_json(URL)
                else:
                    with self.assertRaises(HttpError) as error:
                        child.request_json(URL)
                    caught = error.exception
                    self.assertEqual(isinstance(caught, AccessBlockedError), expected == 3)
                self.assertEqual(parent._gate.consecutive_forbidden, expected)
        self.assertTrue(parent._gate.broken)
        self.assertIsInstance(caught, AccessBlockedError)
        self.assertEqual((caught.status_code, caught.last_status), (403, 403))
        self.assertEqual(caught.response.status_code, 403)
        self.assertEqual(caught.records[-1]["response"]["status_code"], 403)
        self.assertLess(len(str(caught)), 80)
        blocked = parent.fork()
        with patch("exchange_rates.http.requests.Session") as factory:
            with self.assertRaises(AccessBlockedError) as sibling_error:
                blocked.request_json(URL)
            factory.return_value.get.assert_not_called()
        self.assertEqual(blocked.records, [])
        self.assertIs(sibling_error.exception.response, caught.response)
        self.assertEqual(sibling_error.exception.records, caught.records)

    def test_only_200_resets_forbidden_count(self):
        gate = RateGate(forbidden_threshold=3)
        gate.record_response(403, response(403))
        for status in (204, 301, 400, 429, 500):
            gate.record_response(status, response(status))
            self.assertEqual(gate.consecutive_forbidden, 1)
        gate.record_response(200, response())
        self.assertEqual(gate.consecutive_forbidden, 0)

    def test_successful_http_200_resets_before_json_decode(self):
        clock = FakeClock()
        client = controlled_client(clock, forbidden_threshold=3)
        session = Mock()
        session.get.side_effect = [response(403), response(200, b"not JSON")]
        with patch("exchange_rates.http.requests.Session", return_value=session):
            with self.assertRaises(HttpError):
                client.request_json(URL)
            with self.assertRaises(ResponseDecodeError):
                client.request_json(URL)
        self.assertEqual(client._gate.consecutive_forbidden, 0)

    def test_threshold_zero_disables_breaking_but_not_cooldown(self):
        clock = FakeClock()
        gate = RateGate(forbidden_cooldown=60, clock=clock, wait=clock.wait)
        for _ in range(5):
            gate.acquire()
            self.assertFalse(gate.record_response(403, response(403)))
        self.assertEqual(clock(), 250)
        self.assertEqual(gate.consecutive_forbidden, 5)
        self.assertFalse(gate.broken)

    def test_breaking_is_terminal_even_after_inflight_200(self):
        gate = RateGate(forbidden_threshold=1)
        evidence = response(403)
        self.assertTrue(gate.record_response(403, evidence))
        self.assertTrue(gate.record_response(200, response()))
        self.assertEqual(gate.consecutive_forbidden, 1)
        self.assertIs(gate.last_response, evidence)
        with self.assertRaises(GateBlockedError):
            gate.acquire()

    def test_wait_is_outside_lock_and_rechecks_extended_cooldown(self):
        clock = FakeClock()
        gate = RateGate(forbidden_cooldown=60, clock=clock)
        gate.record_response(403, response(403))
        extended = False
        def wait(seconds, cancelled):
            nonlocal extended
            self.assertTrue(gate._lock.acquire(blocking=False))
            gate._lock.release()
            clock.wait(seconds, cancelled)
            if not extended:
                extended = True
                gate.record_response(403, response(403))
        gate._wait = wait
        gate.acquire()
        self.assertEqual(clock(), 70.25)

    def test_waiter_observes_breaking_without_sending(self):
        clock = FakeClock()
        parent = controlled_client(clock, global_interval=1, forbidden_threshold=1)
        parent._gate.acquire()
        evidence = JsonResponse(data=None, body_text="forbidden", body=b"forbidden",
                                url=URL, status_code=403)
        evidence.records = [evidence.as_record()]
        def break_on_wait(seconds, cancelled):
            self.assertTrue(parent._gate._lock.acquire(blocking=False))
            parent._gate._lock.release()
            parent._gate.record_response(403, evidence)
        parent._gate._wait = break_on_wait
        child = parent.fork()
        with patch("exchange_rates.http.requests.Session") as factory:
            with self.assertRaises(AccessBlockedError) as caught:
                child.request_json(URL)
            factory.return_value.get.assert_not_called()
        self.assertIs(caught.exception.response, evidence)
        self.assertEqual(clock(), 10)

    def test_cancel_event_interrupts_cooldown_wait(self):
        clock = FakeClock()
        cancelled = Event()
        parent = controlled_client(clock, forbidden_cooldown=60)
        parent._gate.record_response(403, response(403))
        child = parent.fork(cancelled)
        def cancel(seconds):
            self.assertLessEqual(seconds, RateGate.WAIT_SLICE)
            cancelled.set()
            return True
        # Use the production waiter to prove Event.wait is the interrupt path.
        from exchange_rates.rate_control import _wait
        parent._gate._wait = _wait
        with patch.object(cancelled, "wait", side_effect=cancel) as wait, patch(
                "exchange_rates.http.requests.Session") as factory:
            with self.assertRaises(CancelledError):
                child.request_text(URL)
            wait.assert_called_once_with(0.25)
            factory.return_value.get.assert_not_called()
        self.assertEqual(clock(), 10)

    def test_preexisting_cancellation_remains_cancelled_error(self):
        cancelled = Event()
        cancelled.set()
        gate = RateGate(forbidden_threshold=1, clock=Mock(side_effect=AssertionError))
        gate.record_response(403, response(403))
        with self.assertRaises(CancelledError):
            gate.acquire(cancelled)

    def test_gate_uses_monotonic_deadlines_not_wall_clock(self):
        clock = FakeClock()
        with patch("exchange_rates.rate_control.time.monotonic", side_effect=clock), patch(
                "exchange_rates.rate_control.time.time", side_effect=AssertionError("wall clock")):
            gate = RateGate(global_interval=0.5, forbidden_cooldown=60, wait=clock.wait)
            gate.acquire()
            gate.record_response(403, response(403))
            gate.acquire()
        self.assertEqual(clock(), 70)

    def test_default_configuration_keeps_gate_out_of_legacy_path(self):
        client = HttpClient(interval=0)
        session = Mock()
        session.get.side_effect = [response(403), response()]
        with patch("exchange_rates.http.requests.Session", return_value=session), patch.object(
                client._gate, "acquire", side_effect=AssertionError("disabled gate")), patch.object(
                client._gate, "record_response", side_effect=AssertionError("disabled gate")):
            with self.assertRaises(HttpError) as caught:
                client.request_json(URL)
            self.assertNotIsInstance(caught.exception, AccessBlockedError)
            client.request_json(URL)
        self.assertEqual(client._gate.consecutive_forbidden, 0)

    def test_default_session_interval_is_half_second(self):
        self.assertEqual(HttpClient().interval, 0.5)

    def test_sessions_keep_independent_half_second_budgets(self):
        clock = FakeClock()
        parent = HttpClient(forbidden_cooldown=60)
        parent._gate = RateGate(forbidden_cooldown=60, clock=clock, wait=clock.wait)
        children = [parent.fork(), parent.fork()]
        sessions = [Mock(), Mock()]
        started = []
        for session in sessions:
            session.get.side_effect = lambda *args, **kwargs: (
                started.append(clock()) or response())
        with patch("exchange_rates.http.requests.Session", side_effect=sessions), patch(
                "exchange_rates.http.time.monotonic", side_effect=clock):
            for child in children:
                self.enterContext(patch.object(child, "_sleep", side_effect=clock.advance))
            for child in children + children:
                child.request_json(URL)
        self.assertEqual(started, [10, 10, 10.5, 10.5])
        self.assertEqual(parent.global_interval, 0)

    def test_cooldown_wait_does_not_erase_session_interval(self):
        clock = FakeClock()
        client = HttpClient(forbidden_cooldown=60)
        client._gate = RateGate(forbidden_cooldown=60, clock=clock, wait=clock.wait)
        started = []
        outcomes = iter((response(403), response(), response()))
        session = Mock()
        session.get.side_effect = lambda *args, **kwargs: (
            started.append(clock()) or next(outcomes))
        with patch("exchange_rates.http.requests.Session", return_value=session), patch(
                "exchange_rates.http.time.monotonic", side_effect=clock), patch.object(
                client, "_sleep", side_effect=clock.advance):
            with self.assertRaises(HttpError):
                client.request_json(URL)
            client.request_json(URL)
            client.request_json(URL)
        self.assertEqual(started, [10, 70, 70.5])

    def test_old_inflight_responses_do_not_release_active_probe(self):
        clock = FakeClock()
        gate = RateGate(forbidden_cooldown=60, forbidden_threshold=3,
                        clock=clock, wait=clock.wait)
        old_epoch = gate.acquire()
        gate.record_response(403, response(403), epoch=old_epoch)
        probe_epoch = gate.acquire()
        for status in (200, 403):
            gate.record_response(status, response(status), epoch=old_epoch)
            self.assertTrue(gate._probe_inflight)
            self.assertTrue(gate._recovering)
            self.assertEqual(gate.consecutive_forbidden, 1)
        gate.record_response(200, response(), epoch=probe_epoch)
        self.assertFalse(gate._recovering)

    def test_one_denial_wave_counts_once_and_late_success_cannot_reset(self):
        clock = FakeClock()
        gate = RateGate(forbidden_cooldown=60, forbidden_threshold=3,
                        clock=clock, wait=clock.wait)
        epochs = [gate.acquire() for _ in range(9)]
        evidence = response(403)
        gate.record_response(403, evidence, epoch=epochs[0])
        for epoch in epochs[1:]:
            self.assertFalse(gate.record_response(403, response(403), epoch=epoch))
            self.assertFalse(gate.record_response(200, response(), epoch=epoch))
        self.assertEqual(gate.consecutive_forbidden, 1)
        self.assertFalse(gate.broken)
        self.assertIs(gate.last_response, evidence)
        self.assertEqual(clock(), 10)
        probe_epoch = gate.acquire()
        self.assertEqual(clock(), 70)
        self.assertNotEqual(probe_epoch, epochs[0])
        gate.record_response(403, response(403), epoch=probe_epoch)
        self.assertEqual(gate.consecutive_forbidden, 2)
        probe_epoch = gate.acquire()
        self.assertEqual(clock(), 130)
        self.assertTrue(gate.record_response(403, response(403), epoch=probe_epoch))
        self.assertTrue(gate.record_response(200, response(), epoch=epochs[0]))
        with self.assertRaises(GateBlockedError):
            gate.acquire()

    def test_cooldown_zero_still_counts_each_inflight_denial(self):
        gate = RateGate(forbidden_threshold=3)
        epochs = [gate.acquire() for _ in range(9)]
        for index, epoch in enumerate(epochs[:3]):
            self.assertEqual(gate.record_response(403, response(403), epoch=epoch),
                             index == 2)
        self.assertEqual(gate.consecutive_forbidden, 3)

    def test_half_open_releases_after_transport_or_other_status_failure(self):
        for outcome in (requests.RequestsError("probe failed"), response(429),
                        response(503), response(204, b""), response(301)):
            clock = FakeClock()
            client = controlled_client(clock, retries=0, forbidden_cooldown=60,
                                       forbidden_threshold=3)
            session = Mock()
            session.get.side_effect = [response(403), outcome, response()]
            with self.subTest(outcome=outcome), patch(
                    "exchange_rates.http.requests.Session", return_value=session):
                with self.assertRaises(HttpError):
                    client.request_json(URL)
                with self.assertRaises(HttpError):
                    client.request_json(URL)
                self.assertFalse(client._gate._probe_inflight)
                self.assertTrue(client._gate._recovering)
                self.assertEqual(client._gate.consecutive_forbidden, 1)
                client.request_json(URL)
            self.assertFalse(client._gate._recovering)
            self.assertEqual(client._gate.consecutive_forbidden, 0)
            self.assertEqual(clock(), 70)

    def test_half_open_releases_after_unexpected_transport_exception(self):
        clock = FakeClock()
        client = controlled_client(clock, retries=0, forbidden_cooldown=60)
        session = Mock()
        session.get.side_effect = [response(403), CancelledError("probe cancelled"), response()]
        with patch("exchange_rates.http.requests.Session", return_value=session):
            with self.assertRaises(HttpError):
                client.request_json(URL)
            with self.assertRaises(CancelledError):
                client.request_json(URL)
            self.assertFalse(client._gate._probe_inflight)
            client.request_json(URL)
        self.assertFalse(client._gate._recovering)

    def test_half_open_allows_only_one_probe_until_success(self):
        clock = FakeClock()
        gate = RateGate(forbidden_cooldown=60, forbidden_threshold=3, clock=clock)
        old_epoch = gate.acquire()
        gate.record_response(403, response(403), epoch=old_epoch)
        clock.advance(60)
        started, waiting, release = Event(), Event(), Event()
        probes = []
        lock = Lock()

        def wait(seconds, cancelled):
            waiting.set()
            self.assertTrue(release.wait(2))

        def acquire():
            epoch = gate.acquire()
            with lock:
                probes.append(epoch)
            started.set()
            return epoch

        gate._wait = wait
        with ThreadPoolExecutor(max_workers=9) as pool:
            futures = [pool.submit(acquire) for _ in range(9)]
            try:
                self.assertTrue(started.wait(2))
                self.assertTrue(waiting.wait(2))
                self.assertEqual(len(probes), 1)
                gate.record_response(200, response(), epoch=probes[0])
            finally:
                release.set()
            for future in futures:
                self.assertEqual(future.result(timeout=2), 1)
        self.assertEqual(len(probes), 9)
        self.assertFalse(gate._recovering)
        self.assertFalse(gate._probe_inflight)

    def test_threaded_gate_waits_do_not_hold_shared_lock(self):
        clock = FakeClock()
        waiting, release = Event(), Event()
        gate = RateGate(global_interval=1, forbidden_threshold=1, clock=clock)
        gate.acquire()
        def wait(seconds, cancelled):
            waiting.set()
            self.assertTrue(release.wait(2))
        gate._wait = wait
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(gate.acquire)
            self.assertTrue(waiting.wait(2))
            try:
                # Another worker must be able to update the gate while it waits.
                self.assertTrue(gate._lock.acquire(timeout=1))
                gate._lock.release()
                gate.record_response(403, response(403))
            finally:
                release.set()
            with self.assertRaises(GateBlockedError):
                future.result(timeout=2)


if __name__ == "__main__":
    unittest.main()
