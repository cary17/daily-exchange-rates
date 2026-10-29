"""Offline regional DNS routing and exhaustive Mastercard date checks."""

from threading import Event
import time
import unittest
from unittest.mock import Mock, patch

from curl_cffi import CurlOpt, requests
import dns.exception
import dns.message
import dns.name
import dns.rdatatype
import dns.resolver
import dns.rrset
import simplejson

from exchange_rates.catalog import CurrencyCatalog
from exchange_rates.edge_dns import EdgeResolution, resolve_edges
from exchange_rates.http import AccessBlockedError, HttpClient, HttpError
from exchange_rates.models import json_bytes
from exchange_rates.providers import RecoveryError, fetch_day, fetch_shard
from tests.test_providers import DAY, FakeClient, http_response


SERVERS = ("168.95.1.1", "168.95.192.1", "168.126.63.1", "219.250.36.130")
HOST = "www.mastercard.com"
URL = f"https://{HOST}/rates"
IPS = ("23.53.198.107", "23.45.114.89", "23.0.0.1")


def pool(addresses=IPS, complete=True):
    queries = [{"server": server, "type": kind, "addresses": list(addresses)}
               for server in SERVERS for kind in ("A", "AAAA")]
    if not complete:
        queries[-1]["error"] = "LifetimeTimeout"
    return EdgeResolution(tuple(addresses), queries, time.time() + 60)


def answer(kind, addresses):
    query = dns.message.make_query(HOST, kind)
    message = dns.message.make_response(query)
    message.answer.append(dns.rrset.from_text(HOST + ".", 20, "IN", "CNAME", "edge.example."))
    if addresses:
        message.answer.append(dns.rrset.from_text("edge.example.", 60, "IN", kind, *addresses))
    message = dns.message.from_wire(message.to_wire())
    return dns.resolver.Answer(dns.name.from_text(HOST), dns.rdatatype.from_text(kind),
                               1, message)


class EdgeDnsTests(unittest.TestCase):
    def test_four_resolvers_union_both_families_ttl_and_reject_fake_ip(self):
        def make_resolver(**kwargs):
            self.assertEqual(kwargs, {"configure": False})
            resolver = Mock()

            def resolve(host, kind, **options):
                self.assertEqual(host, HOST)
                self.assertEqual(options, {"search": False, "lifetime": 3,
                                           "raise_on_no_answer": False})
                server = resolver.nameservers[0]
                if server == SERVERS[-1] and kind == "AAAA":
                    raise dns.exception.Timeout()
                if kind == "AAAA":
                    values = ["2600:1406:5e00::17ce:bc1b"]
                else:
                    values = [IPS[0], IPS[SERVERS.index(server) % len(IPS)]]
                    if server == SERVERS[-1]:
                        values += ["198.18.4.220", "127.0.0.1", "224.0.0.1"]
                return answer(kind, values)

            resolver.resolve.side_effect = resolve
            return resolver

        with patch("exchange_rates.edge_dns.dns.resolver.Resolver", side_effect=make_resolver) as factory:
            before = time.time()
            result = resolve_edges(HOST, SERVERS)
        self.assertEqual(set(result.addresses), set(IPS + ("2600:1406:5e00::17ce:bc1b",)))
        self.assertEqual(len(result.addresses), 4)
        self.assertEqual(result.addresses[-1], "2600:1406:5e00::17ce:bc1b")
        self.assertEqual(len(result.queries), 8)
        self.assertFalse(result.complete)
        self.assertLessEqual(result.expires_at, before + 4)
        rejected = next(q for q in result.queries if "rejected_addresses" in q)
        self.assertCountEqual(rejected["rejected_addresses"], ["198.18.4.220", "127.0.0.1", "224.0.0.1"])
        self.assertEqual(factory.call_count, 4)

        with patch("exchange_rates.edge_dns.dns.resolver.Resolver", side_effect=make_resolver):
            healthy = resolve_edges(HOST, SERVERS[:3])
        self.assertTrue(healthy.complete)
        self.assertAlmostEqual(healthy.expires_at, time.time() + 20, delta=1)

    def test_dns_cache_shared_by_forks_and_refreshed_after_ttl(self):
        first, second = pool(), pool((IPS[-1],))
        first.expires_at = 20
        second.expires_at = 40
        with patch("exchange_rates.http.resolve_edges", side_effect=[first, second]) as resolve, \
             patch("exchange_rates.http.time.time", return_value=10) as clock:
            parent = HttpClient(edge_dns_servers=SERVERS)
            child = parent.fork(Event())
            self.assertEqual(parent.edge_candidates(URL).addresses, IPS)
            self.assertEqual(child.edge_candidates(URL).addresses, IPS)
            self.assertEqual(resolve.call_count, 1)
            clock.return_value = 21
            self.assertEqual(child.edge_candidates(URL).addresses, (IPS[-1],))
            self.assertEqual(parent.edge_candidates(URL).addresses, (IPS[-1],))
            self.assertEqual(resolve.call_count, 2)
            self.assertIs(parent._gate, child._gate)
            self.assertEqual(parent.records[0]["dns"]["addresses"], list(IPS))


class EdgeRecoveryTests(unittest.TestCase):
    def collect(self, states, *, sharded=False, complete=True, addresses=IPS):
        calls, sessions, options = [], [], []
        fixture = FakeClient("mastercard")
        catalog = CurrencyCatalog(("USD",), ("CNY",))

        def make_session(**kwargs):
            session = Mock()
            sessions.append(session)
            options.append(kwargs)
            mapping = kwargs.get("curl_options", {}).get(CurlOpt.RESOLVE)
            ip = mapping[0].split(":443:", 1)[1].strip("[]") if mapping else None

            def get(url, params=None, headers=None, **kwargs):
                calls.append(ip)
                state = states[ip]
                if state == "transport":
                    raise requests.RequestsError("edge unavailable")
                if state == "403":
                    result = http_response(403, b"denied")
                else:
                    data = fixture.request_json(url, params, headers).data
                    if state in ("old", "old_invalid"):
                        data["data"]["exchange_date"] = "2026-03-26"
                    if state == "old_invalid":
                        data["data"].pop("conversionRate")
                    result = http_response(body=json_bytes(data))
                result.primary_ip = ip
                return result

            session.get.side_effect = get
            return session

        with patch("exchange_rates.http.resolve_edges", return_value=pool(addresses, complete)), \
             patch("exchange_rates.http.requests.Session", side_effect=make_session), \
             patch("exchange_rates.providers.discover_currencies", return_value=catalog):
            with HttpClient(retries=0, interval=0, edge_dns_servers=SERVERS,
                            forbidden_threshold=1) as client:
                try:
                    if sharded:
                        result = fetch_shard("mastercard", DAY, {"recovery_rounds": 0}, client, 0, 1)
                    else:
                        result = fetch_day("mastercard", DAY, catalog, {"recovery_rounds": 0}, client)
                except (RecoveryError, AccessBlockedError) as exc:
                    result = exc
        self.assertTrue(all(session.close.called for session in sessions))
        for option in options:
            if "curl_options" in option:
                self.assertEqual(option["impersonate"], "chrome")
                self.assertEqual(option["curl_options"][CurlOpt.PROXY], "")
        return result, calls

    def test_last_edge_repairs_stale_pair_in_both_collection_paths(self):
        for sharded in (False, True):
            with self.subTest(sharded=sharded):
                result, calls = self.collect(dict(zip(IPS, ("old", "old", "fresh"))), sharded=sharded)
                self.assertNotIsInstance(result, Exception)
                self.assertEqual(calls, list(IPS))
                raw = result["raw"] if sharded else simplejson.loads(result.raw_parts["shard-01.json"])
                records = [r for r in raw["requests"] if "response" in r]
                self.assertEqual([r["request"]["edge_ip"] for r in records], list(IPS))
                self.assertEqual([r["response"]["primary_ip"] for r in records], list(IPS))
                self.assertIn("2026-03-26", records[0]["response"]["body_text"])
                self.assertNotIn("2026-03-26", records[-1]["response"]["body_text"])
        result, calls = self.collect({ip: "fresh" for ip in IPS})
        self.assertNotIsInstance(result, Exception)
        self.assertEqual(calls, [IPS[0]])

    def test_all_old_requires_every_edge_and_complete_dns(self):
        result, calls = self.collect({ip: "old" for ip in IPS})
        self.assertIsInstance(result, RecoveryError)
        self.assertEqual(calls, list(IPS))
        self.assertIn("all 3 resolved edges", str(result))
        self.assertIn("DateMismatchError", str(result))
        for complete, states in (
            (False, {ip: "old" for ip in IPS}),
            (True, dict(zip(IPS, ("old", "transport", "old")))),
            (True, dict(zip(IPS, ("old", "old_invalid", "old")))),
        ):
            with self.subTest(complete=complete, states=states):
                result, calls = self.collect(states, complete=complete)
                self.assertIsInstance(result, RecoveryError)
                self.assertEqual(calls, list(IPS))
                self.assertIn("edge verification incomplete", str(result))
                self.assertNotIn("all 3 resolved edges", str(result))

    def test_no_public_answers_never_falls_back_to_system_dns(self):
        result, calls = self.collect({}, addresses=(), complete=False)
        self.assertIsInstance(result, RecoveryError)
        self.assertEqual(calls, [])
        self.assertIn("no public edge addresses", str(result))

    def test_403_stops_without_rotating_to_another_edge(self):
        result, calls = self.collect(dict(zip(IPS, ("old", "403", "fresh"))))
        self.assertIsInstance(result, AccessBlockedError)
        self.assertEqual(calls, list(IPS[:2]))

    def test_catalog_transport_fallback_preserves_dns_and_http_evidence(self):
        good = http_response()
        good.primary_ip = IPS[1]
        with patch("exchange_rates.http.resolve_edges", return_value=pool()), \
             patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.side_effect = [requests.RequestsError("unreachable"), good]
            with HttpClient(retries=0, interval=0, edge_dns_servers=SERVERS) as client:
                result = client.request_json(URL)
                self.assertEqual(result.records[0]["dns"]["addresses"], list(IPS))
                self.assertEqual(result.records[1]["request"]["edge_ip"], IPS[0])
                self.assertIn("transport_error", result.records[1])
                self.assertEqual(result.records[2]["response"]["primary_ip"], IPS[1])
                good.primary_ip = IPS[0]
                factory.return_value.get.side_effect = None
                factory.return_value.get.return_value = good
                with self.assertRaisesRegex(HttpError, "did not use the pinned edge IP"):
                    client.request_json(URL, edge_ip=IPS[1])

    def test_pinned_sessions_reuse_only_within_one_ip_and_preserve_hostname(self):
        with patch("exchange_rates.http.requests.Session") as factory:
            factory.return_value.get.return_value = http_response()
            with HttpClient(retries=0, interval=0) as client:
                client.request_json(URL, edge_ip=IPS[0])
                client.request_json(URL, edge_ip=IPS[0])
                client.request_json(URL, edge_ip=IPS[1])
            pinned = [call for call in factory.call_args_list if "curl_options" in call.kwargs]
            self.assertEqual(len(pinned), 2)
            self.assertEqual(pinned[0].kwargs["curl_options"][CurlOpt.RESOLVE], [f"{HOST}:443:{IPS[0]}"])
            self.assertEqual(pinned[1].kwargs["curl_options"][CurlOpt.RESOLVE], [f"{HOST}:443:{IPS[1]}"])
            for call in factory.return_value.get.call_args_list:
                self.assertEqual(call.args[0], URL)
                self.assertFalse(call.kwargs["allow_redirects"])


if __name__ == "__main__":
    unittest.main()
