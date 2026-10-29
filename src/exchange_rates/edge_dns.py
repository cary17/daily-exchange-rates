"""Regional recursive DNS answers used to pin browser-fingerprinted requests."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from ipaddress import ip_address
import time

import dns.edns
import dns.exception
import dns.resolver


@dataclass
class EdgeResolution:
    addresses: tuple[str, ...]
    queries: list[dict]
    expires_at: float

    @property
    def complete(self) -> bool:
        return bool(self.queries) and all("error" not in query for query in self.queries)


def resolve_edges(host: str, servers: tuple[str, ...]) -> EdgeResolution:
    def query(server):
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = [server]
        resolver.timeout = 2
        resolver.use_edns(edns=0, payload=1232,
                          options=[dns.edns.ECSOption("0.0.0.0", 0)])
        records, expirations = [], []
        for kind in ("A", "AAAA"):
            record = {"server": server, "type": kind, "addresses": []}
            try:
                answer = resolver.resolve(host, kind, search=False, lifetime=3,
                                          raise_on_no_answer=False)
                record["canonical_name"] = str(answer.canonical_name)
                record["ttl"] = max(0, answer.expiration - time.time())
                expirations.append(answer.expiration)
                rejected = []
                for item in answer:
                    address = ip_address(item.address)
                    if not address.is_global or address.is_multicast:
                        rejected.append(str(address))
                    else:
                        record["addresses"].append(str(address))
                if rejected:
                    record["rejected_addresses"] = rejected
                    record["error"] = "Non-public DNS answers (possible Fake-IP)"
            except (dns.exception.DNSException, OSError, ValueError) as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
            records.append(record)
        return records, expirations

    with ThreadPoolExecutor(max_workers=len(servers)) as executor:
        results = list(executor.map(query, servers))
    queries = [record for records, _ in results for record in records]
    addresses = tuple(dict.fromkeys(address for record in queries
                                    for address in record["addresses"]))
    # Prefer IPv4 without dropping IPv6 candidates from date verification.
    addresses = tuple(sorted(addresses, key=lambda address: ip_address(address).version))
    expirations = [expiry for _, values in results for expiry in values]
    if not addresses or any("error" in record for record in queries):
        expirations.append(time.time() + 3)
    return EdgeResolution(addresses, queries,
                          min(expirations, default=time.time() + 3))
