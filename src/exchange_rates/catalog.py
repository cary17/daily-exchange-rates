"""Official calculator currency catalogs, kept separate for each direction."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html.parser import HTMLParser
import re

import simplejson

from .http import HttpClient, JsonResponse


class CatalogError(RuntimeError):
    pass


def _codes(values: Sequence[str], label: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise CatalogError(f"{label}: expected a currency sequence")
    codes = tuple(values)
    if not codes or any(not isinstance(code, str) or not re.fullmatch(r"[A-Z]{3}", code) for code in codes):
        raise CatalogError(f"{label}: expected non-empty three-letter currency codes")
    if len(set(codes)) != len(codes):
        raise CatalogError(f"{label}: duplicate currency codes")
    return codes


@dataclass(frozen=True)
class CurrencyCatalog:
    transaction: tuple[str, ...]
    billing: tuple[str, ...]
    responses: tuple[JsonResponse, ...] = ()

    def __post_init__(self):
        object.__setattr__(self, "transaction", _codes(self.transaction, "Transaction currencies"))
        object.__setattr__(self, "billing", _codes(self.billing, "Billing currencies"))
        if not self.pair_count:
            raise CatalogError("Catalog does not contain any non-identity conversion")

    @property
    def pair_count(self) -> int:
        return len(self.transaction) * len(self.billing) - len(set(self.transaction) & set(self.billing))

    def pairs(self):
        for spend in self.transaction:
            for home in self.billing:
                if spend != home:
                    yield spend, home


class _VisaCalculator(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.catalogs = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "dm-calculator":
            return
        content = dict(attrs).get("content")
        if content is None:
            return
        try:
            data = simplejson.loads(content, use_decimal=True, allow_nan=False)
        except ValueError as exc:
            raise CatalogError("Visa calculator content is not valid JSON") from exc
        if isinstance(data, Mapping) and "currencyList" in data:
            self.catalogs.append(data["currencyList"])


def parse_visa_currencies(html: str) -> tuple[str, ...]:
    parser = _VisaCalculator()
    parser.feed(html)
    if len(parser.catalogs) != 1 or not isinstance(parser.catalogs[0], list):
        raise CatalogError("Visa official currencyList was not uniquely found")
    codes = []
    for item in parser.catalogs[0]:
        if not isinstance(item, Mapping) or not isinstance(item.get("key"), str):
            raise CatalogError("Visa currencyList contains a malformed entry")
        code = item["key"]
        if code != "None":
            codes.append(code)
    return tuple(sorted(_codes(codes, "Visa currencyList")))


def parse_mastercard_currencies(data) -> tuple[str, ...]:
    if not isinstance(data, Mapping) or not isinstance(data.get("data"), Mapping):
        raise CatalogError("Mastercard currency response has no data object")
    payload = data["data"]
    if payload.get("errorCode") not in (None, "", 0, "0"):
        raise CatalogError(f"Mastercard currency catalog error: {payload['errorCode']}")
    rows = payload.get("currencies")
    if not isinstance(rows, list):
        raise CatalogError("Mastercard currency response has no currencies array")
    codes = []
    for item in rows:
        if not isinstance(item, Mapping) or not isinstance(item.get("alphaCd"), str):
            raise CatalogError("Mastercard currencies contains a malformed entry")
        codes.append(item["alphaCd"])
    return tuple(sorted(_codes(codes, "Mastercard currencies")))


_CATALOG_URLS = {
    "visa": "https://www.visa.co.in/support/consumer/travel-support/exchange-rate-calculator.html",
    "mastercard": "https://www.mastercard.com/marketingservices/public/mccom-services/currency-conversions/currencies",
}


def discover_currencies(provider: str, config: Mapping, client: HttpClient) -> CurrencyCatalog:
    if provider not in _CATALOG_URLS:
        raise CatalogError(f"No official currency catalog adapter for {provider}")
    url = config.get("currency_endpoint", _CATALOG_URLS[provider])
    if not isinstance(url, str) or not url:
        raise CatalogError("currency_endpoint must be a non-empty URL")
    headers = {"Accept": "application/json, text/plain, */*"}
    if config.get("referer"):
        headers["Referer"] = config["referer"]
    if provider == "visa":
        response = client.request_text(url, headers=headers)
        codes = parse_visa_currencies(response.body_text)
    else:
        response = client.request_json(url, headers=headers)
        codes = parse_mastercard_currencies(response.data)
    # Both current official UIs feed the same catalog to their two selectors.
    return CurrencyCatalog(codes, codes, (response,))
