"""Daily provider adapters; every directional pair is fetched independently."""

from concurrent.futures import CancelledError
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import re
from typing import Any, Callable, Mapping, Sequence

from ..catalog import CurrencyCatalog, discover_currencies
from ..concurrency import SHARD_COUNT, balanced_shards, run_currency_shards
from ..http import HttpClient, HttpError, JsonResponse, utc_now
from ..models import DayResult, json_bytes


class ProviderError(RuntimeError):
    """A provider response failed validation; no day result was committed."""


class NoDataError(ProviderError):
    """The requested day explicitly has no published data."""


class DateMismatchError(ProviderError):
    pass


_DEFAULTS = {
    "unionpay": (
        "https://www.unionpayintl.com/upload/jfimg/{date}.json", None,
    ),
    "visa": (
        "https://www.visa.co.in/cmsapi/fx/rates",
        "https://www.visa.co.in/support/consumer/travel-support/exchange-rate-calculator.html",
    ),
    "mastercard": (
        "https://www.mastercard.com/marketingservices/public/mccom-services/currency-conversions/conversion-rates",
        "https://www.mastercard.com/in/en/personal/get-support/currency-exchange-rate-converter.html",
    ),
}
# Only effective rate dates: publication and wall-clock dates are unrelated.
_DATE_KEYS = {
    "unionpay": {"date", "curdate", "exchangedate", "ratedate", "exchangeratedate"},
    "visa": {"conversioninputdate", "asofdate", "exchangedate", "utcconverteddate"},
    "mastercard": {"fxdate", "exchangedate", "conversiondate"},
}
_AMOUNT = Decimal("1000")


def _object(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProviderError(f"{context}: expected a JSON object")
    return value


def _number(value: Any, context: str, *, allow_string: bool = True,
            positive: bool = True) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ProviderError(f"{context}: missing or invalid number")
    if not isinstance(value, (Decimal, int, float, str)):
        raise ProviderError(f"{context}: invalid number type")
    if isinstance(value, str) and not allow_string:
        raise ProviderError(f"{context}: expected a JSON number, not a string")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ProviderError(f"{context}: invalid decimal number") from exc
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise ProviderError(f"{context}: expected a finite {'positive' if positive else 'non-negative'} number")
    return number


def _check_dates(provider: str, target: date, *objects: Mapping[str, Any]) -> None:
    for obj in objects:
        for key, value in obj.items():
            canonical = key.lower().replace("_", "").replace("-", "")
            if canonical not in _DATE_KEYS[provider]:
                continue
            parsed = None
            if provider == "visa" and canonical == "asofdate":
                if isinstance(value, int) and not isinstance(value, bool):
                    try:
                        parsed = datetime.fromtimestamp(value, timezone.utc).date()
                    except (ValueError, OverflowError, OSError):
                        pass
            elif isinstance(value, str):
                text = value.strip()
                for pattern in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y"):
                    try:
                        parsed = datetime.strptime(text, pattern).date()
                        break
                    except ValueError:
                        pass
                if parsed is None and len(text) > 10 and text[10] in "T ":
                    try:
                        parsed = datetime.fromisoformat(text).date()
                    except ValueError:
                        pass
            if parsed is None:
                raise ProviderError(f"Unrecognized explicit response date {key}={value!r}")
            if parsed != target:
                raise DateMismatchError(
                    f"Requested {target.isoformat()}, response {key}={value!r}",
                )


def _settings(provider: str, config: Mapping[str, Any]) -> tuple[str, dict[str, str]]:
    endpoint, referer = _DEFAULTS[provider]
    endpoint = config.get("endpoint", endpoint)
    referer = config.get("referer", referer)
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ValueError("endpoint must be a non-empty string")
    headers = {"Accept": "application/json, text/plain, */*"}
    if referer is not None:
        if not isinstance(referer, str):
            raise ValueError("referer must be a string or null")
        headers["Referer"] = referer
    return endpoint, headers


def _records(response: JsonResponse) -> list[dict[str, Any]]:
    return response.records or [response.as_record()]


def _result(provider: str, target: date, normalized: bytes,
            responses: list[JsonResponse], pair_count: int,
            currencies: Sequence[str] | None = None, *,
            catalog: CurrencyCatalog | None = None,
            raw_parts: dict[str, bytes] | None = None) -> DayResult:
    metadata: dict[str, Any] = {
        "provider": provider, "requested_date": target.isoformat(),
        "fetched_at_utc": utc_now(), "pair_count": pair_count,
        "rate_direction": "base currency per one transaction currency",
        "source_urls": list(dict.fromkeys(response.url for response in responses)),
    }
    returned_dates = []
    for index, response in enumerate(responses):
        fields = {}
        objects = [("", response.data)]
        if isinstance(response.data, Mapping):
            objects.extend((name + ".", response.data.get(name)) for name in ("originalValues", "data"))
        for prefix, obj in objects:
            if isinstance(obj, Mapping):
                for key, value in obj.items():
                    if key.lower().replace("_", "").replace("-", "") in _DATE_KEYS[provider]:
                        fields[prefix + key] = value
        if fields:
            returned_dates.append({"request_index": index, "fields": fields})
    metadata["response_dates"] = returned_dates
    if currencies is not None:
        metadata["currencies"] = list(currencies)
    if raw_parts is None:
        raw = json_bytes({
            "provider": provider, "requested_date": target.isoformat(),
            "requests": [record for response in responses for record in _records(response)],
        })
        return DayResult(provider, target, normalized, raw, metadata)
    assert catalog is not None
    metadata.update({
        "transaction_currencies": list(catalog.transaction),
        "billing_currencies": list(catalog.billing),
        "shard_count": SHARD_COUNT,
        "shards": [{"index": index, "transaction_currencies": list(codes),
                    "pair_count": sum(1 for spend in codes for home in catalog.billing if spend != home)}
                   for index, codes in enumerate(balanced_shards(catalog.transaction))],
        "catalog_source_urls": [response.url for response in catalog.responses],
    })
    raw = json_bytes({
        "format": "exchange-rates-raw-shards-v1",
        "provider": provider, "requested_date": target.isoformat(),
        "parts": [{"name": name, "size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                  for name, body in sorted(raw_parts.items())],
        "catalog_requests": [record for response in catalog.responses for record in _records(response)],
    })
    return DayResult(provider, target, normalized, raw, metadata, raw_parts)


def _unionpay(target: date, currencies: Sequence[str],
              config: Mapping[str, Any], client: HttpClient) -> DayResult:
    endpoint, headers = _settings("unionpay", config)
    url = endpoint.format(date=target.strftime("%Y%m%d"))
    try:
        response = client.request_json(url, headers=headers)
    except HttpError as exc:
        if exc.status_code == 404:
            raise NoDataError(f"UnionPay has no data for {target.isoformat()}") from exc
        raise
    data = _object(response.data, "UnionPay")
    _check_dates("unionpay", target, data)
    rows = data.get("exchangeRateJson")
    if not isinstance(rows, list):
        raise ProviderError("UnionPay: exchangeRateJson must be an array")
    if not rows:
        raise NoDataError(f"UnionPay published an empty day for {target.isoformat()}")
    seen = set()
    for index, value in enumerate(rows):
        row = _object(value, f"UnionPay row {index}")
        pair = (row.get("transCur"), row.get("baseCur"))
        if any(not isinstance(code, str) or not re.fullmatch(r"[A-Z]{3}", code)
               for code in pair):
            raise ProviderError(f"UnionPay row {index}: invalid currency code")
        if pair in seen:
            raise ProviderError(f"UnionPay row {index}: duplicate pair {pair}")
        seen.add(pair)
        _number(row.get("rateData"), f"UnionPay row {index} rateData",
                allow_string=False)
        _check_dates("unionpay", target, row)
    return _result("unionpay", target, response.body, [response], len(rows))


def _check_bill(rate: Decimal, value: Any, context: str) -> None:
    bill = _number(value, context, positive=False)
    with localcontext() as ctx:
        ctx.prec = max(80, len(rate.as_tuple().digits) + len(bill.as_tuple().digits) + 10)
        # Respect displayed decimal places, with a half-unit cap for integers.
        quantum = Decimal(1).scaleb(min(bill.as_tuple().exponent, 0))
        tolerance = quantum / 2
        if abs(rate * _AMOUNT - bill) > tolerance:
            raise ProviderError(f"{context}: billed amount disagrees with directional rate")


def _query_pair(provider: str, target: date, spend: str, home: str,
                endpoint: str, headers: Mapping[str, str], client: HttpClient):
    if provider == "visa":
        day = target.strftime("%m/%d/%Y")
        params = {"amount": "1000", "fee": "0", "utcConvertedDate": day,
                  "exchangedate": day, "fromCurr": home, "toCurr": spend}
    else:
        params = {"exchange_date": target.isoformat(), "transaction_currency": spend,
                  "cardholder_billing_currency": home, "bank_fee": "0", "transaction_amount": "1000"}
    response = client.request_json(endpoint, params=params, headers=headers)
    data = _object(response.data, f"{provider} {spend}/{home}")
    _check_dates(provider, target, data)
    if provider == "visa":
        if data.get("status") != "success":
            raise ProviderError(f"Visa {spend}/{home}: status is not success: {data.get('status')!r}")
        values = _object(data.get("originalValues"), "Visa originalValues")
        rate = _number(values.get("fxRateVisa"), "Visa fxRateVisa")
        _check_bill(rate, values.get("toAmountWithVisaRate"), "Visa toAmountWithVisaRate")
    else:
        values = _object(data.get("data"), "Mastercard data")
        if values.get("errorCode") not in (None, "", 0, "0"):
            raise ProviderError(f"Mastercard {spend}/{home}: errorCode={values['errorCode']!r}")
        rate = _number(values.get("conversionRate"), "Mastercard conversionRate")
        _check_bill(rate, values.get("crdhldBillAmt"), "Mastercard crdhldBillAmt")
    _check_dates(provider, target, values)
    expected_currencies = ({"fromCurrency": spend, "toCurrency": home} if provider == "visa"
                           else {"transCurr": spend, "crdhldBillCurr": home})
    for field, expected in expected_currencies.items():
        if field in values and values[field] != expected:
            raise ProviderError(f"{provider} {spend}/{home}: response {field}={values[field]!r}, expected {expected}")
    return {"transCur": spend, "baseCur": home, "rateData": rate}, response


def _card(provider: str, target: date, currencies: CurrencyCatalog | Sequence[str] | None,
          config: Mapping[str, Any], client: HttpClient) -> DayResult:
    if currencies is None:
        catalog = discover_currencies(provider, config, client)
    elif isinstance(currencies, CurrencyCatalog):
        catalog = currencies
    else:
        catalog = CurrencyCatalog(currencies, currencies)
    endpoint, headers = _settings(provider, config)
    # Fail an unavailable date before starting all nine workers; reuse this pair.
    first_pair = next(catalog.pairs())
    seed = _query_pair(provider, target, *first_pair, endpoint, headers, client)

    def worker(index, spends, cancelled):
        rows, responses = [], []
        if spends:
            with client.fork(cancelled) as shard_client:
                for spend in spends:
                    for home in catalog.billing:
                        if spend == home:
                            continue
                        if cancelled.is_set():
                            raise CancelledError("Another currency shard failed")
                        if (spend, home) == first_pair:
                            row, response = seed
                        else:
                            shard_client.records.clear()
                            row, response = _query_pair(provider, target, spend, home, endpoint, headers, shard_client)
                        rows.append(row)
                        responses.append(response)
        raw = json_bytes({
            "provider": provider, "requested_date": target.isoformat(), "shard_index": index,
            "transaction_currencies": list(spends),
            "requests": [record for response in responses for record in _records(response)],
        }, compact=True)
        return rows, responses, raw

    batches = run_currency_shards(catalog.transaction, worker)
    rows = [row for batch in batches for row in batch[0]]
    responses = [response for batch in batches for response in batch[1]]
    received = [(row["transCur"], row["baseCur"]) for row in rows]
    if received != list(catalog.pairs()) or len(received) != catalog.pair_count:
        raise ProviderError("Currency shard merge contains missing, duplicate or out-of-order pairs")
    parts = {f"shard-{index + 1:02d}.json": batch[2] for index, batch in enumerate(batches)}
    return _result(provider, target, json_bytes({"exchangeRateJson": rows}), responses,
                   len(rows), catalog=catalog, raw_parts=parts)


def _visa(target: date, currencies: Sequence[str], config: Mapping[str, Any],
          client: HttpClient) -> DayResult:
    return _card("visa", target, currencies, config, client)


def _mastercard(target: date, currencies: Sequence[str], config: Mapping[str, Any],
                client: HttpClient) -> DayResult:
    return _card("mastercard", target, currencies, config, client)


_REGISTRY: dict[str, Callable[[date, Sequence[str], Mapping[str, Any], HttpClient], DayResult]] = {
    "unionpay": _unionpay, "visa": _visa, "mastercard": _mastercard,
}


def provider_ids() -> tuple[str, ...]:
    return tuple(_REGISTRY)


def prepare_catalog(provider: str, config: Mapping[str, Any], client: HttpClient) -> CurrencyCatalog | None:
    if provider not in _REGISTRY:
        raise ValueError(f"Unknown provider: {provider}")
    if provider in ("visa", "mastercard"):
        return discover_currencies(provider, config, client)
    return None


def fetch_day(provider: str, target: date, currencies: CurrencyCatalog | Sequence[str] | None,
              config: Mapping[str, Any], client: HttpClient) -> DayResult:
    if type(target) is not date:
        raise TypeError("target must be datetime.date, not datetime.datetime")
    if not isinstance(config, Mapping):
        raise TypeError("config must be an institution mapping")
    try:
        handler = _REGISTRY[provider]
    except KeyError as exc:
        raise ValueError(f"Unknown provider: {provider}") from exc
    return handler(target, currencies, config, client)
