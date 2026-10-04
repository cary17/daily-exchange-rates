"""Daily provider adapters; every directional pair is fetched independently."""

from concurrent.futures import CancelledError
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import re
from typing import Any, Callable, Mapping, Sequence

import simplejson

from ..catalog import CurrencyCatalog, discover_currencies
from ..concurrency import SHARD_COUNT, balanced_shards, run_currency_shards
from ..http import AccessBlockedError, HttpClient, HttpError, JsonResponse, utc_now
from ..models import DayResult, json_bytes


class ProviderError(RuntimeError):
    """A provider response failed validation; no day result was committed."""


class NoDataError(ProviderError):
    """The requested day explicitly has no published data."""


class DateMismatchError(ProviderError):
    pass


class RecoveryError(ProviderError):
    """Bounded recovery exhausted; retain exact omissions and HTTP evidence."""

    def __init__(self, provider, missing_pairs, errors, raw_parts, recovery):
        self.missing_pairs = list(missing_pairs)
        self.errors = dict(errors)
        self.raw_parts = raw_parts
        self.recovery = recovery
        details = "; ".join(f"{spend}/{home}: {errors[spend, home]}"
                            for spend, home in missing_pairs)
        super().__init__(f"{provider}: {len(missing_pairs)} missing pairs after "
                         f"{recovery['rounds']} recovery rounds: {details}")


def _recovery_rounds(config):
    rounds = config.get("recovery_rounds", 2)
    if isinstance(rounds, bool) or not isinstance(rounds, int) or rounds < 0:
        raise ValueError("recovery_rounds must be a non-negative integer")
    return rounds


def _merge_records(primary, fallback):
    # The two logs may overlap. Match occurrences, not unique values: identical
    # transport attempts are distinct evidence and must remain distinct.
    merged = list(primary)
    unmatched = list(primary)
    for record in fallback:
        if record in unmatched:
            unmatched.remove(record)
        else:
            merged.append(record)
    return merged


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
    metadata["response_dates_index"] = "final_successful_responses"
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


SHARD_FORMAT = "exchange-rates-shard-v1"


def _code_sequence(value: Any, context: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise ProviderError(f"{context}: expected a currency sequence")
    codes = tuple(value)
    if not codes or any(not isinstance(code, str) or not re.fullmatch(r"[A-Z]{3}", code)
                        for code in codes) or len(set(codes)) != len(codes):
        raise ProviderError(f"{context}: expected unique three-letter currency codes")
    return codes


def _shard_bounds(shard_index: int, shard_count: int) -> None:
    if isinstance(shard_count, bool) or not isinstance(shard_count, int) or shard_count <= 0:
        raise ValueError("shard_count must be a positive integer")
    if (isinstance(shard_index, bool) or not isinstance(shard_index, int)
            or not 0 <= shard_index < shard_count):
        raise ValueError("shard_index must be within shard_count")


def _date_fields(provider: str, body_text: str) -> dict[str, Any]:
    try:
        decoded = simplejson.loads(body_text, use_decimal=True, allow_nan=False)
    except (ValueError, UnicodeError):
        return {}
    if not isinstance(decoded, Mapping):
        return {}
    fields: dict[str, Any] = {}
    objects = [("", decoded)]
    objects.extend((name + ".", decoded.get(name))
                   for name in ("originalValues", "data"))
    for prefix, obj in objects:
        if not isinstance(obj, Mapping):
            continue
        for key, value in obj.items():
            if key.lower().replace("_", "").replace("-", "") in _DATE_KEYS[provider]:
                fields[prefix + key] = value
    return fields


def fetch_shard(provider: str, target: date, config: Mapping[str, Any],
                client: HttpClient, shard_index: int,
                shard_count: int = SHARD_COUNT) -> dict[str, Any]:
    """Collect one currency shard in a single serial transport stream.

    Each shard owns its own process and egress address, so one serial stream
    per shard keeps every source address well below its burst limits.
    """
    if provider not in ("visa", "mastercard"):
        raise ValueError(f"Sharding is unsupported for provider: {provider}")
    if type(target) is not date:
        raise TypeError("target must be datetime.date, not datetime.datetime")
    if not isinstance(config, Mapping):
        raise TypeError("config must be an institution mapping")
    _shard_bounds(shard_index, shard_count)
    catalog = discover_currencies(provider, config, client)
    shards = balanced_shards(catalog.transaction, shard_count)
    spends = shards[shard_index] if shard_index < len(shards) else ()
    endpoint, headers = _settings(provider, config)
    expected = [(spend, home) for spend in spends for home in catalog.billing
                if spend != home]
    raw_shard: dict[str, Any] = {
        "provider": provider, "requested_date": target.isoformat(),
        "shard_index": shard_index, "transaction_currencies": list(spends),
        "requests": [], "attempts": [],
    }
    successful: dict[tuple[str, str], dict[str, Any]] = {}
    errors: dict[tuple[str, str], str] = {}
    dates: list[dict[str, Any]] = []
    rounds = 0
    initial_failed = 0
    for round_index in range(_recovery_rounds(config) + 1):
        pending = [pair for pair in expected if pair not in successful]
        if not pending:
            break
        for spend, home in pending:
            start = len(client.records)
            fallback: list[dict[str, Any]] = []
            error = None
            recorded = False
            try:
                row, response = _query_pair(
                    provider, target, spend, home, endpoint, headers, client)
            except AccessBlockedError as exc:
                # Record the denying exchange before wrapping, because finally
                # runs only after this handler raises.
                raw_shard["requests"].extend(client.records[start:])
                recorded = True
                raw_shard["attempts"].append({
                    "pair": [spend, home], "round": round_index,
                    "error": "AccessBlockedError: HTTP 403 circuit is open",
                })
                exc.raw_parts = {f"shard-{shard_index + 1:02d}.json":
                                 json_bytes(raw_shard, compact=True)}
                exc.context = {
                    "provider": provider, "requested_date": target.isoformat(),
                    "expected_pairs": len(expected),
                    "successful_pairs": len(successful),
                    "attempted_pairs": len(raw_shard["attempts"]),
                    "round": round_index, "shard_index": shard_index,
                    "stopped_for_access_control": True,
                }
                raise
            except (HttpError, ProviderError) as exc:
                error = f"{type(exc).__name__}: {exc}"
                errors[(spend, home)] = error
                if isinstance(exc, HttpError):
                    fallback = exc.records or (
                        _records(exc.response) if exc.response is not None else [])
            else:
                successful[(spend, home)] = row
                errors.pop((spend, home), None)
                fallback = _records(response)
                fields = _date_fields(provider, response.body_text)
                if fields:
                    dates.append({"fields": fields})
            finally:
                if not recorded:
                    raw_shard["requests"].extend(_merge_records(
                        client.records[start:], fallback))
            raw_shard["attempts"].append({
                "pair": [spend, home], "round": round_index, "error": error,
            })
        if round_index == 0:
            initial_failed = sum(1 for pair in expected if pair not in successful)
        else:
            rounds = round_index
    missing = [pair for pair in expected if pair not in successful]
    recovery = {"initial_failed": initial_failed, "rounds": rounds,
                "repaired": initial_failed - len(missing)}
    if missing:
        raise RecoveryError(provider, missing, errors,
                            {f"shard-{shard_index + 1:02d}.json":
                             json_bytes(raw_shard, compact=True)}, recovery)
    return {
        "format": SHARD_FORMAT, "provider": provider,
        "requested_date": target.isoformat(),
        "shard_index": shard_index, "shard_count": shard_count,
        "transaction_currencies": list(catalog.transaction),
        "billing_currencies": list(catalog.billing),
        "catalog_source_urls": [response.url for response in catalog.responses],
        "catalog_requests": [record for response in catalog.responses
                             for record in _records(response)],
        "response_dates": dates,
        "recovery": recovery,
        "rows": [{"transCur": pair[0], "baseCur": pair[1],
                  "rateData": str(successful[pair]["rateData"])} for pair in expected],
        "raw": raw_shard,
    }


def _bundle_rows(bundle: Mapping[str, Any], provider: str,
                 shard_index: int) -> list[dict[str, Any]]:
    context = f"Shard {shard_index}"
    if bundle.get("format") != SHARD_FORMAT:
        raise ProviderError(f"{context}: unexpected shard format")
    if bundle.get("provider") != provider:
        raise ProviderError(f"{context}: provider mismatch")
    rows = bundle.get("rows")
    if not isinstance(rows, list):
        raise ProviderError(f"{context}: missing rows")
    normalized = []
    for index, row in enumerate(rows):
        values = _object(row, f"{context} row {index}")
        spend, home = values.get("transCur"), values.get("baseCur")
        if any(not isinstance(code, str) or not re.fullmatch(r"[A-Z]{3}", code)
               for code in (spend, home)):
            raise ProviderError(f"{context} row {index}: invalid currency code")
        normalized.append({
            "transCur": spend, "baseCur": home,
            "rateData": _number(values.get("rateData"),
                                f"{context} row {index} rateData", positive=True),
        })
    return normalized


def merge_shards(provider: str, target: date,
                 bundles: Sequence[Mapping[str, Any]]) -> DayResult:
    """Verify complete, consistent shards and rebuild one atomic day result."""
    if provider not in ("visa", "mastercard"):
        raise ValueError(f"Sharding is unsupported for provider: {provider}")
    if type(target) is not date:
        raise TypeError("target must be datetime.date, not datetime.datetime")
    if isinstance(bundles, (str, bytes)) or not isinstance(bundles, Sequence) or not bundles:
        raise ProviderError("At least one shard bundle is required")
    indexed: dict[int, Mapping[str, Any]] = {}
    for bundle in bundles:
        if not isinstance(bundle, Mapping):
            raise ProviderError("Shard bundle must be a JSON object")
        index = bundle.get("shard_index")
        if isinstance(index, bool) or not isinstance(index, int):
            raise ProviderError("Shard bundle has no integer shard_index")
        if index in indexed:
            raise ProviderError(f"Duplicate shard index {index}")
        indexed[index] = bundle
    count = indexed[next(iter(indexed))].get("shard_count")
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        raise ProviderError("Shard bundle has no valid shard_count")
    if sorted(indexed) != list(range(count)):
        raise ProviderError(f"Expected shards 0..{count - 1}, got {sorted(indexed)}")
    first = indexed[0]
    transaction = _code_sequence(first.get("transaction_currencies"),
                                 "Shard catalog")
    billing = _code_sequence(first.get("billing_currencies"), "Shard catalog")
    catalog = CurrencyCatalog(transaction, billing)
    submitted: dict[tuple[str, str], dict[str, Any]] = {}
    raw_parts: dict[str, bytes] = {}
    source_urls: list[str] = []
    catalog_urls: list[str] = []
    catalog_requests: list[Any] = []
    dates: list[dict[str, Any]] = []
    rounds = initial_failed = repaired = 0
    for index in range(count):
        bundle = indexed[index]
        if bundle.get("requested_date") != target.isoformat():
            raise ProviderError(
                f"Shard {index}: requested_date={bundle.get('requested_date')!r}, "
                f"expected {target.isoformat()}")
        if tuple(bundle.get("transaction_currencies") or ()) != transaction:
            raise ProviderError(f"Shard {index}: transaction catalog differs")
        if tuple(bundle.get("billing_currencies") or ()) != billing:
            raise ProviderError(f"Shard {index}: billing catalog differs")
        for row in _bundle_rows(bundle, provider, index):
            pair = (row["transCur"], row["baseCur"])
            if pair in submitted:
                raise ProviderError(f"Shard {index}: duplicate pair {pair[0]}/{pair[1]}")
            if pair[0] == pair[1]:
                raise ProviderError(f"Shard {index}: identity pair {pair[0]}/{pair[1]}")
            submitted[pair] = row
        recovery = bundle.get("recovery")
        if isinstance(recovery, Mapping):
            shard_rounds = recovery.get("rounds")
            if isinstance(shard_rounds, int) and not isinstance(shard_rounds, bool):
                rounds = max(rounds, shard_rounds)
            shard_failed = recovery.get("initial_failed")
            if isinstance(shard_failed, int) and not isinstance(shard_failed, bool):
                initial_failed += shard_failed
            shard_repaired = recovery.get("repaired")
            if isinstance(shard_repaired, int) and not isinstance(shard_repaired, bool):
                repaired += shard_repaired
        raw = bundle.get("raw")
        if not isinstance(raw, Mapping):
            raise ProviderError(f"Shard {index}: missing raw records")
        raw_parts[f"shard-{index + 1:02d}.json"] = json_bytes(raw, compact=True)
        for record in raw.get("requests") or ():
            if not isinstance(record, Mapping):
                continue
            response = record.get("response") or {}
            # Published source URLs are the exact final request URLs, query included.
            if isinstance(response, Mapping) and isinstance(response.get("url"), str):
                source_urls.append(response["url"])
        for url in bundle.get("catalog_source_urls") or ():
            if isinstance(url, str):
                catalog_urls.append(url)
        for record in bundle.get("catalog_requests") or ():
            catalog_requests.append(record)
        for item in bundle.get("response_dates") or ():
            if isinstance(item, Mapping) and isinstance(item.get("fields"), Mapping):
                dates.append({"request_index": len(dates), "fields": dict(item["fields"])})
    expected = list(catalog.pairs())
    for pair in expected:
        if pair not in submitted:
            raise ProviderError(
                f"Missing pair {pair[0]}/{pair[1]} after merging {count} shards")
    rows = [submitted[pair] for pair in expected]
    if len(rows) != catalog.pair_count:
        raise ProviderError("Merged pairs do not match the catalog")
    metadata: dict[str, Any] = {
        "provider": provider, "requested_date": target.isoformat(),
        "fetched_at_utc": utc_now(), "pair_count": len(rows),
        "rate_direction": "base currency per one transaction currency",
        "source_urls": list(dict.fromkeys(source_urls)),
        "response_dates": dates,
        "response_dates_index": "final_successful_responses",
        "transaction_currencies": list(catalog.transaction),
        "billing_currencies": list(catalog.billing),
        "shard_count": count,
        "shards": [{"index": index, "transaction_currencies": list(codes),
                    "pair_count": sum(1 for spend in codes
                                      for home in catalog.billing if spend != home)}
                   for index, codes in enumerate(balanced_shards(catalog.transaction, count))],
        "catalog_source_urls": list(dict.fromkeys(catalog_urls)),
        "recovery": {"initial_failed": initial_failed, "rounds": rounds,
                     "repaired": repaired},
    }
    raw = json_bytes({
        "format": "exchange-rates-raw-shards-v1",
        "provider": provider, "requested_date": target.isoformat(),
        "parts": [{"name": name, "size": len(body),
                   "sha256": hashlib.sha256(body).hexdigest()}
                  for name, body in sorted(raw_parts.items())],
        "catalog_requests": catalog_requests,
    })
    return DayResult(provider, target,
                     json_bytes({"exchangeRateJson": rows}), raw, metadata, raw_parts)


def _unionpay(target: date, currencies: Sequence[str],
              config: Mapping[str, Any], client: HttpClient, *,
              records: list | None = None) -> DayResult:
    endpoint, headers = _settings("unionpay", config)
    url = endpoint.format(date=target.strftime("%Y%m%d"))
    transport_records = client.records if isinstance(client.records, list) else []
    start = len(transport_records)
    fallback = []
    try:
        response = client.request_json(url, headers=headers)
        fallback = _records(response)
    except HttpError as exc:
        fallback = exc.records or (_records(exc.response) if exc.response is not None else [])
        if exc.status_code == 404:
            raise NoDataError(f"UnionPay has no data for {target.isoformat()}") from exc
        raise
    finally:
        if records is not None:
            records.extend(_merge_records(transport_records[start:], fallback))
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
    expected = list(catalog.pairs())
    successful, errors = {}, {}
    raw_shards = [{
        "provider": provider, "requested_date": target.isoformat(), "shard_index": index,
        "transaction_currencies": list(spends), "requests": [], "attempts": [],
    } for index, spends in enumerate(balanced_shards(catalog.transaction))]
    initial_failed = 0
    rounds = 0
    for round_index in range(_recovery_rounds(config) + 1):
        pending = set(expected) - successful.keys()

        def worker(index, spends, cancelled):
            results, failures = {}, {}
            raw = raw_shards[index]
            with client.fork(cancelled) as shard_client:
                for spend in spends:
                    for home in catalog.billing:
                        pair = (spend, home)
                        if pair not in pending:
                            continue
                        if cancelled.is_set():
                            raise CancelledError("Currency shard interrupted")
                        start = len(shard_client.records)
                        fallback = []
                        error = None
                        try:
                            row, response = _query_pair(
                                provider, target, spend, home, endpoint, headers, shard_client)
                            results[pair] = (row, response)
                            fallback = _records(response)
                        except AccessBlockedError as exc:
                            if exc.request_sent:
                                fallback = exc.records or (
                                    _records(exc.response) if exc.response is not None else [])
                            raw["attempts"].append({
                                "pair": list(pair), "round": round_index,
                                "error": f"{type(exc).__name__}: {exc}",
                                "blocked_before_request": not exc.request_sent,
                            })
                            raise
                        except (HttpError, ProviderError) as exc:
                            error = f"{type(exc).__name__}: {exc}"
                            failures[pair] = error
                            if isinstance(exc, HttpError):
                                fallback = exc.records or (
                                    _records(exc.response) if exc.response is not None else [])
                        finally:
                            raw["requests"].extend(_merge_records(
                                shard_client.records[start:], fallback))
                        raw["attempts"].append({
                            "pair": list(pair), "round": round_index, "error": error,
                        })
            return results, failures

        # Expected failures stay inside workers; unexpected exceptions and
        # interrupts retain run_currency_shards' cancellation/propagation path.
        try:
            batches = run_currency_shards(catalog.transaction, worker)
        except AccessBlockedError as exc:
            exc.raw_parts = {
                f"shard-{index + 1:02d}.json": json_bytes(raw, compact=True)
                for index, raw in enumerate(raw_shards)
            }
            attempted = sum(
                not attempt.get("blocked_before_request", False)
                for raw in raw_shards for attempt in raw["attempts"]
            )
            completed = sum(
                attempt["error"] is None
                for raw in raw_shards for attempt in raw["attempts"]
            )
            exc.context = {
                "provider": provider, "requested_date": target.isoformat(),
                "expected_pairs": catalog.pair_count, "successful_pairs": completed,
                "attempted_pairs": attempted, "round": round_index,
                "stopped_for_access_control": True,
            }
            raise
        for results, failures in batches:
            successful.update(results)
            errors.update(failures)
            for pair in results:
                errors.pop(pair, None)
        missing = [pair for pair in expected if pair not in successful]
        if round_index == 0:
            initial_failed = len(missing)
        else:
            rounds = round_index
        if not missing:
            break

    recovery = {"initial_failed": initial_failed, "rounds": rounds,
                "repaired": initial_failed - len(missing)}
    parts = {f"shard-{index + 1:02d}.json": json_bytes(raw, compact=True)
             for index, raw in enumerate(raw_shards)}
    if missing:
        raise RecoveryError(provider, missing, errors, parts, recovery)
    rows = [successful[pair][0] for pair in expected]
    responses = [successful[pair][1] for pair in expected]
    result = _result(provider, target, json_bytes({"exchangeRateJson": rows}), responses,
                     len(rows), catalog=catalog, raw_parts=parts)
    result.metadata["recovery"] = recovery
    return result


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
    recovery_rounds = _recovery_rounds(config)
    if provider != "unionpay":
        return handler(target, currencies, config, client)
    records, attempts = [], []
    for round_index in range(recovery_rounds + 1):
        try:
            result = _unionpay(target, currencies, config, client, records=records)
        except AccessBlockedError as exc:
            exc.records = records
            exc.attempts = attempts
            raise
        except (HttpError, ProviderError) as exc:
            attempts.append({"pair": None, "round": round_index,
                             "error": f"{type(exc).__name__}: {exc}"})
            if round_index == recovery_rounds:
                exc.records = records
                exc.attempts = attempts
                raise
        else:
            attempts.append({"pair": None, "round": round_index, "error": None})
            result.raw = json_bytes({
                "provider": provider, "requested_date": target.isoformat(),
                "requests": records, "attempts": attempts,
            })
            result.metadata["recovery"] = {
                "initial_failed": int(round_index > 0), "rounds": round_index,
                "repaired": int(round_index > 0), "unit": "file",
            }
            return result
