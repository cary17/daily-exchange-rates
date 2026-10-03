"""Provider results and lossless JSON serialization."""

from dataclasses import dataclass
from datetime import date
from typing import Any

import simplejson


@dataclass
class DayResult:
    provider: str
    requested_date: date
    normalized: bytes
    raw: bytes
    metadata: dict[str, Any]


def json_bytes(obj: Any) -> bytes:
    return (simplejson.dumps(
        obj, use_decimal=True, ensure_ascii=False, indent=2, sort_keys=True,
        allow_nan=False,
    ) + "\n").encode("utf-8")
