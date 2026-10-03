"""Provider results and lossless JSON serialization."""

from dataclasses import dataclass, field
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
    raw_parts: dict[str, bytes] = field(default_factory=dict)


def json_bytes(obj: Any, *, compact: bool = False) -> bytes:
    return (simplejson.dumps(
        obj, use_decimal=True, ensure_ascii=False, indent=None if compact else 2,
        sort_keys=True, separators=(",", ":") if compact else None, allow_nan=False,
    ) + "\n").encode("utf-8")
