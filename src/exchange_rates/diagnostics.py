"""Local diagnostic artifacts, separate from published exchange-rate data."""

from __future__ import annotations

import base64
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from .models import json_bytes


def short_text(value: Any, limit: int = 240) -> str:
    text = str(value)
    return " ".join(text[:limit].split()) + ("..." if len(text) > limit else "")


def json_value(value: Any) -> Any:
    """Keep context JSON-compatible without importing transport exceptions."""
    if hasattr(value, "as_record"):
        return json_value(value.as_record())
    if isinstance(value, BaseException):
        return exception_details(value)
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            return [{"key": json_value(key), "value": json_value(item)}
                    for key, item in value.items()]
        return {key: ("[REDACTED]" if key.lower() in {
            "authorization", "proxy-authorization", "cookie", "set-cookie", "github_token",
        } else json_value(item)) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, bytes):
        return {"encoding": "base64", "data": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def exception_details(error: BaseException, seen: set[int] | None = None) -> dict:
    seen = set() if seen is None else seen
    if id(error) in seen:
        return {"type": type(error).__name__, "error": "[cyclic exception reference]"}
    seen.add(id(error))
    details = {"type": type(error).__name__, "error": str(error)}
    for name in ("missing_pairs", "errors", "recovery", "response", "records",
                 "attempts", "status_code", "last_status", "request_sent", "context"):
        if hasattr(error, name):
            details[name] = json_value(getattr(error, name))
    if error.__cause__ is not None:
        details["cause"] = exception_details(error.__cause__, seen)
    return details


def http_counts(records: list) -> dict[str, int]:
    counts = Counter()
    for record in records:
        if not isinstance(record, Mapping):
            continue
        response = record.get("response") or {}
        status = response.get("status_code") if isinstance(response, Mapping) else None
        if status is not None:
            counts[str(status)] += 1
        elif record.get("transport_error"):
            counts["transport_error"] += 1
    return dict(sorted(counts.items()))


class Diagnostics:
    def __init__(self, directory: Path, data_dir: Path | None = None):
        self.directory = Path(directory)
        self.data_dir = Path(data_dir) if data_dir is not None else None
        self.days: list[dict] = []
        self.fatal: dict | None = None
        self.records: list = []

    def display(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(Path.cwd()))
        except ValueError:
            return str(path)

    def _root(self) -> Path:
        root = self.directory.resolve()
        cwd = Path.cwd().resolve()
        if ".git" in root.parts or root == cwd or root in cwd.parents:
            raise ValueError("Diagnostics must use a dedicated directory outside .git")
        if self.data_dir is not None:
            data = self.data_dir.resolve()
            if root == data or data in root.parents or root in data.parents:
                raise ValueError("Diagnostics and published data directories must not overlap")
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _write(self, relative: Path, body: bytes) -> Path:
        root = self._root()
        path = root / relative
        if root not in path.resolve().parents or ".git" in relative.parts:
            raise ValueError("Diagnostic artifact escapes its dedicated directory")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return self.directory / relative

    def failure(self, provider: str, target: str | None, error: BaseException,
                records: list | None = None, *, fatal: bool = False) -> dict:
        details = {"provider": provider, "target": target, **exception_details(error)}
        all_records = list(records or [])
        parts = {}
        current, seen = error, set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            parts.update(getattr(current, "raw_parts", {}) or {})
            if not all_records:
                all_records = list(getattr(current, "records", []) or [])
                response = getattr(current, "response", None)
                if (not all_records and getattr(current, "request_sent", True)
                        and response is not None and hasattr(response, "as_record")):
                    all_records = [response.as_record()]
            current = current.__cause__
        raw_records = []
        for body in parts.values():
            try:
                value = json.loads(body)
            except (ValueError, UnicodeError, TypeError):
                continue
            if isinstance(value, dict):
                raw_records.extend(value.get("requests", []))
        if raw_records:
            all_records = raw_records
        if all_records and not details.get("records"):
            details["records"] = json_value(all_records)
        counts = http_counts(all_records)
        details["http_counts"] = counts
        missing = getattr(error, "missing_pairs", [])
        samples = list(getattr(error, "errors", {}).items())[:3]
        context = getattr(error, "context", {}) or {}
        stopped = isinstance(context, Mapping) and context.get("stopped_for_access_control")
        incomplete = (context.get("expected_pairs", 0) - context.get("successful_pairs", 0)) if stopped else len(missing)
        brief = f"{type(error).__name__}: failed {len(missing)} pairs" if missing else type(error).__name__
        if stopped:
            brief += (f"; stopped early for access control; completed "
                      f"{context.get('successful_pairs', 0)}/{context.get('expected_pairs', 0)} pairs; "
                      f"uncompleted={incomplete}; attempted={context.get('attempted_pairs', 0)}")
        if counts:
            brief += "; HTTP " + ", ".join(f"{key}={count}" for key, count in counts.items())
        if samples:
            brief += "; samples: " + "; ".join(
                f"{'/'.join(pair) if isinstance(pair, tuple) else short_text(pair)}: {short_text(message)}"
                for pair, message in samples)
        elif not missing:
            brief += ": " + short_text(details["error"])
        entry = {"provider": provider, "target": target, "status": "failed",
                 "type": type(error).__name__, "failed_pairs": len(missing) if not stopped else None,
                 "uncompleted_pairs": incomplete, "stopped_for_access_control": bool(stopped),
                 "http_counts": counts}
        folder = Path(provider or "task") / ("run-error" if fatal else target or "run-error")
        try:
            details["raw_parts"] = []
            for index, (name, body) in enumerate(parts.items()):
                # Preserve bytes; untrusted names never become arbitrary paths.
                filename = name if isinstance(name, str) and Path(name).name == name and name not in {".", "..", ".git"} else f"part-{index:04d}.bin"
                path = self._write(folder / "raw" / filename, body)
                details["raw_parts"].append({"name": name, "path": self.display(path),
                                             "size": len(body), "sha256": hashlib.sha256(body).hexdigest()})
            response = getattr(error, "response", None)
            if response is not None and isinstance(getattr(response, "body", None), bytes):
                path = self._write(folder / "raw" / "response.bin", response.body)
                details["response_body_path"] = self.display(path)
            report = self._write(folder / "report.json", json_bytes(details))
            entry["report"] = self.display(report)
            brief += f"; report: {entry['report']}"
        except Exception as write_error:
            entry["diagnostic_error"] = short_text(write_error)
            brief += f"; diagnostics unavailable: {entry['diagnostic_error']}"
        entry["brief"] = brief
        if fatal:
            self.fatal = entry
        else:
            self.days.append(entry)
        return entry

    def success(self, result) -> dict:
        entry = {"provider": result.provider, "target": result.requested_date.isoformat(),
                 "status": "saved", "pair_count": result.metadata.get("pair_count", 0),
                 "metadata": json_value(result.metadata)}
        try:
            path = self._write(Path(result.provider) / entry["target"] / "report.json", json_bytes(entry))
            entry["report"] = self.display(path)
        except Exception as error:
            entry["diagnostic_error"] = short_text(error)
        self.days.append(entry)
        return entry

    def finish(self, command: str, provider: str, status: int, note: str = "") -> str:
        saved = sum(day["status"] == "saved" for day in self.days)
        failed = len(self.days) - saved
        lines = [f"Command: {command}; provider: {provider or 'none'}; exit: {status}",
                 f"Saved dates: {saved}; failed dates: {failed}"]
        if note:
            lines.append(short_text(note, 1200))
        for day in self.days:
            if day["status"] == "saved":
                recovery = day["metadata"].get("recovery", {})
                lines.append(f"- {day['target']}: saved {day['pair_count']} pairs; recovery: {short_text(recovery)}")
                if day.get("diagnostic_error"):
                    lines.append(f"Diagnostics unavailable: {day['diagnostic_error']}")
            else:
                lines.append(f"- {day['target']}: {day['brief']}")
        if self.fatal:
            lines.append(f"Task failed: {self.fatal['brief']}")
        summary = {"command": command, "provider": provider, "exit_code": status,
                   "saved_dates": saved, "failed_dates": failed, "days": self.days,
                   "fatal": self.fatal, "note": note}
        try:
            path = self._write(Path("summary.json"), json_bytes(summary))
            lines.append(f"Run report: {self.display(path)}")
            self._write(Path("brief.md"), ("\n\n".join(lines) + "\n").encode("utf-8"))
        except Exception as error:
            lines.append(f"Diagnostics unavailable: {short_text(error)}")
        return "\n\n".join(lines)
