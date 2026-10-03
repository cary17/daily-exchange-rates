from __future__ import annotations

import argparse
import json
import re
import sys
import sysconfig
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .http import HttpClient
from .providers import fetch_day, provider_ids
from .publishing import BranchPublisher, write_summary
from .storage import DataStore


def beijing_today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def parse_date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Dates must use YYYY-MM-DD")
    return parsed


def dates(start: str, end: str) -> tuple[date, date]:
    first = parse_date(start) if start else beijing_today()
    last = parse_date(end) if end else first
    if last < first:
        raise ValueError("End date must not precede start date")
    return first, last


def load_config(path: Path | None) -> dict:
    if path is None:
        source = Path(__file__).resolve().parents[2] / "config" / "providers.json"
        installed = Path(sysconfig.get_path("data")) / "share" / "daily-exchange-rates" / "providers.json"
        path = source if source.is_file() else installed
    config = json.loads(path.read_bytes())
    currencies = config.get("currencies")
    if not isinstance(currencies, list) or len(currencies) < 2:
        raise ValueError("Configure at least two currencies")
    if any(not isinstance(item, str) or not re.fullmatch(r"[A-Z]{3}", item) for item in currencies):
        raise ValueError("Currencies must be three-letter uppercase codes")
    if len(currencies) != len(set(currencies)):
        raise ValueError("Currencies must be unique")
    for provider in provider_ids():
        if not isinstance(config.get("providers", {}).get(provider), dict):
            raise ValueError(f"Missing provider configuration: {provider}")
    return config


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description="Manual official exchange-rate collection")
    commands = command.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("fetch", help="Collect an inclusive date range")
    collect.add_argument("--provider", choices=provider_ids(), required=True)
    collect.add_argument("--start-date", default="")
    collect.add_argument("--end-date", default="")
    collect.add_argument("--config", type=Path)
    archive = commands.add_parser("archive", help="Archive all due months and years")
    archive.add_argument("--as-of", default="", help=argparse.SUPPRESS)
    verify = commands.add_parser("verify", help="Verify existing archive manifests and checksums")
    for subcommand in (collect, archive, verify):
        subcommand.add_argument("--data-dir", type=Path, default=Path("rates-data"))
    for subcommand in (collect, archive):
        subcommand.add_argument("--publish", action="store_true")
        subcommand.add_argument("--remote", default="origin")
        subcommand.add_argument("--branch", default="data")
    return command


def execute(args: argparse.Namespace) -> int:
    publisher = None
    interval = None
    config = None
    if args.command == "fetch":
        interval = dates(args.start_date, args.end_date)
        config = load_config(args.config)
    as_of = parse_date(args.as_of) if args.command == "archive" and args.as_of else beijing_today()
    if getattr(args, "publish", False):
        publisher = BranchPublisher(Path.cwd(), args.data_dir, args.remote, args.branch)
        publisher.prepare()
    store = DataStore(args.data_dir)
    if args.command == "verify":
        count = store.verify_archives()
        print(f"Verified {count} archive(s)")
        return 0
    if args.command == "archive":
        periods, snapshot = store.archive_due(as_of)
        if publisher is not None:
            publisher.publish(f"Archive due periods through {as_of.isoformat()}", snapshot=snapshot)
        details = ", ".join(periods) if periods else "none"
        summary = f"Archived periods: {details}; annual snapshot: {snapshot}"
        print(summary)
        write_summary(summary)
        return 0
    assert interval is not None and config is not None
    first, last = interval
    pair_count = len(config["currencies"]) * (len(config["currencies"]) - 1)
    count = (last - first).days + 1
    requests_per_day = 1 if args.provider == "unionpay" else pair_count
    print(f"Provider {args.provider}: {first} through {last}; {count * requests_per_day} base requests")
    successes, failures = [], []
    http_config = config.get("http", {})
    with HttpClient(**http_config) as client:
        target = first
        while target <= last:
            client.records.clear()
            try:
                result = fetch_day(args.provider, target, config["currencies"],
                                   config["providers"][args.provider], client)
            except Exception as exc:
                # Fetch failures are per-day. A storage failure aborts publication entirely.
                failures.append((target.isoformat(), str(exc)))
                print(f"FAILED {target}: {exc}", file=sys.stderr)
            else:
                store.save_day(result)
                successes.append(target.isoformat())
                print(f"SAVED {target}")
            if target == last:
                break
            target += timedelta(days=1)
    if publisher is not None and successes:
        publisher.publish(f"Fetch {args.provider}: {first} through {last}")
    summary = f"Provider: {args.provider}\n\nSaved: {', '.join(successes) or 'none'}"
    if failures:
        summary += "\n\nFailed dates:\n" + "\n".join(f"- {day}: {message}" for day, message in failures)
    print(summary)
    write_summary(summary)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    try:
        return execute(parser().parse_args(argv))
    except (Exception, KeyboardInterrupt) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        write_summary(f"Task failed: {exc}")
        return 1
