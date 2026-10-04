from __future__ import annotations

import argparse
import json
import os
import sys
import sysconfig
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .archive_backend import DirectoryArchiveBackend, GitHubArchiveBackend
from .concurrency import SHARD_COUNT
from .diagnostics import Diagnostics
from .http import HttpClient
from .providers import fetch_day, prepare_catalog, provider_ids
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
    if not isinstance(config, dict):
        raise ValueError("Project configuration must be a JSON object")
    archive = config.get("archive", {})
    for name in ("retain_months", "volume_bytes"):
        value = archive.get(name, 3 if name == "retain_months" else 1900 * 1024 ** 2)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"archive.{name} must be a positive integer")
    if archive.get("volume_bytes", 1900 * 1024 ** 2) > 1900 * 1024 ** 2:
        raise ValueError("archive.volume_bytes exceeds the supported Release asset size")
    for provider in provider_ids():
        if not isinstance(config.get("providers", {}).get(provider), dict):
            raise ValueError(f"Missing provider configuration: {provider}")
        rounds = config["providers"][provider].get("recovery_rounds", 2)
        if type(rounds) is not int or rounds < 0:
            raise ValueError(f"{provider}.recovery_rounds must be a non-negative integer")
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
    archive.add_argument("--config", type=Path)
    verify = commands.add_parser("verify", help="Verify existing archive manifests and checksums")
    download = commands.add_parser("download", help="Download a verified monthly or yearly Release archive")
    download.add_argument("--repository", required=True)
    download.add_argument("--period", required=True, help="YYYY-MM or YYYY")
    download.add_argument("--output-dir", type=Path, required=True)
    for subcommand in (collect, archive, verify):
        subcommand.add_argument("--data-dir", type=Path, default=Path("rates-data"))
    for subcommand in (collect, archive):
        subcommand.add_argument("--diagnostics-dir", type=Path, default=Path("diagnostics"))
        subcommand.add_argument("--release-dir", type=Path, default=Path("release-output"))
        subcommand.add_argument("--publish", action="store_true")
        subcommand.add_argument("--remote", default="origin")
        subcommand.add_argument("--branch", default="data")
    return command


def _emit_summary(text: str) -> None:
    try:
        write_summary(text)
    except OSError:
        print("Step summary unavailable; consult diagnostic report files", file=sys.stderr)


def execute(args: argparse.Namespace, diagnostics: Diagnostics | None = None) -> int:
    diagnostics = diagnostics or Diagnostics(getattr(args, "diagnostics_dir", Path("diagnostics")),
                                             getattr(args, "data_dir", None))
    if args.command == "download":
        if args.output_dir.exists() and any(args.output_dir.iterdir()):
            raise ValueError("Archive download requires an empty output directory")
        backend = GitHubArchiveBackend(args.repository, os.environ.get("GITHUB_TOKEN", ""))
        files = backend.materialize(args.period, args.output_dir)
        if files is None:
            raise ValueError(f"No published archive for {args.period}")
        print(f"Downloaded and verified {len(files)} archive files to {args.output_dir}")
        return 0
    if args.command == "verify":
        count = DataStore(args.data_dir).verify_archives()
        print(f"Verified {count} archive(s)")
        return 0
    config = load_config(args.config)
    interval = dates(args.start_date, args.end_date) if args.command == "fetch" else None
    as_of = parse_date(args.as_of) if args.command == "archive" and args.as_of else beijing_today()
    releases = (GitHubArchiveBackend.from_environment() if args.publish
                else DirectoryArchiveBackend(args.release_dir))
    publisher = None
    if args.publish:
        publisher = BranchPublisher(Path.cwd(), args.data_dir, args.remote, args.branch)
        publisher.prepare()
    store = DataStore(args.data_dir, releases, **config.get("archive", {}))
    if args.command == "archive":
        periods, snapshot = store.archive_due(as_of)
        if publisher is not None:
            publisher.publish(f"Archive due periods through {as_of.isoformat()}", snapshot=snapshot)
        details = ", ".join(periods) if periods else "none"
        summary = f"Released archive periods: {details}; annual data snapshot: {snapshot}"
        summary = diagnostics.finish(args.command, "", 0, summary)
        print(summary)
        _emit_summary(summary)
        return 0
    assert interval is not None
    first, last = interval
    successes, failures = [], []
    http_settings = {**config.get("http", {}), **config["providers"][args.provider].get("http", {})}
    with HttpClient(**http_settings) as client:
        diagnostics.records = client.records
        catalog = prepare_catalog(args.provider, config["providers"][args.provider], client)
        pair_count = catalog.pair_count if catalog is not None else 1
        count = (last - first).days + 1
        if catalog is not None:
            print(f"Official catalog: {len(catalog.transaction)} transaction / "
                  f"{len(catalog.billing)} billing currencies; {SHARD_COUNT} concurrent shards")
        print(f"Provider {args.provider}: {first} through {last}; {count * pair_count} base requests")
        target = first
        while target <= last:
            args._diagnostic_target = target.isoformat()
            client.records.clear()
            try:
                result = fetch_day(args.provider, target, catalog, config["providers"][args.provider], client)
            except Exception as exc:
                entry = diagnostics.failure(args.provider, target.isoformat(), exc, client.records)
                failures.append(target.isoformat())
                print(f"FAILED {target}: {entry['brief']}", file=sys.stderr)
            else:
                # A storage or Release failure aborts data-branch publication entirely.
                store.save_day(result)
                successes.append(target.isoformat())
                diagnostics.success(result)
                print(f"SAVED {target}")
            if target == last:
                break
            target += timedelta(days=1)
    if publisher is not None and successes:
        publisher.publish(f"Fetch {args.provider}: {first} through {last}")
    status = 1 if failures else 0
    summary = diagnostics.finish(args.command, args.provider, status)
    print(summary)
    _emit_summary(summary)
    return status


def main(argv: list[str] | None = None) -> int:
    args = None
    diagnostics = None
    try:
        args = parser().parse_args(argv)
        diagnostics = Diagnostics(getattr(args, "diagnostics_dir", Path("diagnostics")),
                                  getattr(args, "data_dir", None))
        return execute(args, diagnostics)
    except (Exception, KeyboardInterrupt) as exc:
        diagnostics = diagnostics or Diagnostics(Path("diagnostics"))
        entry = diagnostics.failure(getattr(args, "provider", ""),
                                    getattr(args, "_diagnostic_target", None), exc,
                                    diagnostics.records, fatal=True)
        print(f"ERROR: {entry['brief']}", file=sys.stderr)
        summary = diagnostics.finish(getattr(args, "command", "task"),
                                     getattr(args, "provider", ""), 1)
        _emit_summary(summary)
        return 1
