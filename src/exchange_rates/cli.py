from __future__ import annotations

import argparse
import json
import os
import re
import sys
import sysconfig
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .archive_backend import DirectoryArchiveBackend, GitHubArchiveBackend
from .concurrency import SHARD_COUNT
from .diagnostics import Diagnostics
from .http import HttpClient
from .models import json_bytes
from .providers import (
    fetch_day, fetch_shard, merge_shards, prepare_catalog, provider_ids,
)
from .publishing import BranchPublisher, write_summary
from .rate_control import finite_seconds
from .storage import DataStore, atomic_write

SHARDABLE = ("visa", "mastercard")


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


def non_negative_seconds(value: str) -> float:
    try:
        return finite_seconds("request interval", float(value))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def shard_bundle_path(root: Path, provider: str, target: date, index: int) -> Path:
    return Path(root) / provider / target.isoformat() / f"shard-{index + 1:02d}.json"


def load_shard_bundles(root: Path, provider: str, target: date) -> list[dict]:
    """Read a contiguous shard set produced by parallel shard jobs."""
    directory = Path(root) / provider / target.isoformat()
    if not directory.is_dir():
        raise ValueError(f"No shard directory for {provider} {target.isoformat()}")
    indexed = []
    for path in sorted(directory.glob("shard-*.json")):
        match = re.fullmatch(r"shard-(\d{2})\.json", path.name)
        if match is None:
            raise ValueError(f"Unexpected shard file: {path.name}")
        indexed.append((int(match.group(1)), path))
    numbers = [number for number, _ in indexed]
    if numbers != list(range(1, len(numbers) + 1)):
        raise ValueError(f"Incomplete shard set for {target.isoformat()}: {numbers}")
    bundles = []
    for number, path in indexed:
        try:
            bundle = json.loads(path.read_bytes())
        except (ValueError, UnicodeError, OSError) as exc:
            raise ValueError(f"Unreadable shard {path.name}: {exc}") from exc
        if not isinstance(bundle, dict):
            raise ValueError(f"Shard {path.name} is not a JSON object")
        bundles.append(bundle)
    return bundles


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description="Manual official exchange-rate collection")
    commands = command.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("fetch", help="Collect an inclusive date range in one process")
    collect.add_argument("--provider", choices=provider_ids(), required=True)
    collect.add_argument("--start-date", default="")
    collect.add_argument("--end-date", default="")
    collect.add_argument("--config", type=Path)
    collect.add_argument("--interval", type=non_negative_seconds, default=None,
                         help="Per-session request interval in seconds; overrides configured interval")
    shard = commands.add_parser(
        "fetch-shard", help="Collect one currency shard of a date range")
    shard.add_argument("--provider", choices=SHARDABLE, required=True)
    shard.add_argument("--start-date", default="")
    shard.add_argument("--end-date", default="")
    shard.add_argument("--config", type=Path)
    shard.add_argument("--interval", type=non_negative_seconds, default=None,
                       help="Per-session request interval in seconds; overrides configured interval")
    shard.add_argument("--shard-index", type=int, required=True)
    shard.add_argument("--shard-count", type=int, default=SHARD_COUNT,
                       help=f"Total disjoint shards (default {SHARD_COUNT})")
    shard.add_argument("--shard-dir", type=Path, required=True,
                       help="Directory shared by shard jobs and the merge job")
    merge = commands.add_parser(
        "merge-shards", help="Verify shard bundles and publish complete days")
    merge.add_argument("--provider", choices=SHARDABLE, required=True)
    merge.add_argument("--start-date", default="")
    merge.add_argument("--end-date", default="")
    merge.add_argument("--config", type=Path)
    merge.add_argument("--shard-dir", type=Path, required=True)
    archive = commands.add_parser("archive", help="Archive all due months and years")
    archive.add_argument("--as-of", default="", help=argparse.SUPPRESS)
    archive.add_argument("--config", type=Path)
    verify = commands.add_parser("verify", help="Verify existing archive manifests and checksums")
    download = commands.add_parser("download", help="Download a verified monthly or yearly Release archive")
    download.add_argument("--repository", required=True)
    download.add_argument("--period", required=True, help="YYYY-MM or YYYY")
    download.add_argument("--output-dir", type=Path, required=True)
    for subcommand in (collect, merge, archive, verify):
        subcommand.add_argument("--data-dir", type=Path, default=Path("rates-data"))
    for subcommand in (collect, shard, merge, archive):
        subcommand.add_argument("--diagnostics-dir", type=Path, default=Path("diagnostics"))
    for subcommand in (collect, merge, archive):
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


def http_settings(config: dict, args: argparse.Namespace) -> dict:
    settings = {**config.get("http", {}),
                **config["providers"][args.provider].get("http", {})}
    if getattr(args, "interval", None) is not None:
        settings["interval"] = args.interval
    return settings


def _run_shard(config: dict, args: argparse.Namespace, diagnostics: Diagnostics,
               first: date, last: date, settings: dict) -> int:
    provider, index, count = args.provider, args.shard_index, args.shard_count
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("shard_count must be a positive integer")
    if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < count:
        raise ValueError("shard_index must be within shard_count")
    failures, written = [], 0
    with HttpClient(**settings) as client:
        print(f"HTTP pacing: per-session interval={client.interval:g}s; "
              f"global interval={client.global_interval:g}s")
        diagnostics.records = client.records
        print(f"Shard {index + 1}/{count} of {provider}: {first} through {last}")
        target = first
        while target <= last:
            args._diagnostic_target = target.isoformat()
            client.records.clear()
            try:
                bundle = fetch_shard(provider, target, config["providers"][provider],
                                     client, index, count)
            except Exception as exc:
                entry = diagnostics.failure(provider, target.isoformat(), exc,
                                            client.records)
                failures.append(target.isoformat())
                print(f"FAILED {target}: {entry['brief']}", file=sys.stderr)
            else:
                path = shard_bundle_path(args.shard_dir, provider, target, index)
                atomic_write(path, json_bytes(bundle, compact=True))
                written += 1
                print(f"SHARD {target}: {len(bundle['rows'])} pairs -> {path}")
            if target == last:
                break
            target += timedelta(days=1)
    status = 1 if failures else 0
    summary = diagnostics.finish("fetch-shard", provider, status,
                                 f"shard {index + 1}/{count}; bundles written: {written}")
    print(summary)
    _emit_summary(summary)
    return status


def _run_merge(config: dict, args: argparse.Namespace, diagnostics: Diagnostics,
               publisher: BranchPublisher | None, store: DataStore,
               first: date, last: date) -> int:
    provider = args.provider
    successes, failures = [], []
    print(f"Merging shards for {provider}: {first} through {last}")
    target = first
    while target <= last:
        args._diagnostic_target = target.isoformat()
        try:
            bundles = load_shard_bundles(args.shard_dir, provider, target)
            result = merge_shards(provider, target, bundles)
        except Exception as exc:
            entry = diagnostics.failure(provider, target.isoformat(), exc, [])
            failures.append(target.isoformat())
            print(f"FAILED {target}: {entry['brief']}", file=sys.stderr)
        else:
            # A storage or Release failure aborts data-branch publication entirely.
            store.save_day(result)
            successes.append(target.isoformat())
            diagnostics.success(result)
            print(f"SAVED {target}: {result.metadata['pair_count']} pairs")
        if target == last:
            break
        target += timedelta(days=1)
    if publisher is not None and successes:
        publisher.publish(f"Fetch {provider}: {first} through {last}")
    status = 1 if failures else 0
    summary = diagnostics.finish("merge-shards", provider, status)
    print(summary)
    _emit_summary(summary)
    return status


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
    sharded = args.command in ("fetch", "fetch-shard", "merge-shards")
    first, last = dates(args.start_date, args.end_date) if sharded else (None, None)
    as_of = parse_date(args.as_of) if args.command == "archive" and args.as_of else beijing_today()
    if args.command == "fetch-shard":
        # Shard jobs never write published data; they only stage bundle files.
        return _run_shard(config, args, diagnostics, first, last,
                          http_settings(config, args))
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
    if args.command == "merge-shards":
        return _run_merge(config, args, diagnostics, publisher, store, first, last)
    assert first is not None and last is not None
    successes, failures = [], []
    settings = http_settings(config, args)
    with HttpClient(**settings) as client:
        print(f"HTTP pacing: per-session interval={client.interval:g}s; "
              f"global interval={client.global_interval:g}s")
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
