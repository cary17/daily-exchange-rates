"""Publish collected file-provider days after acquiring the shared writer queue."""

import argparse
import json
from pathlib import Path

from exchange_rates.archive_backend import GitHubArchiveBackend
from exchange_rates.cli import load_config, parse_date
from exchange_rates.models import DayResult
from exchange_rates.publishing import BranchPublisher, write_summary
from exchange_rates.storage import DataStore, _regular


JCB_PROVIDERS = ("jcb-jpy", "jcb-usd-buy", "jcb-usd-mid", "jcb-usd-sell")


def publish_collected(collected: Path, data_dir: Path, provider: str = "unionpay") -> int:
    if provider not in ("unionpay", "jcb"):
        raise ValueError(f"Unsupported collected provider: {provider}")
    names = JCB_PROVIDERS if provider == "jcb" else (provider,)
    days = {}
    for name in names:
        for path in sorted((collected / "history").glob(f"????/??/{name}/*.json")):
            target = parse_date(path.stem)
            suffix = f"{target.year:04d}/{target.month:02d}/{name}/{target.isoformat()}.json"
            if path != collected / "history" / suffix:
                raise ValueError(f"Unexpected collected date path: {path}")
            days.setdefault(target, []).append(path)
    if not days:
        raise ValueError(f"No collected {provider} days to publish")
    for target, paths in days.items():
        if {path.parent.name for path in paths} != set(names):
            raise ValueError(f"Incomplete collected {provider} files for {target}")
    publisher = BranchPublisher(Path.cwd(), data_dir)
    publisher.prepare()
    config = load_config(None)
    store = DataStore(data_dir, GitHubArchiveBackend.from_environment(),
                      **config.get("archive", {}))
    for target, paths in sorted(days.items()):
        results = []
        for path in paths:
            name = path.parent.name
            suffix = path.relative_to(collected / "history")
            metadata_path = collected / "metadata/history" / suffix
            raw_path = collected / "raw/history" / suffix
            for source in (path, metadata_path, raw_path):
                _regular(source)
            metadata = json.loads(metadata_path.read_bytes())
            if (metadata.get("provider") != name
                    or metadata.get("requested_date") != target.isoformat()):
                raise ValueError(f"Collected metadata does not match {target}")
            results.append(DayResult(name, target, path.read_bytes(), raw_path.read_bytes(), metadata))
        # Replay complete days through DataStore so archived backfills and latest stay correct.
        for result in results:
            store.save_day(result)
    publisher.publish(f"Fetch {provider}: {min(days)} through {max(days)}")
    return len(days)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("unionpay", "jcb"), default="unionpay")
    parser.add_argument("--collected-dir", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()
    count = publish_collected(args.collected_dir, args.data_dir, args.provider)
    summary = f"Published {count} collected {args.provider} day(s); no repeat HTTP requests"
    print(summary)
    write_summary(summary)


if __name__ == "__main__":
    main()
