"""Publish collected UnionPay days after acquiring the shared writer queue."""

import argparse
import json
from pathlib import Path

from exchange_rates.archive_backend import GitHubArchiveBackend
from exchange_rates.cli import load_config, parse_date
from exchange_rates.models import DayResult
from exchange_rates.publishing import BranchPublisher, write_summary
from exchange_rates.storage import DataStore


def publish_collected(collected: Path, data_dir: Path) -> int:
    paths = sorted((collected / "history").glob("????/??/unionpay/*.json"))
    if not paths:
        raise ValueError("No collected UnionPay days to publish")
    publisher = BranchPublisher(Path.cwd(), data_dir)
    publisher.prepare()
    config = load_config(None)
    store = DataStore(data_dir, GitHubArchiveBackend.from_environment(),
                      **config.get("archive", {}))
    for path in paths:
        target = parse_date(path.stem)
        suffix = f"{target.year:04d}/{target.month:02d}/unionpay/{target.isoformat()}.json"
        if path != collected / "history" / suffix:
            raise ValueError(f"Unexpected collected date path: {path}")
        metadata = json.loads((collected / "metadata/history" / suffix).read_bytes())
        if (metadata.get("provider") != "unionpay"
                or metadata.get("requested_date") != target.isoformat()):
            raise ValueError(f"Collected metadata does not match {target}")
        result = DayResult("unionpay", target, path.read_bytes(),
                           (collected / "raw/history" / suffix).read_bytes(), metadata)
        # Replay days through DataStore so archived backfills and latest stay correct.
        store.save_day(result)
    publisher.publish(f"Fetch unionpay: {paths[0].stem} through {paths[-1].stem}")
    return len(paths)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collected-dir", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()
    count = publish_collected(args.collected_dir, args.data_dir)
    summary = f"Published {count} collected UnionPay day(s); no repeat HTTP requests"
    print(summary)
    write_summary(summary)


if __name__ == "__main__":
    main()
