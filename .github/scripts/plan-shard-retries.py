"""Plan retries from missing bundles without reading large response payloads."""

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path


def retry_matrix(root: Path, provider: str, first: date, last: date,
                 count: int) -> dict:
    entries = []
    for index in range(count):
        missing = []
        target = first
        while target <= last:
            bundle = root / provider / target.isoformat() / f"shard-{index + 1:02d}.json"
            if not bundle.is_file():
                missing.append(target.isoformat())
            if target == last:
                break
            target += timedelta(days=1)
        if missing:
            entries.append({"shard": index, "dates": " ".join(missing)})
    return {"include": entries}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("visa", "mastercard"), required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--shard-dir", type=Path, required=True)
    args = parser.parse_args()
    matrix = retry_matrix(args.shard_dir, args.provider, args.start_date,
                          args.end_date, args.shard_count)
    has_retries = bool(matrix["include"])
    print(f"Retry plan: {len(matrix['include'])}/{args.shard_count} shards")
    # Keep a valid matrix even when the retry job is skipped.
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write(f"has_retries={str(has_retries).lower()}\n")
        out.write("matrix=" + json.dumps(matrix if has_retries else {
            "include": [{"shard": 0, "dates": ""}]
        }) + "\n")


if __name__ == "__main__":
    main()
