import json
import os
import runpy
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/plan-shard-retries.py"
PLANNER = runpy.run_path(str(SCRIPT))
retry_matrix = PLANNER["retry_matrix"]


class ShardRetryTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.first = date(2026, 10, 1)
        self.last = date(2026, 10, 2)

    def bundle(self, provider, day, index):
        path = self.root / provider / day.isoformat() / f"shard-{index + 1:02d}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")

    def test_only_missing_shards_and_dates_are_retried(self):
        for provider in ("visa", "mastercard"):
            with self.subTest(provider=provider):
                self.bundle(provider, self.first, 0)
                self.bundle(provider, self.last, 0)
                self.bundle(provider, self.first, 1)
                self.assertEqual(retry_matrix(self.root, provider, self.first, self.last, 3), {
                    "include": [
                        {"shard": 1, "dates": "2026-10-02"},
                        {"shard": 2, "dates": "2026-10-01 2026-10-02"},
                    ]
                })

    def test_missing_artifacts_retry_every_shard(self):
        self.assertEqual(retry_matrix(self.root, "visa", self.first, self.first, 2), {
            "include": [
                {"shard": 0, "dates": "2026-10-01"},
                {"shard": 1, "dates": "2026-10-01"},
            ]
        })

    def test_complete_run_has_no_retries(self):
        for index in range(2):
            for day in (self.first, self.last):
                self.bundle("visa", day, index)
        self.assertEqual(retry_matrix(self.root, "visa", self.first, self.last, 2),
                         {"include": []})

    def test_other_provider_does_not_mask_missing_results(self):
        self.bundle("mastercard", self.first, 0)
        self.assertEqual(retry_matrix(self.root, "visa", self.first, self.first, 1), {
            "include": [{"shard": 0, "dates": "2026-10-01"}]
        })

    def test_github_outputs_for_retry_and_skip(self):
        output = self.root / "outputs"
        argv = [str(SCRIPT), "--provider", "visa", "--start-date", "2026-10-01",
                "--end-date", "2026-10-01", "--shard-count", "1",
                "--shard-dir", str(self.root)]
        with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}), \
                patch("sys.argv", argv):
            PLANNER["main"]()
            self.bundle("visa", self.first, 0)
            PLANNER["main"]()
        lines = output.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines[0], "has_retries=true")
        self.assertEqual(json.loads(lines[1].removeprefix("matrix=")), {
            "include": [{"shard": 0, "dates": "2026-10-01"}]
        })
        self.assertEqual(lines[2], "has_retries=false")
        self.assertEqual(json.loads(lines[3].removeprefix("matrix=")), {
            "include": [{"shard": 0, "dates": ""}]
        })


if __name__ == "__main__":
    unittest.main()
