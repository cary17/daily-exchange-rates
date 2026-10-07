import json
import runpy
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from exchange_rates.archive_backend import DirectoryArchiveBackend
from exchange_rates.models import DayResult, json_bytes
from exchange_rates.storage import DataStore


ROOT = Path(__file__).resolve().parents[1]
PUBLISH = runpy.run_path(str(ROOT / ".github/scripts/publish-unionpay.py"))["publish_collected"]


def day_result(provider, target, rate):
    return DayResult(provider, target, json_bytes({"exchangeRateJson": [
        {"transCur": "USD", "baseCur": "CNY", "rateData": rate},
    ]}), json_bytes({"response": rate}), {"pair_count": 1})


class ProviderConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.collected = self.root / "collected"
        self.data_dir = self.root / "data"
        self.releases = DirectoryArchiveBackend(self.root / "releases")
        self.publisher = Mock()
        self.publisher.prepare.side_effect = self.prepare_latest_branch
        factory = Mock(return_value=self.publisher)
        backend = SimpleNamespace(from_environment=lambda: self.releases)
        self.enterContext(patch.dict(PUBLISH.__globals__, {
            "BranchPublisher": factory, "GitHubArchiveBackend": backend,
        }))

    def prepare_latest_branch(self):
        store = DataStore(self.data_dir, self.releases)
        store.save_day(day_result("visa", date(2026, 10, 3), "7.2"))
        store.save_day(day_result("unionpay", date(2026, 10, 5), "7.3"))

    def collect(self, target, rate):
        store = DataStore(self.collected, DirectoryArchiveBackend(self.root / "staged-releases"))
        store.save_day(day_result("unionpay", target, rate))

    def test_publish_preserves_other_provider_and_newer_latest(self):
        self.collect(date(2026, 10, 1), "7.0")
        self.collect(date(2026, 10, 2), "7.1")
        self.assertEqual(PUBLISH(self.collected, self.data_dir), 2)
        self.publisher.prepare.assert_called_once()
        self.publisher.publish.assert_called_once_with("Fetch unionpay: 2026-10-01 through 2026-10-02")
        self.assertEqual(json.loads((self.data_dir / "latest/visa.json").read_bytes())["exchangeRateJson"][0]["rateData"], "7.2")
        latest = json.loads((self.data_dir / "metadata/latest/unionpay.json").read_bytes())
        self.assertEqual(latest["requested_date"], "2026-10-05")
        suffix = "2026/10/unionpay/2026-10-02.json"
        for prefix in ("history", "raw/history", "metadata/history"):
            self.assertEqual((self.data_dir / prefix / suffix).read_bytes(),
                             (self.collected / prefix / suffix).read_bytes())

    def test_empty_collection_does_not_prepare_or_publish(self):
        with self.assertRaisesRegex(ValueError, "No collected"):
            PUBLISH(self.collected, self.data_dir)
        self.publisher.prepare.assert_not_called()
        self.publisher.publish.assert_not_called()

    def test_mismatched_metadata_does_not_publish(self):
        self.collect(date(2026, 10, 1), "7.0")
        metadata = self.collected / "metadata/history/2026/10/unionpay/2026-10-01.json"
        metadata.write_text(json.dumps({"provider": "visa", "requested_date": "2026-10-01"}))
        with self.assertRaisesRegex(ValueError, "metadata does not match"):
            PUBLISH(self.collected, self.data_dir)
        self.publisher.publish.assert_not_called()

    def test_workflows_queue_by_provider_and_lock_only_writers(self):
        workflows = ROOT / ".github/workflows"
        for provider in ("visa", "mastercard", "unionpay"):
            with self.subTest(provider=provider):
                text = (workflows / f"fetch-{provider}.yml").read_text()
                self.assertIn(f"concurrency:\n  group: fetch-{provider}\n  queue: max\n  cancel-in-progress: false", text)
                self.assertIn("    concurrency:\n      group: data-branch-writes\n      queue: max\n      cancel-in-progress: false", text)
        unionpay = (workflows / "fetch-unionpay.yml").read_text()
        collect_job, publish_job = unionpay.split("  publish:\n", 1)
        self.assertNotIn("--publish", collect_job)
        self.assertNotIn("data-branch-writes", collect_job)
        self.assertNotIn("exchange_rates fetch", publish_job)
        self.assertIn("needs: fetch", publish_job)
        self.assertIn("!cancelled()", publish_job)
        self.assertIn("publish-unionpay.py", publish_job)
        self.assertIn("group: data-branch-writes", (workflows / "archive.yml").read_text())


if __name__ == "__main__":
    unittest.main()
