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
from exchange_rates.storage import DataStore, _archive_members


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

    def test_jcb_four_feeds_publish_archive_and_keep_newer_latest(self):
        names = PUBLISH.__globals__["JCB_PROVIDERS"]
        target = date(2025, 12, 29)
        collected_store = DataStore(self.collected, self.releases)
        store = DataStore(self.data_dir, self.releases)
        for name in names:
            collected_store.save_day(day_result(name, target, "0.00625"))
            store.save_day(day_result(name, date(2026, 10, 5), "0.0063"))
        self.assertEqual(PUBLISH(self.collected, self.data_dir, "jcb"), 1)
        self.publisher.publish.assert_called_once_with("Fetch jcb: 2025-12-29 through 2025-12-29")
        expected = {}
        for name in names:
            metadata = json.loads((self.data_dir / "metadata/latest" / f"{name}.json").read_bytes())
            self.assertEqual(metadata["requested_date"], "2026-10-05")
            suffix = f"2025/12/{name}/2025-12-29.json"
            for role in ("history", "raw/history", "metadata/history"):
                path = f"{role}/{suffix}"
                expected[path] = (self.collected / path).read_bytes()
                self.assertEqual((self.data_dir / path).read_bytes(), expected[path])
        self.assertTrue((self.data_dir / "latest/visa.json").exists())
        self.assertEqual(store.archive_due(date(2026, 1, 3)), (["2025-12", "2025"], True))
        for period in ("2025-12", "2025"):
            destination = self.root / period
            self.releases.materialize(period, destination)
            members = {name: stream.read() for name, _, stream
                       in _archive_members(store._artifact(destination, period))}
            self.assertEqual(members, expected)

    def test_incomplete_jcb_day_does_not_prepare_or_publish(self):
        store = DataStore(self.collected, self.releases)
        for name in PUBLISH.__globals__["JCB_PROVIDERS"][:-1]:
            store.save_day(day_result(name, date(2026, 10, 1), "0.00625"))
        with self.assertRaisesRegex(ValueError, "Incomplete collected jcb"):
            PUBLISH(self.collected, self.data_dir, "jcb")
        self.publisher.prepare.assert_not_called()
        self.publisher.publish.assert_not_called()

    def test_bad_jcb_metadata_is_rejected_before_saving_any_feed(self):
        target = date(2026, 10, 1)
        store = DataStore(self.collected, self.releases)
        for name in PUBLISH.__globals__["JCB_PROVIDERS"]:
            store.save_day(day_result(name, target, "0.00625"))
        metadata = self.collected / "metadata/history/2026/10/jcb-usd-sell/2026-10-01.json"
        metadata.write_text(json.dumps({"provider": "jcb-usd-buy", "requested_date": str(target)}))
        self.publisher.prepare.side_effect = None
        with patch.object(DataStore, "save_day") as save:
            with self.assertRaisesRegex(ValueError, "metadata does not match"):
                PUBLISH(self.collected, self.data_dir, "jcb")
            save.assert_not_called()
        self.publisher.publish.assert_not_called()

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


if __name__ == "__main__":
    unittest.main()
