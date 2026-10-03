import json
import tarfile
import tempfile
import unittest
from datetime import date
from pathlib import Path

from exchange_rates.models import DayResult, json_bytes
from exchange_rates.storage import DataStore, StorageError, sha256


def result(day, provider="visa", rate=7):
    return DayResult(provider, day, json_bytes({"exchangeRateJson": [
        {"transCur": "USD", "baseCur": "CNY", "rateData": rate},
    ]}), json_bytes({"body_text": str(rate)}), {"fetched_at_utc": "2026-10-03T00:00:00Z"})


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = DataStore(self.root)

    def entries(self, artifact):
        with tarfile.open(artifact.archive, "r:gz") as archive:
            return {member.name: archive.extractfile(member).read() for member in archive}

    def test_same_day_overwrite_and_older_day_does_not_replace_latest(self):
        self.store.save_day(result(date(2026, 9, 30), rate=7))
        self.store.save_day(result(date(2026, 9, 30), rate=8))
        self.store.save_day(result(date(2026, 9, 20), rate=6))
        current = json.loads((self.root / "latest/visa.json").read_bytes())
        self.assertEqual(current["exchangeRateJson"][0]["rateData"], 8)
        self.assertEqual(json.loads((self.root / "metadata/latest/visa.json").read_bytes())["requested_date"], "2026-09-30")
        self.assertEqual(json.loads((self.root / "raw/latest/visa.json").read_bytes())["body_text"], "8")

    def test_month_cutoff_uses_third_day(self):
        self.store.save_day(result(date(2026, 9, 30)))
        self.assertEqual(self.store.archive_due(date(2026, 10, 2)), ([], False))
        self.assertEqual(self.store.archive_due(date(2026, 10, 3)), (["2026-09"], False))
        artifact = self.store.month_artifact(2026, 9)
        self.assertTrue(artifact.archive.exists())
        self.assertEqual(len(self.entries(artifact)), 3)
        self.assertFalse((self.root / "history/2026/09/visa").exists())
        self.assertTrue((self.root / "latest/visa.json").exists())
        before = sha256(artifact.archive)
        self.assertEqual(self.store.archive_due(date(2026, 10, 3)), ([], False))
        self.assertEqual(sha256(artifact.archive), before)
        self.assertEqual(self.store.verify_archives(), 1)

    def test_month_supplement_preserves_other_providers(self):
        day = date(2026, 9, 30)
        self.store.save_day(result(day, "visa", 7))
        self.store.save_day(result(day, "mastercard", 6))
        self.store.archive_due(date(2026, 10, 3))
        artifact = self.store.month_artifact(2026, 9)
        before = self.entries(artifact)
        self.store.save_day(result(day, "visa", 8))
        after = self.entries(artifact)
        self.assertEqual(len(after), 6)
        for name in before:
            if "/mastercard/" in name:
                self.assertEqual(before[name], after[name])
        self.assertEqual(json.loads(after["history/2026/09/visa/2026-09-30.json"])["exchangeRateJson"][0]["rateData"], 8)
        self.assertEqual(self.store.verify_archives(), 1)

    def test_year_flatten_cleanup_snapshot_and_supplement(self):
        self.store.save_day(result(date(2025, 11, 20)))
        self.store.save_day(result(date(2025, 12, 30), "mastercard"))
        self.store.archive_due(date(2025, 12, 3))
        self.store.save_day(result(date(2026, 1, 1)))
        periods, snapshot = self.store.archive_due(date(2026, 1, 3))
        self.assertEqual(periods, ["2025-12", "2025"])
        self.assertTrue(snapshot)
        year = self.store.year_artifact(2025)
        self.assertEqual(len(self.entries(year)), 6)
        self.assertTrue(all(name.endswith(".json") for name in self.entries(year)))
        self.assertFalse((self.root / "history/2025").exists())
        self.assertTrue((self.root / "history/2026/01/visa/2026-01-01.json").exists())
        self.assertEqual(self.store.archive_due(date(2026, 1, 3)), ([], False))
        self.store.save_day(result(date(2025, 11, 21)))
        self.assertEqual(len(self.entries(year)), 9)
        self.assertFalse((self.root / "history/2025").exists())
        self.assertEqual(self.store.verify_archives(), 1)

    def test_manual_archive_catches_missed_months_and_years(self):
        self.store.save_day(result(date(2025, 6, 1)))
        self.store.save_day(result(date(2026, 7, 1)))
        periods, snapshot = self.store.archive_due(date(2026, 8, 3))
        self.assertEqual(periods, ["2025-06", "2026-07", "2025"])
        self.assertTrue(snapshot)
        self.assertEqual(self.store.verify_archives(), 2)

    def test_corrupted_archive_blocks_supplement_and_keeps_other_files(self):
        day = date(2026, 9, 30)
        self.store.save_day(result(day))
        self.store.archive_due(date(2026, 10, 3))
        archive = self.store.month_artifact(2026, 9).archive
        archive.write_bytes(archive.read_bytes() + b"corruption")
        latest = (self.root / "latest/visa.json").read_bytes()
        with self.assertRaises(StorageError):
            self.store.save_day(result(day, rate=8))
        self.assertEqual((self.root / "latest/visa.json").read_bytes(), latest)

    def test_incomplete_group_is_not_archived_or_deleted(self):
        day = date(2026, 9, 30)
        self.store.save_day(result(day))
        (self.root / "raw/history/2026/09/visa/2026-09-30.json").unlink()
        with self.assertRaises(StorageError):
            self.store.archive_due(date(2026, 10, 3))
        self.assertTrue((self.root / "history/2026/09/visa/2026-09-30.json").exists())
        self.assertFalse(self.store.month_artifact(2026, 9).archive.exists())

    def test_archive_member_traversal_is_rejected(self):
        from exchange_rates.storage import member_info
        for path in ("../secret", "/history/2026/09/visa/2026-09-30.json",
                     "history/2026/08/visa/2026-09-30.json"):
            with self.assertRaises(StorageError):
                member_info(path, "2026-09")


if __name__ == "__main__":
    unittest.main()
