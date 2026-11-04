import json
import tarfile
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from exchange_rates.archive_backend import DirectoryArchiveBackend
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
        self.root = Path(self.temporary.name) / "data"
        self.backend = DirectoryArchiveBackend(Path(self.temporary.name) / "releases")
        self.store = DataStore(self.root, self.backend)

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

    def test_manual_archive_catches_missed_months_and_years(self):
        self.store.save_day(result(date(2025, 6, 1)))
        self.store.save_day(result(date(2026, 7, 1)))
        periods, snapshot = self.store.archive_due(date(2026, 8, 3))
        self.assertEqual(periods, ["2025-06", "2026-07", "2025"])
        self.assertTrue(snapshot)
        self.assertEqual(self.store.verify_archives(), 1)
        self.assertTrue(self.backend.has("2025-06"))
        self.assertTrue(self.backend.has("2025"))
        self.assertFalse(self.store.month_artifact(2025, 6).exists)

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

    def test_legacy_state_migrates_with_all_fields_and_is_not_recreated(self):
        self.root.mkdir(parents=True, exist_ok=True)
        legacy = {"published_months": ["2025-06"], "published_years": ["2025"],
                  "retention_as_of": "2026-08-03", "last_snapshot_year": 2025,
                  "last_error": {"prepared": ["2025-06"], "published": [], "error": "fixture"}}
        (self.root / "maintenance.json").write_bytes(json_bytes(legacy))
        self.assertEqual(DataStore(self.root, self.backend)._state(), legacy)
        migrated = self.root / "metadata/maintenance.json"
        self.assertEqual(json.loads(migrated.read_bytes()), legacy)
        self.assertFalse((self.root / "maintenance.json").exists())
        DataStore(self.root, self.backend).save_day(result(date(2026, 9, 3)))
        self.assertFalse((self.root / "maintenance.json").exists())
        self.assertIn("retention_as_of", json.loads(migrated.read_bytes()))

    def test_fresh_store_does_not_create_state(self):
        DataStore(self.root, self.backend)
        self.assertFalse((self.root / "metadata/maintenance.json").exists())

    def test_existing_new_state_is_used(self):
        metadata = self.root / "metadata/maintenance.json"
        metadata.parent.mkdir(parents=True)
        metadata.write_bytes(json_bytes({"published_months": ["2025-06"], "published_years": ["2025"]}))
        self.assertEqual(DataStore(self.root, self.backend)._state()["published_months"], ["2025-06"])
        self.assertFalse((self.root / "maintenance.json").exists())

    def test_equivalent_state_cleans_legacy_and_conflict_keeps_both(self):
        metadata = self.root / "metadata/maintenance.json"
        metadata.parent.mkdir(parents=True)
        state = {"published_months": ["2025-06"], "published_years": ["2025"]}
        metadata.write_bytes(json_bytes(state))
        legacy = self.root / "maintenance.json"
        legacy.write_bytes(json_bytes(state))
        self.assertEqual(DataStore(self.root, self.backend)._state(), state)
        self.assertTrue(metadata.exists())
        self.assertFalse(legacy.exists())
        legacy.write_bytes(json_bytes({**state, "last_snapshot_year": 2025}))
        with self.assertRaises(StorageError):
            DataStore(self.root, self.backend)
        self.assertTrue(metadata.exists() and legacy.exists())
        legacy.write_bytes(json_bytes(state))
        self.assertEqual(DataStore(self.root, self.backend)._state(), state)
        self.assertFalse(legacy.exists())

    def test_state_migration_failures_keep_legacy_state(self):
        self.root.mkdir(parents=True, exist_ok=True)
        legacy = self.root / "maintenance.json"
        state = {"published_months": ["2025-06"], "published_years": []}
        legacy.write_bytes(json_bytes(state))
        with mock.patch("exchange_rates.storage.atomic_write", side_effect=OSError("fixture write failed")):
            with self.assertRaises(OSError):
                DataStore(self.root, self.backend)
        self.assertEqual(json.loads(legacy.read_bytes()), state)
        self.assertFalse((self.root / "metadata/maintenance.json").exists())
        with mock.patch.object(DataStore, "_remove", side_effect=StorageError("fixture delete failed")):
            with self.assertRaises(StorageError):
                DataStore(self.root, self.backend)
        self.assertEqual(json.loads(legacy.read_bytes()), state)
        self.assertEqual(json.loads((self.root / "metadata/maintenance.json").read_bytes()), state)
        self.assertEqual(DataStore(self.root, self.backend)._state(), state)
        self.assertFalse(legacy.exists())

    def test_corrupt_legacy_state_is_not_overwritten(self):
        self.root.mkdir(parents=True, exist_ok=True)
        legacy = self.root / "maintenance.json"
        legacy.write_bytes(b"{not json")
        with self.assertRaises(StorageError):
            DataStore(self.root, self.backend)
        self.assertEqual(legacy.read_bytes(), b"{not json")
        self.assertFalse((self.root / "metadata/maintenance.json").exists())


if __name__ == "__main__":
    unittest.main()
