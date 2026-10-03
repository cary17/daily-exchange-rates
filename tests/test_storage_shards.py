import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from exchange_rates.archive_backend import DirectoryArchiveBackend
from exchange_rates.models import DayResult, json_bytes
from exchange_rates.storage import DataStore, StorageError, _archive_members, member_info, sha256


def daily(day, provider="visa", value=7, sharded=True):
    parts = {}
    if sharded:
        for number in range(1, 10):
            parts[f"shard-{number:02d}.json"] = json_bytes({
                "provider": provider, "requested_date": day.isoformat(), "shard_index": number,
                "transaction_currencies": ["USD"] if number == 1 else [],
                "requests": [{"body_text": str(value)}] if number == 1 else [],
            })
        raw = json_bytes({
            "format": "exchange-rates-raw-shards-v1", "provider": provider,
            "requested_date": day.isoformat(), "catalog_requests": [],
            "parts": [{"name": name, "size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                      for name, body in sorted(parts.items())],
        })
    else:
        raw = json_bytes({"body_text": str(value)})
    return DayResult(provider, day, json_bytes({"rate": value}), raw, {"fetched_at_utc": "fixture"}, parts)


class ControlledBackend(DirectoryArchiveBackend):
    def __init__(self, root):
        super().__init__(root)
        self.fail_period = None
        self.published = []
        self.downloaded = []

    def publish(self, period, files):
        if period == self.fail_period:
            raise RuntimeError("fixture publish failed")
        result = super().publish(period, files)
        self.published.append(period)
        return result

    def materialize(self, period, destination):
        self.downloaded.append(period)
        return super().materialize(period, destination)


class ShardedStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "data"
        self.backend = ControlledBackend(self.base / "releases")
        self.store = DataStore(self.root, self.backend)

    def members(self, artifact):
        result = {}
        for name, _, stream in _archive_members(artifact):
            chunks = []
            while body := stream.read(1024):
                chunks.append(body)
            result[name] = b"".join(chunks)
        return result

    def remote(self, period):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.assertIsNotNone(self.backend.materialize(period, directory))
            return self.members(self.store._artifact(directory, period))

    def latest(self):
        return {path.relative_to(self.root).as_posix(): path.read_bytes()
                for role in ("latest", "raw/latest", "metadata/latest")
                for path in (self.root / role).rglob("*") if path.is_file()}

    def test_nine_parts_daily_latest_archive_and_legacy_unionpay(self):
        day = date(2026, 9, 30)
        result = daily(day)
        self.store.save_day(result)
        index = json.loads((self.root / "raw/history/2026/09/visa/2026-09-30.json").read_bytes())
        latest = json.loads((self.root / "raw/latest/visa.json").read_bytes())
        self.assertEqual(index["parts_directory"], "2026-09-30.parts")
        self.assertEqual(latest["parts_directory"], "visa.parts")
        self.assertEqual(set(result.raw_parts), {path.name for path in (self.root / "raw/latest/visa.parts").iterdir()})
        (self.root / "raw/latest/visa.parts/shard-10.json").write_bytes(b"orphan")
        self.store.save_day(result)
        self.assertFalse((self.root / "raw/latest/visa.parts/shard-10.json").exists())
        legacy = daily(day, "unionpay", sharded=False)
        self.store.save_day(legacy)
        self.assertEqual((self.root / "raw/latest/unionpay.json").read_bytes(), legacy.raw)
        self.store.archive_due(date(2026, 10, 3))
        members = self.remote("2026-09")
        self.assertEqual(len(members), 15)
        self.assertEqual(sum(".parts/" in name for name in members), 9)
        self.assertFalse((self.root / "raw/history/2026/09").exists())
        self.assertEqual(self.store.verify_archives(), 1)

    def test_bad_raw_result_is_rejected_before_writes(self):
        day = date(2026, 9, 30)
        self.store.save_day(daily(day))
        before = self.latest()
        bad = daily(day, value=8)
        index = json.loads(bad.raw)
        index["parts"][0]["sha256"] = "0" * 64
        bad.raw = json_bytes(index)
        with self.assertRaises(StorageError):
            self.store.save_day(bad)
        self.assertEqual(self.latest(), before)
        bad = daily(day, value=8)
        del bad.raw_parts["shard-09.json"]
        with self.assertRaises(StorageError):
            self.store.save_day(bad)
        self.assertEqual(self.latest(), before)

    def test_missing_bad_and_orphan_loose_shards_block_cleanup(self):
        for defect in ("missing", "hash", "orphan", "symlink"):
            with self.subTest(defect=defect):
                root = self.base / defect
                store = DataStore(root, DirectoryArchiveBackend(self.base / (defect + "-releases")))
                store.save_day(daily(date(2026, 9, 30)))
                parts = root / "raw/history/2026/09/visa/2026-09-30.parts"
                target = parts / "shard-09.json"
                if defect == "missing":
                    target.unlink()
                elif defect == "hash":
                    target.write_bytes(b"bad")
                elif defect == "orphan":
                    (parts / "shard-10.json").write_bytes(b"{}")
                else:
                    target.unlink()
                    target.symlink_to(parts / "shard-01.json")
                latest = (root / "latest/visa.json").read_bytes()
                with self.assertRaises(StorageError):
                    store.archive_due(date(2026, 10, 3))
                self.assertTrue((root / "history/2026/09/visa/2026-09-30.json").exists())
                self.assertEqual((root / "latest/visa.json").read_bytes(), latest)
                self.assertFalse(store.month_artifact(2026, 9).exists)

    def test_volume_stream_hash_corruption_and_replacement(self):
        self.store = DataStore(self.root, self.backend, volume_bytes=160)
        day = date(2026, 9, 30)
        self.store.save_day(daily(day))
        self.store.archive_due(date(2026, 10, 3))
        artifact = self.store.month_artifact(2026, 9)
        manifest = json.loads(artifact.manifest.read_bytes())
        self.assertFalse(artifact.archive.exists())
        self.assertGreater(len(artifact.volumes), 1)
        self.assertTrue(all(path.stat().st_size <= 160 for path in artifact.volumes))
        self.assertEqual(manifest["stream_sha256"], hashlib.sha256(b"".join(path.read_bytes() for path in artifact.volumes)).hexdigest())
        self.assertEqual({row["filename"] for row in manifest["volumes"]}, {path.name for path in artifact.volumes})
        self.assertEqual(len(self.members(artifact)), 12)
        latest = self.latest()
        bad_volume = artifact.volumes[-1]
        original = bad_volume.read_bytes()
        bad_volume.write_bytes(original + b"bad")
        with self.assertRaises(StorageError):
            self.store.save_day(daily(day, value=8))
        self.assertEqual(self.latest(), latest)
        bad_volume.write_bytes(original)
        old_volumes = artifact.volumes
        self.store.volume_bytes = 1024 * 1024
        self.store.save_day(daily(day, value=8, sharded=False))
        self.assertTrue(artifact.archive.exists())
        self.assertTrue(all(not path.exists() for path in old_volumes))
        self.assertFalse((self.root / "raw/latest/visa.parts").exists())
        self.assertEqual(len(self.members(artifact)), 3)
        self.assertEqual(self.store.verify_archives(), 1)

    def test_cross_year_three_month_window_and_streamed_annual_snapshot_once(self):
        for month in (9, 10, 11, 12):
            self.store.save_day(daily(date(2025, month, 20)))
        self.store.save_day(daily(date(2026, 1, 1)))
        with mock.patch("exchange_rates.storage._extract_verified", side_effect=AssertionError("extract forbidden")):
            periods, snapshot = self.store.archive_due(date(2026, 1, 3))
        self.assertEqual(periods, ["2025-09", "2025-10", "2025-11", "2025-12", "2025"])
        self.assertTrue(snapshot)
        self.assertEqual(set(self.store._repo_months()), {"2025-10", "2025-11", "2025-12"})
        self.assertFalse(self.store.year_artifact(2025).exists)
        self.assertEqual(len(self.remote("2025")), 48)
        self.assertTrue(all(self.backend.has(f"2025-{month:02d}") for month in (9, 10, 11, 12)))
        self.backend.downloaded.clear()
        self.assertEqual(self.store.archive_due(date(2026, 1, 3)), ([], False))
        self.store.verify_archives()
        self.assertNotIn("2025", self.backend.downloaded)
        self.assertTrue((self.root / "history/2026/01/visa/2026-01-01.json").exists())

    def test_annual_uses_old_month_backend_not_repo_and_supplement_syncs_both(self):
        self.store.save_day(daily(date(2025, 1, 20)))
        self.store.save_day(daily(date(2025, 1, 20), "mastercard"))
        self.store.archive_due(date(2025, 2, 3))
        self.store.archive_due(date(2025, 6, 3))
        self.assertFalse(self.store.month_artifact(2025, 1).exists)
        self.store.save_day(daily(date(2025, 12, 20)))
        self.store.save_day(daily(date(2026, 1, 1)))
        before_latest = self.latest()
        with mock.patch("exchange_rates.storage._extract_verified", side_effect=AssertionError("extract forbidden")):
            self.store.archive_due(date(2026, 1, 3))
            before = self.remote("2025")
            self.store.save_day(daily(date(2025, 1, 20), value=8, sharded=False))
            self.store.save_day(daily(date(2025, 1, 21), value=9))
        after = self.remote("2025")
        month = self.remote("2025-01")
        self.assertEqual(len(after), 39)
        self.assertEqual(len(month), 27)
        for name, body in before.items():
            if "/mastercard/" in name or "/12/" in name:
                self.assertEqual(after[name], body)
        self.assertEqual(after["history/2025/01/visa/2025-01-20.json"], json_bytes({"rate": 8}))
        self.assertEqual(after["history/2025/01/visa/2025-01-21.json"], json_bytes({"rate": 9}))
        self.assertTrue(all(after[name] == body for name, body in month.items()))
        self.assertFalse(self.store.month_artifact(2025, 1).exists)
        self.assertFalse(self.store.year_artifact(2025).exists)
        self.assertEqual(self.latest(), before_latest)

    def test_old_month_only_backend_supplement_preserves_members(self):
        self.store.save_day(daily(date(2026, 1, 20)))
        self.store.archive_due(date(2026, 2, 3))
        self.store.archive_due(date(2026, 6, 3))
        self.assertFalse(self.store.month_artifact(2026, 1).exists)
        before = self.remote("2026-01")
        self.store.save_day(daily(date(2026, 1, 21)))
        after = self.remote("2026-01")
        self.assertEqual(len(after), 24)
        self.assertTrue(all(after[name] == body for name, body in before.items()))
        self.assertFalse(self.store.month_artifact(2026, 1).exists)

    def test_pack_and_publish_failures_keep_daily_and_latest(self):
        day = date(2026, 9, 30)
        self.store.save_day(daily(day))
        latest = self.latest()
        with mock.patch("exchange_rates.storage._pack_stream", side_effect=RuntimeError("fixture pack failed")):
            with self.assertRaises(RuntimeError):
                self.store.archive_due(date(2026, 10, 3))
        self.assertTrue((self.root / "raw/history/2026/09/visa/2026-09-30.parts/shard-09.json").exists())
        self.assertEqual(self.latest(), latest)
        self.backend.fail_period = "2026-09"
        with self.assertRaises(RuntimeError):
            self.store.archive_due(date(2026, 10, 3))
        self.assertTrue((self.root / "history/2026/09/visa/2026-09-30.json").exists())
        self.assertEqual(self.latest(), latest)
        self.assertFalse(self.store.month_artifact(2026, 9).exists)
        self.backend.fail_period = None
        self.assertEqual(self.store.archive_due(date(2026, 10, 3)), (["2026-09"], False))

    def test_partial_month_year_publish_keeps_repo_and_replays(self):
        day = date(2025, 12, 20)
        self.store.save_day(daily(day))
        self.store.archive_due(date(2026, 1, 3))
        monthly = self.store.month_artifact(2025, 12)
        before_files = {path: path.read_bytes() for path in monthly.files}
        before_year = self.remote("2025")
        before_latest = self.latest()
        self.backend.fail_period = "2025"
        with self.assertRaises(RuntimeError):
            self.store.save_day(daily(day, value=8))
        self.assertEqual({path: path.read_bytes() for path in monthly.files}, before_files)
        self.assertEqual(self.latest(), before_latest)
        self.assertEqual(self.remote("2025"), before_year)
        self.assertNotEqual(self.remote("2025-12"), before_year)
        state = json.loads((self.root / "maintenance.json").read_bytes())
        self.assertEqual(state["last_error"]["published"], ["2025-12"])
        with self.assertRaises(StorageError):
            self.store.archive_due(date(2026, 1, 3))
        self.assertIn("last_error", json.loads((self.root / "maintenance.json").read_bytes()))
        self.backend.fail_period = None
        self.store.save_day(daily(day, value=8))
        self.assertEqual(self.remote("2025"), self.remote("2025-12"))
        self.assertEqual(self.members(monthly), self.remote("2025"))
        self.assertNotIn("last_error", json.loads((self.root / "maintenance.json").read_bytes()))
        self.assertEqual(self.store.archive_due(date(2026, 1, 3)), ([], False))

    def test_annual_publish_failure_does_not_mark_snapshot(self):
        self.store.save_day(daily(date(2025, 12, 20)))
        self.backend.fail_period = "2025"
        with self.assertRaises(RuntimeError):
            self.store.archive_due(date(2026, 1, 3))
        state = json.loads((self.root / "maintenance.json").read_bytes())
        self.assertNotIn("2025", state["published_years"])
        self.assertNotIn("last_snapshot_year", state)
        self.backend.fail_period = None
        self.assertEqual(self.store.archive_due(date(2026, 1, 3)), (["2025"], True))
        self.assertEqual(self.store.archive_due(date(2026, 1, 3)), ([], False))

    def test_legacy_manifest_without_volumes_and_missing_backend_republish(self):
        self.store.save_day(daily(date(2026, 9, 30), sharded=False))
        self.store.archive_due(date(2026, 10, 3))
        artifact = self.store.month_artifact(2026, 9)
        manifest = json.loads(artifact.manifest.read_bytes())
        del manifest["volumes"]
        del manifest["stream_sha256"]
        artifact.manifest.write_bytes(json_bytes(manifest))
        artifact.checksums.write_text(f"{sha256(artifact.archive)}  {artifact.archive.name}\n"
                                      f"{sha256(artifact.manifest)}  {artifact.manifest.name}\n", encoding="ascii")
        self.assertEqual(self.store.verify_archives(), 1)
        missing_backend = ControlledBackend(self.base / "empty-releases")
        self.store = DataStore(self.root, missing_backend)
        self.assertEqual(self.store.archive_due(date(2026, 10, 3)), (["2026-09"], False))
        self.assertTrue(missing_backend.has("2026-09"))
        self.store.save_day(daily(date(2026, 9, 29), sharded=False))
        self.assertEqual(len(self.members(artifact)), 6)

    def test_paths_reject_cross_period_and_non_raw_parts(self):
        for name in ("raw/history/2026/09/visa/2026-09-30.parts/../shard-01.json",
                     "raw/history/2026/09/visa/2026-08-30.parts/shard-01.json",
                     "history/2026/09/visa/2026-09-30.parts/shard-01.json",
                     "/raw/history/2026/09/visa/2026-09-30.parts/shard-01.json",
                     "raw/history/2026/09/visa/2026-09-31.parts/shard-01.json"):
            with self.subTest(name=name), self.assertRaises(StorageError):
                member_info(name, "2026-09")


if __name__ == "__main__":
    unittest.main()
