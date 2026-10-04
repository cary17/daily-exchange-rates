import contextlib
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from exchange_rates.archive_backend import DirectoryArchiveBackend
from exchange_rates.catalog import CurrencyCatalog
from exchange_rates.cli import dates, load_config, main
from exchange_rates.models import DayResult, json_bytes


class FakeClient:
    def __init__(self, **kwargs):
        self.records = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


class CliTests(unittest.TestCase):
    def test_recovery_configuration_is_checked_before_collection(self):
        config = load_config(None)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "providers.json"
            for value in (-1, True, "2", 1.5):
                with self.subTest(value=value):
                    config["providers"]["visa"]["recovery_rounds"] = value
                    path.write_text(json.dumps(config))
                    with self.assertRaisesRegex(ValueError, "recovery_rounds"):
                        load_config(path)
            config["providers"]["visa"]["recovery_rounds"] = 0
            path.write_text(json.dumps(config))
            self.assertEqual(load_config(path)["providers"]["visa"]["recovery_rounds"], 0)

    def test_blank_dates_use_beijing_today(self):
        with patch("exchange_rates.cli.beijing_today", return_value=date(2026, 10, 3)):
            self.assertEqual(dates("", ""), (date(2026, 10, 3), date(2026, 10, 3)))
        with self.assertRaises(ValueError):
            dates("2026-10-03", "2026-10-01")
        with self.assertRaises(ValueError):
            dates("20261003", "")

    def test_range_failure_keeps_previous_complete_day_without_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "data"
            release_dir = Path(temporary) / "releases"
            from exchange_rates.storage import DataStore
            store = DataStore(root, DirectoryArchiveBackend(release_dir))
            old = DayResult("visa", date(2026, 10, 2), json_bytes({"exchangeRateJson": []}),
                            json_bytes({"old": True}), {})
            store.save_day(old)
            old_body = (root / "history/2026/10/visa/2026-10-02.json").read_bytes()
            called = []

            def fetch(provider, target, currencies, config, client):
                called.append(target)
                if target == date(2026, 10, 2):
                    raise RuntimeError("No data for requested day")
                return DayResult(provider, target, json_bytes({"exchangeRateJson": []}),
                                 json_bytes({"requested": target.isoformat()}), {})

            with patch("exchange_rates.cli.HttpClient", FakeClient), \
                 patch("exchange_rates.cli.prepare_catalog", return_value=CurrencyCatalog(("USD", "CNY"), ("USD", "CNY"))) as discover, \
                 patch("exchange_rates.cli.fetch_day", side_effect=fetch), \
                 contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                status = main(["fetch", "--provider", "visa", "--start-date", "2026-10-01",
                               "--end-date", "2026-10-03", "--data-dir", str(root),
                               "--release-dir", str(release_dir)])
            self.assertEqual(discover.call_count, 1)
            self.assertEqual(status, 1)
            self.assertEqual(called, [date(2026, 10, day) for day in (1, 2, 3)])
            self.assertEqual((root / "history/2026/10/visa/2026-10-02.json").read_bytes(), old_body)
            self.assertEqual(json.loads((root / "metadata/latest/visa.json").read_bytes())["requested_date"], "2026-10-03")

    def test_storage_failure_aborts_instead_of_publishing_partial_state(self):
        value = DayResult("visa", date(2026, 10, 3), b"{}", b"{}", {})
        with tempfile.TemporaryDirectory() as temporary, \
             patch("exchange_rates.cli.HttpClient", FakeClient), \
             patch("exchange_rates.cli.prepare_catalog", return_value=CurrencyCatalog(("USD", "CNY"), ("USD", "CNY"))), \
             patch("exchange_rates.cli.GitHubArchiveBackend.from_environment",
                   return_value=DirectoryArchiveBackend(Path(temporary) / "releases")), \
             patch("exchange_rates.cli.fetch_day", return_value=value), \
             patch("exchange_rates.cli.DataStore.save_day", side_effect=OSError("Disk failure")) as save, \
             patch("exchange_rates.cli.BranchPublisher") as publisher, \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            status = main(["fetch", "--provider", "visa", "--start-date", "2026-10-03",
                           "--data-dir", temporary, "--publish"])
            self.assertEqual(status, 1)
            save.assert_called_once()
            publisher.return_value.publish.assert_not_called()

    def test_download_uses_verified_release_backend(self):
        with tempfile.TemporaryDirectory() as temporary, \
             patch("exchange_rates.cli.GitHubArchiveBackend") as factory, \
             patch.dict("os.environ", {"GITHUB_TOKEN": "fixture-token"}), \
             contextlib.redirect_stdout(io.StringIO()):
            output = Path(temporary) / "download"
            factory.return_value.materialize.return_value = {"2026-09.tar.gz": output / "2026-09.tar.gz"}
            status = main(["download", "--repository", "cary17/daily-exchange-rates",
                           "--period", "2026-09", "--output-dir", str(output)])
            self.assertEqual(status, 0)
            factory.assert_called_once_with("cary17/daily-exchange-rates", "fixture-token")
            factory.return_value.materialize.assert_called_once_with("2026-09", output)

    def test_download_keeps_existing_output_directory(self):
        with tempfile.TemporaryDirectory() as temporary, \
             patch("exchange_rates.cli.GitHubArchiveBackend") as factory, \
             contextlib.redirect_stderr(io.StringIO()):
            output = Path(temporary)
            marker = output / "keep.txt"
            marker.write_text("keep")
            self.assertEqual(main(["download", "--repository", "cary17/daily-exchange-rates",
                                   "--period", "2026-09", "--output-dir", str(output)]), 1)
            factory.assert_not_called()
            self.assertEqual(marker.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
