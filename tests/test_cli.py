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
from exchange_rates.cli import dates, load_config, main, parser
from exchange_rates.diagnostics import Diagnostics
from exchange_rates.models import DayResult, json_bytes
from exchange_rates.providers import RecoveryError


class FakeClient:
    def __init__(self, **kwargs):
        self.records = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


class CliTests(unittest.TestCase):
    def setUp(self):
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.default_diagnostics = Path(temporary) / "diagnostics"
        self.enterContext(patch("exchange_rates.cli.Diagnostics", side_effect=lambda directory, data_dir=None:
                               Diagnostics(self.default_diagnostics if directory == Path("diagnostics") else directory,
                                           data_dir)))

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

    def test_diagnostics_default_and_provider_http_overrides(self):
        self.assertEqual(parser().parse_args(["fetch", "--provider", "visa"]).diagnostics_dir,
                         Path("diagnostics"))
        self.assertEqual(parser().parse_args(["archive"]).diagnostics_dir, Path("diagnostics"))
        config = load_config(None)
        config["http"] = {"timeout": 5, "global_interval": 0.1}
        config["providers"]["mastercard"]["http"] = {"global_interval": 0.5}
        with tempfile.TemporaryDirectory() as temporary, \
             patch("exchange_rates.cli.load_config", return_value=config), \
             patch("exchange_rates.cli.HttpClient", side_effect=FakeClient) as factory, \
             patch("exchange_rates.cli.prepare_catalog", side_effect=RuntimeError("catalog fixture")), \
             contextlib.redirect_stderr(io.StringIO()):
            status = main(["fetch", "--provider", "mastercard", "--data-dir", temporary])
        self.assertEqual(status, 1)
        factory.assert_called_once_with(timeout=5, global_interval=0.5)
        self.assertTrue((self.default_diagnostics / "mastercard/run-error/report.json").is_file())

    def test_nine_megabyte_error_is_artifact_not_terminal_or_summary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            diagnostics = root / "diagnostics"
            summary = root / "step-summary"
            body = json_bytes({"requests": [{"response": {"status_code": 403, "body_text": "denied"}}]})
            pair = ("USD", "CNY")
            error = RecoveryError("mastercard", [pair], {pair: "x" * (9 * 1024 * 1024)},
                                  {"shard-01.json": body}, {"initial_failed": 1, "rounds": 2, "repaired": 0})
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch("exchange_rates.cli.HttpClient", FakeClient), \
                 patch("exchange_rates.cli.prepare_catalog", return_value=None), \
                 patch("exchange_rates.cli.fetch_day", side_effect=error), \
                 patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": str(summary)}), \
                 contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = main(["fetch", "--provider", "mastercard", "--start-date", "2026-10-01",
                               "--end-date", "2026-10-03", "--data-dir", str(root / "data"),
                               "--diagnostics-dir", str(diagnostics)])
            self.assertEqual(status, 1)
            self.assertLess(summary.stat().st_size, 1024 * 1024)
            self.assertLess(len(stdout.getvalue().encode()) + len(stderr.getvalue().encode()), 12000)
            for day in (1, 2, 3):
                folder = diagnostics / f"mastercard/2026-10-{day:02d}"
                self.assertGreater((folder / "report.json").stat().st_size, 9 * 1024 * 1024)
                self.assertEqual((folder / "raw/shard-01.json").read_bytes(), body)
            self.assertEqual(json.loads((diagnostics / "summary.json").read_bytes())["failed_dates"], 3)

    def test_main_failure_and_unwritable_artifacts_preserve_business_exit(self):
        error = RuntimeError("business " + "x" * (1024 * 1024))
        error.context = {"phase": "catalog"}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "step-summary"
            stderr = io.StringIO()
            with patch("exchange_rates.cli.HttpClient", FakeClient), \
                 patch("exchange_rates.cli.prepare_catalog", side_effect=error), \
                 patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": str(summary)}), \
                 contextlib.redirect_stderr(stderr):
                status = main(["fetch", "--provider", "visa", "--data-dir", str(root / "data"),
                               "--diagnostics-dir", str(root / "diagnostics")])
            self.assertEqual(status, 1)
            report = json.loads((root / "diagnostics/visa/run-error/report.json").read_bytes())
            self.assertEqual(report["error"], str(error))
            self.assertEqual(report["context"], error.context)
            self.assertLess(len(stderr.getvalue().encode()), 2000)
            with patch("exchange_rates.cli.HttpClient", FakeClient), \
                 patch("exchange_rates.cli.prepare_catalog", side_effect=error), \
                 patch("exchange_rates.diagnostics.Diagnostics._write", side_effect=PermissionError("no fixture write")), \
                 patch("exchange_rates.cli.write_summary", side_effect=OSError("summary read-only")), \
                 contextlib.redirect_stderr(stderr):
                self.assertEqual(main(["fetch", "--provider", "visa", "--data-dir", str(root / "data")]), 1)
            self.assertIn("business", stderr.getvalue())
            self.assertIn("no fixture write", stderr.getvalue())

    def test_partial_fetch_failures_still_publish_successful_dates(self):
        def fetch(provider, target, catalog, config, client):
            if target.day == 2:
                raise RuntimeError("fixture day failure")
            return DayResult(provider, target, b"{}", b"{}", {"pair_count": 2, "recovery": {"rounds": 0}})

        with tempfile.TemporaryDirectory() as temporary, \
             patch("exchange_rates.cli.HttpClient", FakeClient), \
             patch("exchange_rates.cli.prepare_catalog", return_value=None), \
             patch("exchange_rates.cli.GitHubArchiveBackend.from_environment",
                   return_value=DirectoryArchiveBackend(Path(temporary) / "releases")), \
             patch("exchange_rates.cli.fetch_day", side_effect=fetch), \
             patch("exchange_rates.cli.DataStore.save_day") as save, \
             patch("exchange_rates.cli.BranchPublisher") as publisher, \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            status = main(["fetch", "--provider", "visa", "--start-date", "2026-10-01",
                           "--end-date", "2026-10-03", "--data-dir", str(Path(temporary) / "data"), "--publish"])
        self.assertEqual(status, 1)
        self.assertEqual(save.call_count, 2)
        publisher.return_value.publish.assert_called_once()
        run = json.loads((self.default_diagnostics / "summary.json").read_bytes())
        self.assertEqual(run["saved_dates"], 2)
        self.assertEqual(run["failed_dates"], 1)

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
