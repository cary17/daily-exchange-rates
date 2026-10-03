import contextlib
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from exchange_rates.cli import dates, main
from exchange_rates.models import DayResult, json_bytes


class FakeClient:
    def __init__(self, **kwargs):
        self.records = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


class CliTests(unittest.TestCase):
    def test_blank_dates_use_beijing_today(self):
        with patch("exchange_rates.cli.beijing_today", return_value=date(2026, 10, 3)):
            self.assertEqual(dates("", ""), (date(2026, 10, 3), date(2026, 10, 3)))
        with self.assertRaises(ValueError):
            dates("2026-10-03", "2026-10-01")
        with self.assertRaises(ValueError):
            dates("20261003", "")

    def test_range_failure_keeps_previous_complete_day_without_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            from exchange_rates.storage import DataStore
            store = DataStore(root)
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
                 patch("exchange_rates.cli.fetch_day", side_effect=fetch), \
                 contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                status = main(["fetch", "--provider", "visa", "--start-date", "2026-10-01",
                               "--end-date", "2026-10-03", "--data-dir", str(root)])
            self.assertEqual(status, 1)
            self.assertEqual(called, [date(2026, 10, day) for day in (1, 2, 3)])
            self.assertEqual((root / "history/2026/10/visa/2026-10-02.json").read_bytes(), old_body)
            self.assertEqual(json.loads((root / "metadata/latest/visa.json").read_bytes())["requested_date"], "2026-10-03")

    def test_storage_failure_aborts_instead_of_publishing_partial_state(self):
        value = DayResult("visa", date(2026, 10, 3), b"{}", b"{}", {})
        with tempfile.TemporaryDirectory() as temporary, \
             patch("exchange_rates.cli.HttpClient", FakeClient), \
             patch("exchange_rates.cli.fetch_day", return_value=value), \
             patch("exchange_rates.cli.DataStore.save_day", side_effect=OSError("Disk failure")), \
             patch("exchange_rates.cli.BranchPublisher") as publisher, \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            status = main(["fetch", "--provider", "visa", "--start-date", "2026-10-03",
                           "--data-dir", temporary, "--publish"])
            self.assertEqual(status, 1)
            publisher.return_value.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
