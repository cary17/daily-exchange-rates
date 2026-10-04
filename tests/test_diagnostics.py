import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from exchange_rates.diagnostics import Diagnostics
from exchange_rates.http import HttpError, JsonResponse
from exchange_rates.models import DayResult, json_bytes
from exchange_rates.providers import RecoveryError
from exchange_rates.publishing import write_summary


class DiagnosticsTests(unittest.TestCase):
    def test_recovery_retains_complete_errors_and_exact_raw_parts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "diagnostics"
            records = [{"response": {"status_code": 403, "body_text": "blocked"}}]
            body = json_bytes({"requests": records, "attempts": [{"pair": ["USD", "CNY"]}]})
            pairs = [("USD", "CNY"), ("EUR", "CNY"), ("JPY", "CNY"), ("GBP", "CNY")]
            errors = {pair: "HTTP 403 " + "x" * 1000 for pair in pairs}
            error = RecoveryError("mastercard", pairs, errors, {"shard-01.json": body},
                                  {"initial_failed": 4, "rounds": 2, "repaired": 0})
            diagnostics = Diagnostics(root)
            entry = diagnostics.failure("mastercard", "2026-10-03", error)
            details = json.loads((root / "mastercard/2026-10-03/report.json").read_bytes())
            self.assertEqual(details["error"], str(error))
            self.assertEqual(details["missing_pairs"], [list(pair) for pair in pairs])
            self.assertEqual(len(details["errors"]), 4)
            self.assertEqual(details["recovery"]["rounds"], 2)
            self.assertEqual(details["http_counts"], {"403": 1})
            self.assertEqual((root / "mastercard/2026-10-03/raw/shard-01.json").read_bytes(), body)
            self.assertLess(len(entry["brief"].encode()), 2000)
            self.assertNotIn("GBP/CNY", entry["brief"])
            self.assertTrue(Path(entry["report"]).is_file())

    def test_http_context_records_attempts_and_body_survive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "diagnostics"
            response = JsonResponse(None, "denied", b"denied\x00", "https://institution.test",
                                    request_headers={"Authorization": "secret", "Cookie": "secret"},
                                    status_code=403, response_headers={"Set-Cookie": "secret"})
            error = HttpError("HTTP 403", 403, response, [response.as_record()])
            error.attempts = [{"round": 0}]
            error.context = {"provider": "mastercard", "expected_pairs": 99,
                             "successful_pairs": 2, "attempted_pairs": 5,
                             "stopped_for_access_control": True}
            error.request_sent = False
            error.last_status = 403
            diagnostics = Diagnostics(root)
            entry = diagnostics.failure("mastercard", "2026-10-03", error)
            details = json.loads((root / "mastercard/2026-10-03/report.json").read_bytes())
            self.assertEqual(details["context"], error.context)
            self.assertFalse(details["request_sent"])
            self.assertEqual(details["last_status"], 403)
            self.assertEqual(details["attempts"], [{"round": 0}])
            self.assertEqual(details["response"]["response"]["status_code"], 403)
            self.assertNotIn("secret", json.dumps(details))
            self.assertEqual((root / "mastercard/2026-10-03/raw/response.bin").read_bytes(), response.body)
            self.assertIn("completed 2/99", entry["brief"])
            self.assertIn("uncompleted=97", entry["brief"])
            self.assertIn("stopped early for access control", entry["brief"])
            self.assertIsNone(entry["failed_pairs"])

    def test_diagnostics_failures_and_data_overlap_do_not_replace_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            diagnostics = Diagnostics(root / "data/diagnostics", root / "data")
            entry = diagnostics.failure("visa", "2026-10-03", RuntimeError("business failure"))
            self.assertIn("business failure", entry["brief"])
            self.assertIn("must not overlap", entry["diagnostic_error"])
            self.assertFalse((root / "data").exists())
            diagnostics = Diagnostics(root / "diagnostics")
            with patch.object(Path, "mkdir", side_effect=PermissionError("read-only fixture")):
                entry = diagnostics.failure("visa", "2026-10-03", RuntimeError("business failure"))
                summary = diagnostics.finish("fetch", "visa", 1)
            self.assertIn("business failure", entry["brief"])
            self.assertIn("read-only fixture", summary)

    def test_raw_paths_and_symlinks_never_write_outside_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "diagnostics"
            error = RecoveryError("visa", [("USD", "CNY")], {("USD", "CNY"): "error"},
                                  {"../../escape": b"original"}, {"rounds": 0})
            diagnostics = Diagnostics(root)
            diagnostics.failure("visa", "2026-10-03", error)
            self.assertFalse((Path(temporary) / "escape").exists())
            self.assertEqual((root / "visa/2026-10-03/raw/part-0000.bin").read_bytes(), b"original")
            outside = Path(temporary) / "outside"
            outside.mkdir()
            (root / "visa/2026-10-04").symlink_to(outside, target_is_directory=True)
            entry = diagnostics.failure("visa", "2026-10-04", error)
            self.assertIn("escapes", entry["diagnostic_error"])
            self.assertEqual(list(outside.iterdir()), [])

    def test_success_has_counts_and_recovery_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "diagnostics"
            diagnostics = Diagnostics(root)
            metadata = {"pair_count": 42, "recovery": {"initial_failed": 3, "rounds": 1, "repaired": 3}}
            diagnostics.success(DayResult("visa", date(2026, 10, 3), b"{}", b"{}", metadata))
            brief = diagnostics.finish("fetch", "visa", 0)
            summary = json.loads((root / "summary.json").read_bytes())
            self.assertEqual(summary["days"][0]["metadata"], metadata)
            self.assertEqual(summary["saved_dates"], 1)
            self.assertIn("saved 42 pairs", brief)
            self.assertIn("repaired", (root / "brief.md").read_text())

    def test_summary_utf8_byte_cap_includes_existing_and_repeated_writes(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "step-summary"
            path.write_bytes(("initial " + "\u4e2d" * 180000).encode())
            with patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(path)}):
                write_summary("\u6587" * (3 * 1024 * 1024))
                for _ in range(20):
                    write_summary("next day " + "\u6587" * 10000)
            self.assertLessEqual(path.stat().st_size, 512 * 1024)
            self.assertLess(path.stat().st_size, 1024 * 1024)
            self.assertIn("Summary truncated", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
