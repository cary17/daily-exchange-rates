import contextlib
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from exchange_rates.archive_backend import DirectoryArchiveBackend
from exchange_rates.catalog import CurrencyCatalog
from exchange_rates.cli import dates, load_config, main, parser
from exchange_rates.diagnostics import Diagnostics
from exchange_rates.models import DayResult, json_bytes
from exchange_rates.providers import ProviderError, RecoveryError


class FakeClient:
    def __init__(self, **kwargs):
        self.records = []
        self.interval = kwargs.get("interval", 0.3)
        self.global_interval = kwargs.get("global_interval", 0)

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

    def test_default_pacing_is_per_session_without_shared_limit(self):
        config = load_config(None)
        self.assertEqual(config["http"]["interval"], 0.3)
        self.assertEqual(config["http"]["global_interval"], 0)
        for provider in ("visa", "mastercard"):
            with self.subTest(provider=provider):
                self.assertNotIn("global_interval", config["providers"][provider].get("http", {}))

    def test_interval_parser_defaults_and_explicit_values(self):
        self.assertIsNone(parser().parse_args(["fetch", "--provider", "visa"]).interval)
        for text, expected in (("0", 0.0), ("0.75", 0.75)):
            with self.subTest(value=text):
                args = parser().parse_args(["fetch", "--provider", "visa", "--interval", text])
                self.assertEqual(args.interval, expected)

    def test_invalid_interval_exits_before_client_or_diagnostics(self):
        for value in ("-0.1", "nan", "inf", "-inf", "1e309", "invalid"):
            with self.subTest(value=value), \
                 patch("exchange_rates.cli.HttpClient") as client, \
                 patch("exchange_rates.cli.Diagnostics") as diagnostics, \
                 contextlib.redirect_stderr(io.StringIO()), \
                 self.assertRaises(SystemExit) as caught:
                main(["fetch", "--provider", "visa", f"--interval={value}"])
            self.assertEqual(caught.exception.code, 2)
            client.assert_not_called()
            diagnostics.assert_not_called()

    def test_interval_cli_overrides_provider_but_omission_preserves_config(self):
        config = load_config(None)
        config["http"] = {"interval": 0.5, "global_interval": 0}
        config["providers"]["mastercard"]["http"] = {"interval": 0.9}
        for arguments, expected in (([], 0.9), (["--interval", "0"], 0.0),
                                    (["--interval", "0.75"], 0.75)):
            output = io.StringIO()
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as temporary, \
                 patch("exchange_rates.cli.load_config", return_value=config), \
                 patch("exchange_rates.cli.HttpClient", side_effect=FakeClient) as factory, \
                 patch("exchange_rates.cli.prepare_catalog", side_effect=RuntimeError("catalog fixture")), \
                 contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
                status = main(["fetch", "--provider", "mastercard", "--data-dir", temporary,
                               *arguments])
            self.assertEqual(status, 1)
            factory.assert_called_once_with(interval=expected, global_interval=0)
            self.assertIn(f"per-session interval={expected:g}s; global interval=0s", output.getvalue())
            self.assertEqual(config["providers"]["mastercard"]["http"]["interval"], 0.9)

    def test_unionpay_workflow_keeps_single_job_with_interval_default(self):
        workflows = Path(__file__).resolve().parents[1] / ".github" / "workflows"
        text = (workflows / "fetch-unionpay.yml").read_text()
        self.assertIn("      interval:\n", text)
        self.assertIn("        default: '0.3'\n        type: string", text)
        self.assertIn("REQUEST_INTERVAL: ${{ inputs.interval || '0.3' }}", text)
        self.assertIn('--interval "$REQUEST_INTERVAL"', text)
        self.assertNotIn("fetch-shard", text)
        self.assertNotIn("global_interval", text)

    def test_sharded_workflows_split_jobs_then_merge_bundles(self):
        workflows = Path(__file__).resolve().parents[1] / ".github" / "workflows"
        for provider in ("visa", "mastercard"):
            with self.subTest(provider=provider):
                text = (workflows / f"fetch-{provider}.yml").read_text()
                # Defaults are the single source of truth for pacing.
                self.assertIn("        default: '0.3'\n        type: string", text)
                self.assertIn("REQUEST_INTERVAL: ${{ inputs.interval || '0.3' }}", text)
                self.assertIn("        default: '9'\n        type: string", text)
                self.assertNotIn("global_interval", text)
                # Three stages: plan, one job per shard, then verify and merge.
                for job in ("  prepare:\n", "  shard:\n", "  merge:\n"):
                    self.assertIn(job, text)
                self.assertIn("matrix: ${{ fromJSON(needs.prepare.outputs.matrix) }}", text)
                self.assertIn("fail-fast: false", text)
                self.assertIn('python -m exchange_rates fetch-shard', text)
                self.assertIn('python -m exchange_rates merge-shards', text)
                self.assertIn('--shard-index "$SHARD_INDEX"', text)
                self.assertIn('--shard-count "$SHARD_COUNT"', text)
                self.assertIn('--shard-dir "$RUNNER_TEMP/shards"', text)
                # Bundles must round-trip through artifacts without the diagnostics.
                self.assertIn(f"          name: {provider}-shard-${{{{ matrix.shard }}}}", text)
                self.assertIn(f"          pattern: {provider}-shard-*", text)
                self.assertIn("          merge-multiple: true", text)
                self.assertNotIn(f"{provider}-shard-${{{{ matrix.shard }}}}-diagnostics", text)

    def test_fetch_shard_and_merge_shards_cli_contract(self):
        plan = parser().parse_args([
            "fetch-shard", "--provider", "mastercard", "--start-date", "2026-10-03",
            "--end-date", "2026-10-03", "--shard-index", "4", "--shard-count", "9",
            "--shard-dir", "/tmp/shards", "--interval", "0.3",
        ])
        self.assertEqual((plan.provider, plan.shard_index, plan.shard_count),
                         ("mastercard", 4, 9))
        self.assertEqual(plan.interval, 0.3)
        # Shard jobs only stage bundles; they never touch published data.
        self.assertFalse(hasattr(plan, "publish"))
        self.assertEqual(parser().parse_args(
            ["fetch-shard", "--provider", "visa", "--shard-index", "0",
             "--shard-dir", "/tmp/s"]).shard_count, 9)
        merge = parser().parse_args([
            "merge-shards", "--provider", "visa", "--start-date", "2026-10-03",
            "--shard-dir", "/tmp/shards",
        ])
        self.assertEqual(merge.command, "merge-shards")
        self.assertFalse(merge.publish)
        with self.assertRaises(SystemExit):
            parser().parse_args(["fetch-shard", "--provider", "mastercard",
                                 "--shard-dir", "/tmp/s"])
        with self.assertRaises(SystemExit):
            parser().parse_args(["fetch-shard", "--provider", "unionpay",
                                 "--shard-index", "0", "--shard-dir", "/tmp/s"])
        with self.assertRaises(SystemExit):
            parser().parse_args(["merge-shards", "--provider", "visa",
                                 "--shard-dir", "/tmp/s", "--interval", "0.3"])

    def test_fetch_shard_writes_bundle_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = {"format": "exchange-rates-shard-v1", "provider": "mastercard",
                      "requested_date": "2026-10-03", "shard_index": 2, "shard_count": 9,
                      "rows": [{"transCur": "USD", "baseCur": "CNY", "rateData": "7.1"}]}
            with patch("exchange_rates.cli.HttpClient", FakeClient), \
                 patch("exchange_rates.cli.fetch_shard", return_value=bundle), \
                 contextlib.redirect_stdout(io.StringIO()):
                status = main(["fetch-shard", "--provider", "mastercard",
                               "--start-date", "2026-10-03", "--end-date", "2026-10-03",
                               "--shard-index", "2", "--shard-count", "9",
                               "--shard-dir", str(root / "shards"),
                               "--diagnostics-dir", str(root / "diag")])
            self.assertEqual(status, 0)
            path = root / "shards/mastercard/2026-10-03/shard-03.json"
            self.assertEqual(json.loads(path.read_bytes())["rows"][0]["rateData"], "7.1")

    def test_merge_shards_publishes_verified_day(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shard_dir, data_dir = root / "shards", root / "data"
            directory = shard_dir / "mastercard/2026-10-03"
            directory.mkdir(parents=True)
            rows_by_shard = {0: [("USD", "CNY", "7.1")], 1: [("CNY", "USD", "0.14")]}
            for index in range(9):
                bundle = {
                    "format": "exchange-rates-shard-v1", "provider": "mastercard",
                    "requested_date": "2026-10-03", "shard_index": index, "shard_count": 9,
                    "transaction_currencies": ["USD", "CNY"],
                    "billing_currencies": ["USD", "CNY"],
                    "catalog_source_urls": [], "catalog_requests": [],
                    "response_dates": [], "recovery": {"rounds": 0},
                    "rows": [{"transCur": spend, "baseCur": home, "rateData": rate}
                             for spend, home, rate in rows_by_shard.get(index, [])],
                    "raw": {"provider": "mastercard", "requested_date": "2026-10-03",
                            "shard_index": index, "transaction_currencies": [],
                            "requests": [], "attempts": []},
                }
                (directory / f"shard-{index + 1:02d}.json").write_bytes(
                    json_bytes(bundle, compact=True))
            with patch("exchange_rates.cli.BranchPublisher") as publisher, \
                 patch("exchange_rates.cli.GitHubArchiveBackend.from_environment",
                       return_value=DirectoryArchiveBackend(root / "releases")), \
                 contextlib.redirect_stdout(io.StringIO()), \
                 contextlib.redirect_stderr(io.StringIO()):
                status = main(["merge-shards", "--provider", "mastercard",
                               "--start-date", "2026-10-03", "--end-date", "2026-10-03",
                               "--shard-dir", str(shard_dir), "--data-dir", str(data_dir),
                               "--diagnostics-dir", str(root / "diag"), "--publish"])
            self.assertEqual(status, 0)
            saved = json.loads(
                (data_dir / "history/2026/10/mastercard/2026-10-03.json").read_bytes())
            self.assertEqual([(row["transCur"], row["baseCur"])
                              for row in saved["exchangeRateJson"]],
                             [("USD", "CNY"), ("CNY", "USD")])
            self.assertEqual(
                json.loads((data_dir / "metadata/latest/mastercard.json").read_bytes())["pair_count"], 2)
            self.assertTrue((data_dir / "raw/history/2026/10/mastercard/"
                             "2026-10-03.parts/shard-09.json").is_file())
            publisher.return_value.publish.assert_called_once()

    def test_shard_denial_records_exchange_once_with_context(self):
        from exchange_rates.http import AccessBlockedError, HttpClient
        from exchange_rates.providers import fetch_shard
        catalog = CurrencyCatalog(("USD", "CNY", "EUR"), ("USD", "CNY", "EUR"))
        calls = []

        def responder(status, body):
            return SimpleNamespace(status_code=status, content=body,
                                   text=body.decode(),
                                   url="https://example.test/rates?x=1",
                                   headers={"Content-Type": "application/json"})

        def get(*args, **kwargs):
            calls.append(1)
            if len(calls) == 3:
                return responder(403, b"<html>denied</html>")
            return responder(200, json_bytes({"data": {
                "errorCode": "0", "conversionRate": "1.5",
                "crdhldBillAmt": "1500.00", "fxDate": "2026-10-03"}}))

        session = Mock()
        session.get.side_effect = get
        with patch("exchange_rates.providers.discover_currencies", return_value=catalog), \
             patch("exchange_rates.http.requests.Session", return_value=session):
            client = HttpClient(retries=0, interval=0, forbidden_threshold=1)
            with self.assertRaises(AccessBlockedError) as caught:
                fetch_shard("mastercard", date(2026, 10, 3),
                            {"recovery_rounds": 0}, client, 0, 1)
        error = caught.exception
        raw = json.loads(error.raw_parts["shard-01.json"])
        self.assertEqual(len(raw["requests"]), len(calls))
        self.assertEqual([record["response"]["status_code"] for record in raw["requests"]],
                         [200, 200, 403])
        self.assertEqual(len(raw["attempts"]), len(calls))
        self.assertEqual(error.context["successful_pairs"], 2)
        self.assertEqual(error.context["attempted_pairs"], 3)
        self.assertTrue(error.context["stopped_for_access_control"])
        self.assertEqual(error.context["shard_index"], 0)

    def test_merge_shards_rejects_incomplete_or_duplicate_sets(self):
        from exchange_rates.cli import load_shard_bundles
        from exchange_rates.providers import merge_shards
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "mastercard/2026-10-03"
            directory.mkdir(parents=True)
            for index in (1, 3):
                (directory / f"shard-{index:02d}.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "Incomplete shard set"):
                load_shard_bundles(root, "mastercard", date(2026, 10, 3))
            with self.assertRaisesRegex(ValueError, "No shard directory"):
                load_shard_bundles(root, "visa", date(2026, 10, 3))
            good = {"format": "exchange-rates-shard-v1", "provider": "mastercard",
                    "requested_date": "2026-10-03", "shard_count": 2,
                    "transaction_currencies": ["USD", "CNY"],
                    "billing_currencies": ["USD", "CNY"],
                    "raw": {"requests": []}}
            usd_cny = {"transCur": "USD", "baseCur": "CNY", "rateData": "7.1"}
            cny_usd = {"transCur": "CNY", "baseCur": "USD", "rateData": "0.14"}
            # Every bundle must carry an integer shard_index.
            with self.assertRaisesRegex(ProviderError, "integer shard_index"):
                merge_shards("mastercard", date(2026, 10, 3), [good, {**good}])
            shard0 = {**good, "shard_index": 0, "rows": [usd_cny]}
            empty = {**good, "shard_index": 1, "rows": []}
            with self.assertRaisesRegex(ProviderError, "Missing pair"):
                merge_shards("mastercard", date(2026, 10, 3), [shard0, empty])
            with self.assertRaisesRegex(ProviderError, "Expected shards"):
                merge_shards("mastercard", date(2026, 10, 3), [shard0])
            duplicate = {**good, "shard_index": 1, "rows": [usd_cny]}
            with self.assertRaisesRegex(ProviderError, "duplicate pair"):
                merge_shards("mastercard", date(2026, 10, 3), [shard0, duplicate])
            identity = {**good, "shard_index": 1,
                        "rows": [cny_usd, {"transCur": "CNY", "baseCur": "CNY",
                                           "rateData": "1"}]}
            with self.assertRaisesRegex(ProviderError, "identity pair"):
                merge_shards("mastercard", date(2026, 10, 3), [shard0, identity])
            wrong_day = {**good, "shard_index": 1, "requested_date": "2026-10-02",
                         "rows": [cny_usd]}
            with self.assertRaisesRegex(ProviderError, "requested_date"):
                merge_shards("mastercard", date(2026, 10, 3), [shard0, wrong_day])
            differ = {**good, "shard_index": 1, "rows": [cny_usd],
                      "billing_currencies": ["USD", "EUR"]}
            with self.assertRaisesRegex(ProviderError, "billing catalog differs"):
                merge_shards("mastercard", date(2026, 10, 3), [shard0, differ])
            complete = {**good, "shard_index": 1, "rows": [cny_usd]}
            merged = merge_shards("mastercard", date(2026, 10, 3), [shard0, complete])
            self.assertEqual(merged.metadata["pair_count"], 2)
            self.assertEqual(sorted(merged.raw_parts), ["shard-01.json", "shard-02.json"])

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
