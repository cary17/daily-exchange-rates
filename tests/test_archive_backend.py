import copy
import hashlib
import io
import json
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import patch

from exchange_rates.archive_backend import (
    ArchiveBackendError, DirectoryArchiveBackend, GitHubArchiveBackend,
    _GitHubHTTP, _remove,
)


def digest(content):
    return hashlib.sha256(content).hexdigest()


def fixture(root, period="2026-09", content=b"compressed-data", split=False):
    root.mkdir(parents=True, exist_ok=True)
    manifest_name = "manifest.json" if "-" in period else f"{period}.manifest.json"
    checksums_name = "SHA256SUMS" if "-" in period else f"{period}.sha256"
    if split:
        contents = {f"{period}.tar.gz.part001": content[:3], f"{period}.tar.gz.part002": content[3:]}
    else:
        contents = {f"{period}.tar.gz": content}
    manifest = {"format": "exchange-rates-archive-v1", "period": period, "files": [],
                "volumes": [{"filename": name, "size": len(value), "sha256": digest(value)}
                            for name, value in contents.items()], "stream_sha256": digest(content)}
    contents[manifest_name] = json.dumps(manifest).encode()
    contents[checksums_name] = "".join(f"{digest(value)}  {name}\n" for name, value in contents.items()).encode()
    for name, value in contents.items():
        (root / name).write_bytes(value)
    return [root / name for name in contents]


class Response(io.BytesIO):
    def __init__(self, status=200, payload=None, headers=None, binary=None):
        super().__init__(binary if binary is not None else json.dumps(payload).encode())
        self.status = status
        self.headers = headers or {}
        self.binary = binary is not None

    def read(self, size=-1):
        if self.binary:
            assert 0 < size <= 1024 * 1024, "Unbounded binary read"
        return super().read(size)


class FakeHTTP:
    def __init__(self):
        self.release = None
        self.assets = {}
        self.blobs = {}
        self.calls = []
        self.main_sha = "a" * 40
        self.tag_sha = None
        self.comparison = "ahead"
        self.next_id = 1
        self.fail_upload = False
        self.fail_patch = False
        self.fail_delete = False
        self.bad_digest = False
        self.omit_digest = False
        self.draft_before_upload = []
        self.body_before_upload = []
        self.upload_streams = []
        self.tag_status = None

    def request(self, method, url, headers, body=None):
        parsed = urllib.parse.urlsplit(url)
        path = parsed.path.removeprefix("/repos/owner/repo")
        query = urllib.parse.parse_qs(parsed.query)
        self.calls.append((method, path, dict(headers)))
        if path.startswith("/releases/tags/"):
            if self.tag_status:
                return Response(self.tag_status)
            if self.release is None or self.release["draft"]:
                return Response(404)
            return Response(payload=self.release)
        if path == "/releases" and method == "GET":
            return Response(payload=[self.release] if self.release else [])
        if path.startswith("/git/ref/tags/"):
            if self.tag_sha is None:
                return Response(404)
            return Response(payload={"object": {"type": "commit", "sha": self.tag_sha}})
        if path == "/git/ref/heads/main":
            return Response(payload={"object": {"type": "commit", "sha": self.main_sha}})
        if path.startswith("/compare/"):
            return Response(payload={"status": self.comparison})
        if path == "/releases" and method == "POST":
            payload = json.loads(body)
            assert payload["draft"] is True
            assert payload["target_commitish"] == "main"
            self.tag_sha = self.main_sha
            self.release = {**payload, "id": 17}
            return Response(201, payload=self.release)
        if path == "/releases/17/assets" and method == "GET":
            page = int(query.get("page", ["1"])[0])
            return Response(payload=list(self.assets.values())[(page - 1) * 100:page * 100])
        if path == "/releases/17/assets" and method == "POST":
            assert parsed.hostname == "uploads.github.com"
            assert hasattr(body, "read") and not isinstance(body, bytes)
            self.upload_streams.append(body)
            self.draft_before_upload.append(self.release["draft"])
            self.body_before_upload.append(self.release["body"])
            if self.fail_upload:
                return Response(502)
            pieces = []
            while value := body.read(8192):
                pieces.append(value)
            content = b"".join(pieces)
            assert int(headers["Content-Length"]) == len(content)
            name = query["name"][0]
            assert name not in {asset["name"] for asset in self.assets.values()}
            asset_id = self.next_id
            self.next_id += 1
            asset = {"id": asset_id, "name": name, "size": len(content), "state": "uploaded"}
            if not self.omit_digest:
                asset["digest"] = "sha256:" + ("0" * 64 if self.bad_digest else digest(content))
            self.assets[asset_id] = asset
            self.blobs[asset_id] = content
            return Response(201, payload=asset)
        if path == "/releases/17" and method == "PATCH":
            if self.fail_patch:
                return Response(500)
            payload = json.loads(body)
            index = json.loads(payload["body"])
            for entry in index["files"]:
                assert entry["asset_id"] in self.assets
                assert digest(self.blobs[entry["asset_id"]]) == entry["sha256"]
            self.release.update(payload)
            return Response(payload=self.release)
        if path.startswith("/releases/assets/"):
            asset_id = int(path.rsplit("/", 1)[1])
            if method == "DELETE":
                if self.fail_delete:
                    return Response(403)
                assert asset_id not in {entry["asset_id"] for entry in json.loads(self.release["body"])["files"]}
                self.assets.pop(asset_id, None)
                self.blobs.pop(asset_id, None)
                return Response(204)
            if asset_id not in self.blobs:
                return Response(404)
            return Response(binary=self.blobs[asset_id])
        raise AssertionError((method, path))


class DirectoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.backend = DirectoryArchiveBackend(self.root / "exports")
        self.files = fixture(self.root / "inputs")

    def test_exact_roundtrip_and_missing(self):
        self.assertFalse(self.backend.has("2025"))
        self.assertIsNone(self.backend.materialize("2025", self.root / "missing"))
        self.assertFalse((self.root / "missing").exists())
        index = self.backend.publish("2026-09", self.files)
        self.assertEqual(index["format"], "exchange-rates-release-v1")
        self.assertTrue(self.backend.has("2026-09"))
        paths = self.backend.materialize("2026-09", self.root / "out")
        self.assertEqual(set(paths), {path.name for path in self.files})
        for original in self.files:
            self.assertEqual(paths[original.name].read_bytes(), original.read_bytes())
            self.assertEqual(paths[original.name].parent, self.root / "out")
            self.assertTrue(paths[original.name].is_file())
        self.assertTrue((self.root / "exports/2026-09/index.json").is_file())

    def test_has_checks_metadata_without_copying_or_hashing(self):
        self.backend.publish("2026-09", self.files)
        with patch.object(self.backend, "materialize", side_effect=AssertionError("Unexpected copy")):
            with patch("exchange_rates.archive_backend._hash", side_effect=AssertionError("Unexpected hash")):
                self.assertTrue(self.backend.has("2026-09"))
        archive = self.root / "exports/2026-09/2026-09.tar.gz"
        original = archive.read_bytes()
        archive.write_bytes(b"x" * len(original))
        self.assertTrue(self.backend.has("2026-09"))
        with self.assertRaises(ArchiveBackendError):
            self.backend.materialize("2026-09", self.root / "out")

    def test_year_roundtrip(self):
        files = fixture(self.root / "year", "2025")
        self.backend.publish("2025", files)
        materialized = self.backend.materialize("2025", self.root / "out")
        self.assertEqual(set(materialized), {"2025.tar.gz", "2025.manifest.json", "2025.sha256"})

    def test_overwrite_removes_obsolete_volumes(self):
        split = fixture(self.root / "split", split=True)
        self.backend.publish("2026-09", split)
        self.backend.publish("2026-09", self.files)
        directory = self.root / "exports/2026-09"
        self.assertFalse((directory / "2026-09.tar.gz.part001").exists())
        self.assertFalse((directory / "2026-09.tar.gz.part002").exists())
        self.assertEqual((directory / "2026-09.tar.gz").read_bytes(), b"compressed-data")

    def test_incomplete_or_bad_hash_keeps_old_data(self):
        self.backend.publish("2026-09", self.files)
        before = (self.root / "exports/2026-09/index.json").read_bytes()
        with self.assertRaises(ArchiveBackendError):
            self.backend.publish("2026-09", self.files[:-1])
        self.files[0].write_bytes(b"bad")
        with self.assertRaises(ArchiveBackendError):
            self.backend.publish("2026-09", self.files)
        self.assertEqual((self.root / "exports/2026-09/index.json").read_bytes(), before)
        self.assertTrue(self.backend.has("2026-09"))

    def test_replacement_failure_rolls_back_old_generation(self):
        self.backend.publish("2026-09", self.files)
        replacement = fixture(self.root / "new", content=b"new-data")
        original_replace = Path.replace

        def fail_stage(path, target):
            if path.name.startswith(".archive-stage-"):
                raise OSError("synthetic replace failure")
            return original_replace(path, target)

        with patch.object(Path, "replace", fail_stage):
            with self.assertRaises(OSError):
                self.backend.publish("2026-09", replacement)
        self.assertEqual((self.root / "exports/2026-09/2026-09.tar.gz").read_bytes(), b"compressed-data")
        self.assertTrue(self.backend.has("2026-09"))

    def test_corruption_and_partial_existing_directory_raise(self):
        (self.root / "exports/2025").mkdir()
        with self.assertRaises(ArchiveBackendError):
            self.backend.has("2025")
        self.backend.publish("2026-09", self.files)
        (self.root / "exports/2026-09/2026-09.tar.gz").write_bytes(b"corrupt")
        with self.assertRaises(ArchiveBackendError):
            self.backend.has("2026-09")

    def test_period_duplicates_and_checksum_traversal(self):
        for period in ("../out", "2026-13", "/2026", "0000", "2026/09"):
            with self.subTest(period=period), self.assertRaises(ArchiveBackendError):
                self.backend.publish(period, self.files)
        with self.assertRaises(ArchiveBackendError):
            self.backend.publish("2026-09", self.files + self.files[:1])
        self.files[-1].write_text(f"{'0' * 64}  ../outside\n", encoding="ascii")
        with self.assertRaises(ArchiveBackendError):
            self.backend.publish("2026-09", self.files)

    def test_symlinks_rejected_for_root_input_target_and_index(self):
        link = self.root / "linked"
        link.symlink_to(self.root / "exports", target_is_directory=True)
        with self.assertRaises(ArchiveBackendError):
            DirectoryArchiveBackend(link)
        archive = self.files[0]
        original = archive.read_bytes()
        archive.unlink()
        actual = self.root / "actual"
        actual.write_bytes(original)
        archive.symlink_to(actual)
        with self.assertRaises(ArchiveBackendError):
            self.backend.publish("2026-09", self.files)
        archive.unlink()
        archive.write_bytes(original)
        self.backend.publish("2026-09", self.files)
        destination = self.root / "out"
        destination.mkdir()
        (destination / archive.name).symlink_to(actual)
        with self.assertRaises(ArchiveBackendError):
            self.backend.materialize("2026-09", destination)
        index = self.root / "exports/2026-09/index.json"
        index.unlink()
        index.symlink_to(actual)
        with self.assertRaises(ArchiveBackendError):
            self.backend.has("2026-09")

    def test_volume_metadata_order_and_old_manifest_compatibility(self):
        files = fixture(self.root / "split", split=True)
        manifest_path = self.root / "split/manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        manifest["volumes"] = [dict(reversed(list(volume.items())))
                               for volume in reversed(manifest["volumes"])]
        manifest_path.write_text(json.dumps(manifest))
        checksum_path = self.root / "split/SHA256SUMS"
        checksum_path.write_text("".join(f"{digest(path.read_bytes())}  {path.name}\n"
                                         for path in files if path != checksum_path))
        self.backend.publish("2026-09", files)
        legacy = fixture(self.root / "legacy")
        manifest_path = self.root / "legacy/manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        del manifest["volumes"]
        manifest_path.write_text(json.dumps(manifest))
        checksum_path = self.root / "legacy/SHA256SUMS"
        checksum_path.write_text("".join(f"{digest(path.read_bytes())}  {path.name}\n"
                                         for path in legacy if path != checksum_path))
        self.backend.publish("2026-09", legacy)
        self.assertTrue(self.backend.has("2026-09"))

    def test_cleanup_confirms_root_containment(self):
        outside = self.root / "outside"
        outside.mkdir()
        with self.assertRaises(ArchiveBackendError):
            _remove(outside, self.backend.root)
        self.assertTrue(outside.is_dir())
        with self.assertRaises(ArchiveBackendError):
            _remove(self.backend.root, self.backend.root)

    def test_bad_index_filename_rejected(self):
        self.backend.publish("2026-09", self.files)
        path = self.root / "exports/2026-09/index.json"
        index = json.loads(path.read_bytes())
        index["files"][0]["filename"] = "../out"
        path.write_text(json.dumps(index))
        with self.assertRaises(ArchiveBackendError):
            self.backend.materialize("2026-09", self.root / "out")


class GitHubTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.files = fixture(self.root / "inputs")
        self.backend = GitHubArchiveBackend("owner/repo", "test-token")
        self.http = FakeHTTP()
        self.backend._http = self.http

    def publish(self):
        return self.backend.publish("2026-09", self.files)

    def test_new_release_draft_stream_upload_and_original_filenames(self):
        index = self.publish()
        self.assertFalse(self.http.release["draft"])
        self.assertEqual(self.http.draft_before_upload, [True] * 3)
        self.assertEqual(len(self.http.upload_streams), 3)
        self.assertEqual(self.http.release["target_commitish"], "main")
        self.assertEqual(self.http.tag_sha, self.http.main_sha)
        for entry in index["files"]:
            self.assertEqual(entry["asset_name"], entry["filename"] + "--" + entry["sha256"])
        paths = self.backend.materialize("2026-09", self.root / "out")
        self.assertEqual({name: path.read_bytes() for name, path in paths.items()},
                         {path.name: path.read_bytes() for path in self.files})
        self.http.calls.clear()
        with patch.object(self.backend, "_download", side_effect=AssertionError("Unexpected download")):
            self.assertTrue(self.backend.has("2026-09"))
        self.assertFalse(any(path.startswith("/releases/assets/")
                             for method, path, headers in self.http.calls))

    def test_content_addressed_update_switches_body_before_cleanup(self):
        old = self.publish()
        old_body = self.http.release["body"]
        self.http.calls.clear()
        self.http.body_before_upload.clear()
        self.files = fixture(self.root / "new", content=b"changed-data")
        index = self.publish()
        self.assertTrue(all(body == old_body for body in self.http.body_before_upload))
        calls = [(method, path) for method, path, headers in self.http.calls]
        patch_index = calls.index(("PATCH", "/releases/17"))
        self.assertTrue(all(i < patch_index for i, item in enumerate(calls) if item[0] == "POST"))
        self.assertTrue(all(i > patch_index for i, item in enumerate(calls) if item[0] == "DELETE"))
        old_ids = {entry["asset_id"] for entry in old["files"]}
        new_ids = {entry["asset_id"] for entry in index["files"]}
        self.assertFalse(old_ids & new_ids)
        self.assertEqual(set(self.http.assets), new_ids)

    def test_identical_publish_reuses_assets(self):
        first = self.publish()
        self.http.calls.clear()
        second = self.publish()
        self.assertEqual(first["files"], second["files"])
        self.assertFalse(any(method in {"POST", "DELETE"} for method, path, headers in self.http.calls))

    def test_bad_upload_hash_preserves_old_body_and_assets(self):
        first = self.publish()
        body = self.http.release["body"]
        self.files = fixture(self.root / "new", content=b"changed")
        self.http.bad_digest = True
        with self.assertRaises(ArchiveBackendError):
            self.publish()
        self.assertEqual(self.http.release["body"], body)
        self.assertTrue({entry["asset_id"] for entry in first["files"]} <= set(self.http.assets))
        self.assertTrue(self.backend.has("2026-09"))

    def test_patch_failure_preserves_old_body_and_assets(self):
        first = self.publish()
        body = self.http.release["body"]
        self.files = fixture(self.root / "new", content=b"changed")
        self.http.fail_patch = True
        with self.assertRaises(ArchiveBackendError):
            self.publish()
        self.assertEqual(self.http.release["body"], body)
        self.assertTrue({entry["asset_id"] for entry in first["files"]} <= set(self.http.assets))
        self.assertTrue(self.backend.has("2026-09"))
        self.http.fail_patch = False
        self.publish()
        self.assertEqual(len(self.http.assets), 3)

    def test_cleanup_failure_reports_success_and_retains_old_assets(self):
        self.publish()
        self.files = fixture(self.root / "new", content=b"changed")
        self.http.fail_delete = True
        index = self.publish()
        self.assertEqual(len(index["cleanup_errors"]), 3)
        self.assertEqual(len(self.http.assets), 6)
        self.assertTrue(self.backend.has("2026-09"))

    def test_failed_new_release_stays_invisible_and_resumes_draft(self):
        self.http.fail_upload = True
        with self.assertRaises(ArchiveBackendError):
            self.publish()
        self.assertTrue(self.http.release["draft"])
        self.assertFalse(self.backend.has("2026-09"))
        self.assertEqual(sum(method == "POST" and path == "/releases/17/assets"
                             for method, path, headers in self.http.calls), 1)
        self.http.fail_upload = False
        self.publish()
        self.assertFalse(self.http.release["draft"])

    def test_missing_digest_downloads_and_hashes_uploaded_assets(self):
        self.http.omit_digest = True
        self.publish()
        self.assertEqual(sum(method == "GET" and path.startswith("/releases/assets/")
                             for method, path, headers in self.http.calls), 3)

    def test_404_only_means_missing_release_not_missing_asset(self):
        self.assertFalse(self.backend.has("2026-09"))
        self.assertIsNone(self.backend.materialize("2026-09", self.root / "absent"))
        self.assertFalse((self.root / "absent").exists())
        index = self.publish()
        del self.http.blobs[index["files"][0]["asset_id"]]
        with self.assertRaisesRegex(ArchiveBackendError, "HTTP 404"):
            self.backend.materialize("2026-09", self.root / "out")

    def test_binary_corruption_leaves_destination_unchanged(self):
        index = self.publish()
        destination = self.root / "out"
        before = self.backend.materialize("2026-09", destination)
        before_bytes = {name: path.read_bytes() for name, path in before.items()}
        self.http.blobs[index["files"][-1]["asset_id"]] = b"corrupted"
        with self.assertRaises(ArchiveBackendError):
            self.backend.materialize("2026-09", destination)
        self.assertEqual({name: path.read_bytes() for name, path in before.items()}, before_bytes)
        self.assertFalse(any(path.name.startswith(".archive-download") for path in destination.iterdir()))

    def test_foreign_body_period_and_path_mapping_raise(self):
        self.publish()
        original = copy.deepcopy(self.http.release)
        bodies = ["foreign body", json.dumps({"format": "other", "period": "2026-09", "files": []})]
        index = json.loads(original["body"])
        index["period"] = "2025"
        bodies.append(json.dumps(index))
        index = json.loads(original["body"])
        index["files"][0]["filename"] = "../outside"
        bodies.append(json.dumps(index))
        for body in bodies:
            with self.subTest(body=body):
                self.http.release = {**original, "body": body}
                with self.assertRaises(ArchiveBackendError):
                    self.publish()
                with self.assertRaises(ArchiveBackendError):
                    self.backend.has("2026-09")

    def test_main_anchor_rejects_data_history_and_unknown_existing_tag(self):
        with self.assertRaises(ArchiveBackendError):
            GitHubArchiveBackend("owner/repo", "test", "data")
        self.http.tag_sha = "b" * 40
        with self.assertRaises(ArchiveBackendError):
            self.publish()
        self.assertIsNone(self.http.release)
        self.http.tag_sha = None
        self.publish()
        self.http.tag_sha = "b" * 40
        self.http.comparison = "diverged"
        with self.assertRaises(ArchiveBackendError):
            self.publish()
        self.http.comparison = "ahead"
        self.publish()

    def test_non404_errors_and_get_retry_limit(self):
        self.http.tag_status = 403
        with self.assertRaisesRegex(ArchiveBackendError, "HTTP 403"):
            self.backend.has("2026-09")
        self.http.tag_status = 503
        with patch("exchange_rates.archive_backend.time.sleep") as sleep:
            with self.assertRaisesRegex(ArchiveBackendError, "HTTP 503"):
                self.backend.has("2026-09")
        self.assertEqual(sleep.call_count, 2)

    def test_redirect_strips_auth_and_cookie_and_blocks_non_github_host(self):
        calls = []

        class RedirectHTTP:
            target = "https://release-assets.githubusercontent.com/signed?secret=opaque"

            def request(inner, method, url, headers, body=None):
                calls.append((url, dict(headers)))
                if len(calls) == 1:
                    return Response(302, headers={"Location": inner.target})
                return Response(binary=b"ok")

        redirect = RedirectHTTP()
        self.backend._http = redirect
        response = self.backend._request("GET", self.backend._base + "/releases/assets/1",
                                         download=True, headers={"Cookie": "credential"})
        response.close()
        self.assertIn("Authorization", calls[0][1])
        self.assertIn("Cookie", calls[0][1])
        self.assertNotIn("Authorization", calls[1][1])
        self.assertNotIn("Cookie", calls[1][1])
        calls.clear()
        redirect.target = "https://attacker.example/signed?secret=opaque"
        with self.assertRaises(ArchiveBackendError) as caught:
            self.backend._request("GET", self.backend._base + "/releases/assets/1", download=True)
        self.assertNotIn("opaque", str(caught.exception))
        self.assertNotIn("test-token", str(caught.exception))
        self.assertEqual(len(calls), 1)

    def test_http_transport_passes_stream_to_urllib_with_content_length(self):
        http = _GitHubHTTP()
        stream = io.BytesIO(b"archive")
        with patch.object(http.opener, "open", return_value=Response(201, payload={})) as opened:
            response = http.request("POST", "https://uploads.github.com/upload",
                                    {"Content-Length": "7"}, stream)
        response.close()
        request = opened.call_args.args[0]
        self.assertIs(request.data, stream)
        self.assertEqual(request.get_header("Content-length"), "7")

    def test_json_null_is_format_failure_not_missing(self):
        class NullHTTP:
            def request(inner, method, url, headers, body=None):
                return Response(payload=None)

        self.backend._http = NullHTTP()
        with self.assertRaisesRegex(ArchiveBackendError, "JSON structure"):
            self.backend.has("2026-09")

    def test_draft_without_tag_can_resume(self):
        self.http.fail_upload = True
        with self.assertRaises(ArchiveBackendError):
            self.publish()
        self.http.tag_sha = None
        self.http.fail_upload = False
        index = self.publish()
        self.assertEqual(len(index["files"]), 3)
        self.assertFalse(self.http.release["draft"])

    def test_tag_changed_during_upload_blocks_publication(self):
        original_request = self.http.request

        def change_tag(method, url, headers, body=None):
            response = original_request(method, url, headers, body)
            if method == "POST" and urllib.parse.urlsplit(url).hostname == "uploads.github.com":
                self.http.tag_sha = "b" * 40
                self.http.comparison = "diverged"
            return response

        self.http.request = change_tag
        with self.assertRaisesRegex(ArchiveBackendError, "outside main"):
            self.publish()
        self.assertTrue(self.http.release["draft"])
        self.assertFalse(any(method == "PATCH" for method, path, headers in self.http.calls))

    def test_upload_without_digest_detects_download_corruption(self):
        self.http.omit_digest = True
        original_request = self.http.request

        def corrupt(method, url, headers, body=None):
            response = original_request(method, url, headers, body)
            if method == "GET" and "/releases/assets/" in url:
                response.close()
                return Response(binary=b"bad-data")
            return response

        self.http.request = corrupt
        with self.assertRaises(ArchiveBackendError):
            self.publish()
        self.assertTrue(self.http.release["draft"])

    def test_large_upload_stays_file_stream(self):
        self.files = fixture(self.root / "large", content=b"x" * (3 * 1024 * 1024 + 37))
        self.publish()
        self.assertTrue(all(hasattr(stream, "fileno") for stream in self.http.upload_streams))
        materialized = self.backend.materialize("2026-09", self.root / "out")
        self.assertEqual(materialized["2026-09.tar.gz"].stat().st_size, 3 * 1024 * 1024 + 37)

    def test_retry_after_is_bounded_and_post_never_retried(self):
        calls = []

        class RateHTTP:
            def request(inner, method, url, headers, body=None):
                calls.append(method)
                return Response(429, headers={"Retry-After": "3600"})

        self.backend._http = RateHTTP()
        with patch("exchange_rates.archive_backend.time.sleep") as sleep:
            with self.assertRaisesRegex(ArchiveBackendError, "delay exceeds"):
                self.backend.has("2026-09")
            self.assertEqual(calls, ["GET"])
            sleep.assert_not_called()
            with self.assertRaisesRegex(ArchiveBackendError, "HTTP 429"):
                self.backend._json("POST", "/releases", {})
            self.assertEqual(calls, ["GET", "POST"])
            sleep.assert_not_called()

    def test_environment_factory(self):
        with patch.dict("os.environ", {"GITHUB_REPOSITORY": "owner/repo", "GITHUB_TOKEN": "test"}, clear=True):
            self.assertEqual(GitHubArchiveBackend.from_environment().repository, "owner/repo")
        with patch.dict("os.environ", {}, clear=True), self.assertRaises(ArchiveBackendError):
            GitHubArchiveBackend.from_environment()


if __name__ == "__main__":
    unittest.main()
