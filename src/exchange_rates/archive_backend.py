"""Transactional exports independent of storage's artifact model."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import http.client
import json
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import BinaryIO, Protocol, Sequence

_FORMAT = "exchange-rates-release-v1"
_CHUNK = 1024 * 1024
_JSON_LIMIT = 8 * _CHUNK


class ArchiveBackendError(RuntimeError):
    """Malformed archives, integrity failures, and remote operation failures."""


class ArchiveBackend(Protocol):
    def publish(self, period: str, files: Sequence[Path]) -> dict: ...

    def materialize(self, period: str, destination: Path) -> dict[str, Path] | None: ...

    def has(self, period: str) -> bool: ...


def _period(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}(?:-(?:0[1-9]|1[0-2]))?", value):
        raise ArchiveBackendError("Invalid archive period")
    if value[:4] == "0000":
        raise ArchiveBackendError("Invalid archive year")
    return value


def _basename(value: str) -> str:
    if (not isinstance(value, str) or value in ("", ".", "..")
            or "/" in value or "\\" in value or ":" in value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ArchiveBackendError("Invalid archive filename")
    return value


def _no_symlinks(path: Path) -> None:
    path = path.absolute()
    for component in (path, *path.parents):
        if component.is_symlink():
            raise ArchiveBackendError("Symlink archive path")


def _regular(path: Path) -> None:
    _no_symlinks(path)
    if not path.is_file():
        raise ArchiveBackendError("Archive input is not a regular file")


def _directory(path: Path) -> Path:
    _no_symlinks(path)
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise ArchiveBackendError("Archive destination is not a directory")
    return path.resolve()


def _remove(path: Path, root: Path) -> None:
    # Verify the exact absolute deletion target, including intermediate links.
    _no_symlinks(path)
    resolved, boundary = path.resolve(), root.resolve()
    if resolved == boundary or not resolved.is_relative_to(boundary):
        raise ArchiveBackendError("Archive cleanup outside root")
    if path.is_dir():
        if any(child.is_symlink() for child in path.rglob("*")):
            raise ArchiveBackendError("Symlink in archive tree")
        shutil.rmtree(resolved)
    elif path.exists():
        resolved.unlink()


def _hash(path: Path) -> str:
    _regular(path)
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _json_file(path: Path) -> dict:
    _regular(path)
    try:
        if path.stat().st_size > _JSON_LIMIT:
            raise ArchiveBackendError("Archive JSON exceeds metadata limit")
        result = json.loads(path.read_bytes())
    except (ValueError, UnicodeError):
        raise ArchiveBackendError("Malformed archive JSON") from None
    if not isinstance(result, dict):
        raise ArchiveBackendError("Archive JSON must be an object")
    return result


def _sidecars(period: str) -> tuple[str, str]:
    return (("manifest.json", "SHA256SUMS") if "-" in period
            else (f"{period}.manifest.json", f"{period}.sha256"))


def _archive_names(period: str, names: set[str]) -> None:
    manifest, checksums = _sidecars(period)
    archives = names - {manifest, checksums}
    if not {manifest, checksums} <= names or not archives:
        raise ArchiveBackendError("Incomplete archive file set")
    pattern = re.escape(period) + r"\.tar\.gz(?:\.part[0-9]+)?"
    if any(not re.fullmatch(pattern, name) for name in archives):
        raise ArchiveBackendError("Unexpected archive filename")
    if f"{period}.tar.gz" in archives and len(archives) != 1:
        raise ArchiveBackendError("Mixed split and unsplit archive")
    if f"{period}.tar.gz" not in archives:
        numbers = sorted(int(name.rsplit(".part", 1)[1]) for name in archives)
        if numbers != list(range(1, len(archives) + 1)):
            raise ArchiveBackendError("Incomplete archive volumes")


def _inputs(period: str, files: Sequence[Path]) -> tuple[dict[str, Path], dict]:
    _period(period)
    paths: dict[str, Path] = {}
    for source in files:
        source = Path(source)
        name = _basename(source.name)
        if name in paths:
            raise ArchiveBackendError("Duplicate archive filename")
        _regular(source)
        paths[name] = source
    _archive_names(period, set(paths))
    manifest_name, checksum_name = _sidecars(period)
    manifest = _json_file(paths[manifest_name])
    if (manifest.get("format") != "exchange-rates-archive-v1"
            or manifest.get("period") != period or not isinstance(manifest.get("files"), list)):
        raise ArchiveBackendError("Invalid archive manifest")
    expected: dict[str, str] = {}
    try:
        checksum_path = paths[checksum_name]
        if checksum_path.stat().st_size > _JSON_LIMIT:
            raise ArchiveBackendError("Archive checksums exceed metadata limit")
        for line in checksum_path.read_text(encoding="ascii").splitlines():
            match = re.fullmatch(r"([a-f0-9]{64})  (.+)", line)
            if not match:
                raise ArchiveBackendError("Malformed archive checksum")
            digest, name = match.groups()
            _basename(name)
            if name in expected:
                raise ArchiveBackendError("Duplicate archive checksum")
            expected[name] = digest
    except UnicodeError:
        raise ArchiveBackendError("Malformed archive checksum") from None
    if set(expected) != set(paths) - {checksum_name}:
        raise ArchiveBackendError("Incomplete archive checksums")
    entries = []
    for name, path in sorted(paths.items()):
        digest = _hash(path)
        if name != checksum_name and digest != expected[name]:
            raise ArchiveBackendError("Archive checksum mismatch")
        entries.append({"filename": name, "size": path.stat().st_size, "sha256": digest})
    if "volumes" in manifest:
        volumes = manifest["volumes"]
        archive_entries = [entry for entry in entries if entry["filename"] not in {manifest_name, checksum_name}]
        if (not isinstance(volumes, list)
                or any(not isinstance(item, dict) or not isinstance(item.get("filename"), str)
                       for item in volumes)
                or sorted(volumes, key=lambda item: item["filename"])
                != sorted(archive_entries, key=lambda item: item["filename"])):
            raise ArchiveBackendError("Archive volume manifest mismatch")
    return paths, {"format": _FORMAT, "period": period, "files": entries}


def _asset_name(filename: str, digest: str) -> str:
    return f"{filename}--{digest}"


def _index(index: dict, period: str, *, remote: bool = False, empty: bool = False) -> dict:
    if (not isinstance(index, dict) or index.get("format") != _FORMAT
            or index.get("period") != period or not isinstance(index.get("files"), list)):
        raise ArchiveBackendError("Invalid archive index")
    names, ids, assets = set(), set(), set()
    for entry in index["files"]:
        if not isinstance(entry, dict):
            raise ArchiveBackendError("Invalid archive index entry")
        name = _basename(entry.get("filename"))
        if name in names:
            raise ArchiveBackendError("Duplicate archive index filename")
        names.add(name)
        if (type(entry.get("size")) is not int or entry["size"] < 0
                or not isinstance(entry.get("sha256"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", entry["sha256"])):
            raise ArchiveBackendError("Invalid archive size or digest")
        if remote:
            asset_name = _basename(entry.get("asset_name"))
            asset_id = entry.get("asset_id")
            if (type(asset_id) is not int or asset_id <= 0 or asset_id in ids
                    or asset_name in assets or asset_name != _asset_name(name, entry["sha256"])):
                raise ArchiveBackendError("Invalid archive asset mapping")
            ids.add(asset_id)
            assets.add(asset_name)
    if names or not empty:
        _archive_names(period, names)
    return index


def _copy_verified(source: BinaryIO, target: BinaryIO, entry: dict) -> None:
    digest, size = hashlib.sha256(), 0
    while chunk := source.read(_CHUNK):
        size += len(chunk)
        if size > entry["size"]:
            raise ArchiveBackendError("Archive asset size mismatch")
        digest.update(chunk)
        target.write(chunk)
    if size != entry["size"] or digest.hexdigest() != entry["sha256"]:
        raise ArchiveBackendError("Archive asset checksum mismatch")


def _install(stage: Path, destination: Path, entries: list[dict]) -> dict[str, Path]:
    destination = _directory(destination)
    for entry in entries:
        target = destination / entry["filename"]
        _no_symlinks(target)
        if target.exists() and not target.is_file():
            raise ArchiveBackendError("Nonregular materialization target")
    result = {}
    for entry in entries:
        name = entry["filename"]
        target = destination / name
        (stage / name).replace(target)
        result[name] = target
    return result


class DirectoryArchiveBackend:
    """Serialize readers/writers and replace a verified complete generation."""

    def __init__(self, root: Path):
        self.root = _directory(Path(root))

    @contextlib.contextmanager
    def _locked(self, *, exclusive: bool = False):
        _no_symlinks(self.root)
        lock = self.root / ".archive.lock"
        _no_symlinks(lock)
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "rb") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    def _visible(self, period: str) -> tuple[Path, dict] | None:
        path = self.root / _period(period)
        _no_symlinks(path)
        if not path.exists():
            return None
        if not path.is_dir():
            raise ArchiveBackendError("Invalid archive directory")
        if any(child.is_symlink() for child in path.rglob("*")):
            raise ArchiveBackendError("Symlink in archive tree")
        index = _index(_json_file(path / "index.json"), period)
        return path, index

    def publish(self, period: str, files: Sequence[Path]) -> dict:
        paths, index = _inputs(period, files)
        with self._locked(exclusive=True):
            current = self._visible(period)
            target = self.root / period
            stage = Path(tempfile.mkdtemp(prefix=".archive-stage-", dir=self.root))
            backup = None
            try:
                for entry in index["files"]:
                    with paths[entry["filename"]].open("rb") as incoming:
                        with (stage / entry["filename"]).open("xb") as outgoing:
                            _copy_verified(incoming, outgoing, entry)
                _inputs(period, [stage / name for name in paths])
                (stage / "index.json").write_text(json.dumps(index), encoding="utf-8")
                if current is not None:
                    backup = Path(tempfile.mkdtemp(prefix=".archive-old-", dir=self.root))
                    _remove(backup, self.root)
                    _no_symlinks(target)
                    if not target.resolve().is_relative_to(self.root):
                        raise ArchiveBackendError("Archive replacement outside root")
                    target.replace(backup)
                try:
                    stage.replace(target)
                except BaseException:
                    if backup is not None:
                        backup.replace(target)
                    raise
                if backup is not None:
                    try:
                        _remove(backup, self.root)
                    except (OSError, ArchiveBackendError):
                        return {**index, "cleanup_errors": [{"error": "Archive cleanup failed"}]}
                return index
            finally:
                if stage.exists():
                    _remove(stage, self.root)

    def materialize(self, period: str, destination: Path) -> dict[str, Path] | None:
        with self._locked():
            visible = self._visible(period)
            if visible is None:
                return None
            source, index = visible
            destination = _directory(Path(destination))
            stage = Path(tempfile.mkdtemp(prefix=".archive-download-", dir=destination))
            try:
                for entry in index["files"]:
                    path = source / entry["filename"]
                    _regular(path)
                    with path.open("rb") as incoming, (stage / entry["filename"]).open("xb") as outgoing:
                        _copy_verified(incoming, outgoing, entry)
                _inputs(period, [stage / entry["filename"] for entry in index["files"]])
                return _install(stage, destination, index["files"])
            finally:
                _remove(stage, destination)

    def has(self, period: str) -> bool:
        """Check index, regular files and sizes; materialize verifies content hashes."""
        with self._locked():
            visible = self._visible(period)
            if visible is None:
                return False
            source, index = visible
            for entry in index["files"]:
                path = source / entry["filename"]
                _regular(path)
                if path.stat().st_size != entry["size"]:
                    raise ArchiveBackendError("Archive asset size mismatch")
            return True


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _GitHubHTTP:
    """Replaceable transport; archive bodies are streams, never buffered."""

    def __init__(self):
        self.opener = urllib.request.build_opener(_NoRedirect())

    def request(self, method: str, url: str, headers: dict, body=None):
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            return self.opener.open(request, timeout=120)
        except urllib.error.HTTPError as response:
            return response
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            raise ArchiveBackendError("GitHub transport failed") from None


class GitHubArchiveBackend:
    def __init__(self, repository: str, token: str, target_commitish: str = "main"):
        if not isinstance(repository, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ArchiveBackendError("Invalid GitHub repository")
        if target_commitish != "main":
            raise ArchiveBackendError("Archive tags must target main")
        if not isinstance(token, str) or any(char in token for char in "\r\n"):
            raise ArchiveBackendError("Invalid GitHub token")
        self.repository = repository
        self._token = token
        self.target_commitish = target_commitish
        self._http = _GitHubHTTP()
        self._base = f"https://api.github.com/repos/{repository}"

    @classmethod
    def from_environment(cls) -> GitHubArchiveBackend:
        repository = os.environ.get("GITHUB_REPOSITORY", "")
        token = os.environ.get("GITHUB_TOKEN", "")
        if not repository or not token:
            raise ArchiveBackendError("Missing GitHub archive environment")
        return cls(repository, token)

    def _request(self, method: str, url: str, *, body=None, headers=None, missing=False, download=False):
        request_headers = {"Accept": "application/vnd.github+json", "User-Agent": "exchange-rates-archive",
                           "X-GitHub-Api-Version": "2022-11-28"}
        if self._token:
            request_headers["Authorization"] = f"Bearer {self._token}"
        request_headers.update(headers or {})
        attempts, redirects = 0, 0
        while True:
            try:
                parsed = urllib.parse.urlsplit(url)
                trusted = parsed.hostname in {"api.github.com", "uploads.github.com"}
                asset_host = (parsed.hostname == "github.com" or
                              (parsed.hostname or "").endswith(".githubusercontent.com"))
                valid = (parsed.scheme == "https" and not parsed.username and not parsed.password
                         and parsed.port in (None, 443) and (trusted or (download and asset_host)))
            except ValueError:
                valid = False
            if not valid:
                raise ArchiveBackendError("Invalid GitHub endpoint")
            if not trusted:
                request_headers = {key: value for key, value in request_headers.items()
                                   if key.lower() not in {"authorization", "cookie"}}
            response = self._http.request(method, url, request_headers, body)
            status = response.status
            if status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                response.close()
                if method != "GET" or not download or not location or redirects >= 5:
                    raise ArchiveBackendError("Unexpected GitHub redirect")
                next_url = urllib.parse.urljoin(url, location)
                if urllib.parse.urlsplit(next_url).netloc != parsed.netloc:
                    request_headers = {key: value for key, value in request_headers.items()
                                       if key.lower() not in {"authorization", "cookie"}}
                url = next_url
                redirects += 1
                continue
            if method == "GET" and status in (429, 500, 502, 503, 504) and attempts < 2:
                delay = response.headers.get("Retry-After", str(2 ** attempts))
                response.close()
                try:
                    delay = float(delay)
                except (ValueError, TypeError):
                    delay = 2 ** attempts
                if delay < 0 or delay > 30 or not delay < float("inf"):
                    raise ArchiveBackendError("GitHub retry delay exceeds limit")
                time.sleep(delay)
                attempts += 1
                continue
            if status == 404 and missing:
                response.close()
                return None
            if not 200 <= status < 300:
                response.close()
                raise ArchiveBackendError(f"GitHub {method} failed (HTTP {status})")
            return response

    def _json(self, method: str, path: str, payload=None, *, missing=False):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {} if body is None else {"Content-Type": "application/json", "Content-Length": str(len(body))}
        response = self._request(method, self._base + path, body=body, headers=headers, missing=missing)
        if response is None:
            return None
        try:
            content = response.read(_JSON_LIMIT + 1)
            if len(content) > _JSON_LIMIT:
                raise ArchiveBackendError("GitHub JSON exceeds metadata limit")
            if response.status == 204:
                return None
            result = json.loads(content)
            if not isinstance(result, (dict, list)):
                raise ArchiveBackendError("Malformed GitHub JSON structure")
            return result
        except (ValueError, UnicodeError):
            raise ArchiveBackendError("Malformed GitHub JSON") from None
        finally:
            response.close()

    def _list(self, path: str) -> list:
        values, page = [], 1
        while True:
            batch = self._json("GET", f"{path}?per_page=100&page={page}")
            if not isinstance(batch, list):
                raise ArchiveBackendError("Malformed GitHub list")
            values.extend(batch)
            if len(batch) < 100:
                return values
            page += 1

    def _release(self, period: str, *, drafts=False):
        release = self._json("GET", f"/releases/tags/rates-{_period(period)}", missing=True)
        if release is None and drafts:
            matches = [item for item in self._list("/releases")
                       if isinstance(item, dict) and item.get("tag_name") == f"rates-{period}"]
            if len(matches) > 1:
                raise ArchiveBackendError("Duplicate GitHub release tag")
            release = matches[0] if matches else None
        return release

    def _release_index(self, release, period: str) -> dict:
        if (not isinstance(release, dict) or release.get("tag_name") != f"rates-{period}"
                or release.get("target_commitish") != "main" or type(release.get("id")) is not int
                or release["id"] <= 0 or type(release.get("draft")) is not bool):
            raise ArchiveBackendError("Invalid GitHub archive release")
        try:
            body = json.loads(release.get("body", ""))
        except (TypeError, ValueError):
            raise ArchiveBackendError("Foreign GitHub release body") from None
        return _index(body, period, remote=True, empty=release["draft"])

    def _anchor(self, period: str, *, existing: bool, draft: bool = False) -> None:
        tag = self._json("GET", f"/git/ref/tags/rates-{period}", missing=True)
        if tag is None:
            if existing and not draft:
                raise ArchiveBackendError("Missing GitHub release tag")
            return
        if not existing:
            raise ArchiveBackendError("Existing tag without owned release")
        main = self._json("GET", "/git/ref/heads/main")
        try:
            tag_object, main_object = tag["object"], main["object"]
            if tag_object["type"] != "commit" or main_object["type"] != "commit":
                raise ArchiveBackendError("Archive tags must reference commits")
            tag_sha, main_sha = tag_object["sha"], main_object["sha"]
            if not all(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{40}", value)
                       for value in (tag_sha, main_sha)):
                raise ArchiveBackendError("Invalid GitHub commit reference")
        except (KeyError, TypeError):
            raise ArchiveBackendError("Malformed GitHub reference") from None
        if tag_sha != main_sha:
            comparison = self._json("GET", f"/compare/{tag_sha}...{main_sha}")
            if not isinstance(comparison, dict) or comparison.get("status") not in {"ahead", "identical"}:
                raise ArchiveBackendError("Archive tag is outside main history")

    def _download(self, entry: dict, target: Path) -> None:
        response = self._request("GET", f"{self._base}/releases/assets/{entry['asset_id']}",
                                 headers={"Accept": "application/octet-stream"}, download=True)
        try:
            with target.open("xb") as outgoing:
                _copy_verified(response, outgoing, entry)
        finally:
            response.close()

    def _verify_asset(self, asset: dict, entry: dict) -> None:
        if (not isinstance(asset, dict) or type(asset.get("id")) is not int or asset["id"] <= 0
                or asset.get("name") != entry["asset_name"] or type(asset.get("size")) is not int
                or asset["size"] != entry["size"] or asset.get("state") != "uploaded"):
            raise ArchiveBackendError("Invalid GitHub archive asset")
        entry["asset_id"] = asset["id"]
        digest = asset.get("digest")
        if digest is not None:
            if digest != f"sha256:{entry['sha256']}":
                raise ArchiveBackendError("GitHub asset digest mismatch")
        else:
            with tempfile.TemporaryDirectory(prefix="rates-verify-") as temporary:
                self._download(entry, Path(temporary) / "asset")

    def publish(self, period: str, files: Sequence[Path]) -> dict:
        paths, metadata = _inputs(period, files)
        release = self._release(period, drafts=True)
        if release is not None:
            self._release_index(release, period)
        self._anchor(period, existing=release is not None,
                     draft=release is not None and release["draft"])
        if release is None:
            release = self._json("POST", "/releases", {
                "tag_name": f"rates-{period}", "target_commitish": "main", "name": f"rates-{period}",
                "draft": True, "body": json.dumps({"format": _FORMAT, "period": period, "files": []}),
                "make_latest": "false",
            })
            self._release_index(release, period)
        release_id = release["id"]
        old_assets = self._list(f"/releases/{release_id}/assets")
        by_name, asset_ids = {}, set()
        for asset in old_assets:
            if (not isinstance(asset, dict) or not isinstance(asset.get("name"), str)
                    or asset["name"] in by_name or type(asset.get("id")) is not int
                    or asset["id"] <= 0 or asset["id"] in asset_ids):
                raise ArchiveBackendError("Invalid GitHub asset list")
            _basename(asset["name"])
            by_name[asset["name"]] = asset
            asset_ids.add(asset["id"])
        entries = []
        for source_entry in metadata["files"]:
            entry = dict(source_entry)
            entry["asset_name"] = _asset_name(entry["filename"], entry["sha256"])
            asset = by_name.get(entry["asset_name"])
            if asset is None:
                url = (f"https://uploads.github.com/repos/{self.repository}/releases/{release_id}/assets?"
                       + urllib.parse.urlencode({"name": entry["asset_name"]}))
                with paths[entry["filename"]].open("rb") as incoming:
                    response = self._request("POST", url, body=incoming, headers={
                        "Content-Type": "application/octet-stream", "Content-Length": str(entry["size"]),
                    })
                    try:
                        content = response.read(_JSON_LIMIT + 1)
                        if len(content) > _JSON_LIMIT:
                            raise ArchiveBackendError("GitHub JSON exceeds metadata limit")
                        asset = json.loads(content)
                    except (ValueError, UnicodeError):
                        raise ArchiveBackendError("Malformed GitHub upload response") from None
                    finally:
                        response.close()
            self._verify_asset(asset, entry)
            entries.append(entry)
        index = _index({"format": _FORMAT, "period": period, "files": entries}, period, remote=True)
        # Recheck the tag after uploads; a draft may not have created it yet.
        self._anchor(period, existing=True, draft=release["draft"])
        updated = self._json("PATCH", f"/releases/{release_id}", {
            "body": json.dumps(index, sort_keys=True), "draft": False, "target_commitish": "main",
        })
        if (self._release_index(updated, period) != index or updated["draft"]
                or updated["id"] != release_id):
            raise ArchiveBackendError("GitHub publication confirmation mismatch")
        retained = {entry["asset_id"] for entry in entries}
        cleanup_errors = []
        for asset in old_assets:
            if asset.get("id") not in retained:
                try:
                    self._json("DELETE", f"/releases/assets/{asset['id']}")
                except ArchiveBackendError:
                    cleanup_errors.append({"asset_id": asset.get("id"), "error": "Asset cleanup failed"})
        return {**index, "release_id": release_id, "cleanup_errors": cleanup_errors}

    def materialize(self, period: str, destination: Path) -> dict[str, Path] | None:
        release = self._release(period)
        if release is None:
            return None
        index = self._release_index(release, period)
        if release["draft"]:
            return None
        destination = _directory(Path(destination))
        stage = Path(tempfile.mkdtemp(prefix=".archive-download-", dir=destination))
        try:
            for entry in index["files"]:
                self._download(entry, stage / entry["filename"])
            _inputs(period, [stage / entry["filename"] for entry in index["files"]])
            return _install(stage, destination, index["files"])
        finally:
            _remove(stage, destination)

    def has(self, period: str) -> bool:
        """Check release metadata and main ancestry without downloading assets.

        Asset existence, sizes, and complete hashes are verified by materialize.
        """
        release = self._release(period)
        if release is None:
            return False
        self._release_index(release, period)
        if release["draft"]:
            return False
        self._anchor(period, existing=True)
        return True
