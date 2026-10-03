from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

from .archive_backend import ArchiveBackend, DirectoryArchiveBackend
from .models import DayResult, json_bytes

_MEMBER = re.compile(
    r"(?P<role>history|raw/history|metadata/history)/(?P<year>\d{4})/"
    r"(?P<month>\d{2})/(?P<provider>[a-z][a-z0-9_-]*)/"
    r"(?P<day>\d{4}-\d{2}-\d{2})(?:\.json|\.parts/(?P<part>shard-0[1-9]\.json))\Z"
)
_ROLES = {"history", "raw/history", "metadata/history"}
_PARTS = {f"shard-{index:02d}.json" for index in range(1, 10)}
_CHUNK = 1024 * 1024
_INDEX_LIMIT = 8 * _CHUNK


class StorageError(RuntimeError):
    pass


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def atomic_write(path: Path, content: bytes) -> None:
    if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
        raise StorageError(f"Symlink output path: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    pending = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
        pending.replace(path)
    finally:
        pending.unlink(missing_ok=True)


def member_info(name: str, period: str | None = None) -> tuple[str, str, str]:
    match = _MEMBER.fullmatch(name)
    if not match or (match["part"] and match["role"] != "raw/history"):
        raise StorageError(f"Unexpected archive path: {name}")
    try:
        day = date.fromisoformat(match["day"])
    except ValueError as exc:
        raise StorageError(f"Invalid archived date: {name}") from exc
    if f"{day.year:04d}" != match["year"] or f"{day.month:02d}" != match["month"]:
        raise StorageError(f"Archive date/path mismatch: {name}")
    if period is not None and not day.isoformat().startswith(period + "-"):
        raise StorageError(f"Archive entry outside period {period}: {name}")
    return match["role"], match["provider"], day.isoformat()


@dataclass(frozen=True)
class Artifact:
    archive: Path
    manifest: Path
    checksums: Path
    period: str

    @property
    def volumes(self) -> tuple[Path, ...]:
        def volume_order(path: Path):
            suffix = path.name.rsplit(".part", 1)[-1]
            return (0, int(suffix)) if suffix.isdecimal() else (1, suffix)
        parts = tuple(sorted(self.archive.parent.glob(self.archive.name + ".part*"), key=volume_order))
        if self.archive.exists() and parts:
            raise StorageError(f"Mixed single/volume archive: {self.archive}")
        if parts:
            expected = [self.archive.name + f".part{index:03d}" for index in range(1, len(parts) + 1)]
            if [path.name for path in parts] != expected:
                raise StorageError(f"Missing/unexpected archive volume: {self.archive}")
            return parts
        return (self.archive,) if self.archive.exists() else ()

    @property
    def files(self) -> tuple[Path, ...]:
        return (*self.volumes, self.manifest, self.checksums)

    @property
    def exists(self) -> bool:
        return bool(self.volumes)


class _VolumesReader(io.RawIOBase):
    def __init__(self, paths: tuple[Path, ...]):
        self.paths = iter(paths)
        self.current: BinaryIO | None = None

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        while True:
            if self.current is None:
                path = next(self.paths, None)
                if path is None:
                    return 0
                self.current = path.open("rb")
            count = self.current.readinto(buffer)
            if count:
                return count
            self.current.close()
            self.current = None

    def close(self) -> None:
        if self.current is not None:
            self.current.close()
        super().close()


class _VolumeWriter:
    def __init__(self, archive: Path, limit: int):
        self.archive = archive
        self.limit = limit
        self.stream_digest = hashlib.sha256()
        self.paths: list[Path] = []
        self.current: BinaryIO | None = None
        self.size = 0

    def write(self, body: bytes) -> int:
        self.stream_digest.update(body)
        remaining = memoryview(body)
        while remaining:
            if self.current is None or self.size == self.limit:
                if self.current is not None:
                    self.current.close()
                path = self.archive.with_name(self.archive.name + f".part{len(self.paths) + 1:03d}")
                self.paths.append(path)
                self.current = path.open("xb")
                self.size = 0
            count = min(len(remaining), self.limit - self.size)
            self.current.write(remaining[:count])
            self.size += count
            remaining = remaining[count:]
        return len(body)

    def finish(self) -> list[dict]:
        if self.current is not None:
            self.current.close()
            self.current = None
        if len(self.paths) == 1:
            # The target is the explicitly supplied logical archive path.
            self.paths[0].replace(self.archive)
            self.paths = [self.archive]
        return [{"filename": path.name, "size": path.stat().st_size, "sha256": sha256(path)}
                for path in self.paths]


class _HashedMember:
    def __init__(self, stream: BinaryIO, capture: bool):
        self.stream = stream
        self.digest = hashlib.sha256()
        self.size = 0
        self.capture = capture
        self.body = bytearray()
        self.overflow = False

    def read(self, size: int = -1) -> bytes:
        # tarfile.addfile requests bounded chunks. Discarding also stays bounded.
        body = self.stream.read(_CHUNK if size < 0 else min(size, _CHUNK))
        self.digest.update(body)
        self.size += len(body)
        if self.capture:
            if len(self.body) + len(body) <= _INDEX_LIMIT:
                self.body.extend(body)
            else:
                self.overflow = True
                self.body.clear()
                self.capture = False
        return body

    def drain(self) -> None:
        while self.read(_CHUNK):
            pass


def _raw_index(body: bytes | bytearray, overflow: bool = False) -> dict | None:
    if overflow:
        return None
    try:
        value = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    if isinstance(value, dict) and value.get("format") == "exchange-rates-raw-shards-v1":
        return value
    return None


def _validate_groups(entries: dict[str, dict], indices: dict[str, dict], period: str) -> None:
    groups: dict[tuple[str, str], set[str]] = {}
    shards: dict[tuple[str, str], set[str]] = {}
    for name in entries:
        role, provider, day = member_info(name, period)
        pair = provider, day
        if ".parts/" in name:
            shards.setdefault(pair, set()).add(name.rsplit("/", 1)[1])
        else:
            groups.setdefault(pair, set()).add(role)
    if not entries:
        raise StorageError(f"Empty archive: {period}")
    if set(shards) - set(groups):
        raise StorageError("Orphan raw parts")
    for pair, roles in groups.items():
        if roles != _ROLES:
            raise StorageError(f"Incomplete normalized/raw/metadata group: {pair}")
        provider, day = pair
        index_name = f"raw/history/{day[:4]}/{day[5:7]}/{provider}/{day}.json"
        index = indices.get(index_name)
        if index is None:
            if shards.get(pair):
                raise StorageError(f"Raw parts without shard index: {pair}")
            continue
        if (index.get("provider") != provider or index.get("requested_date") != day
                or index.get("parts_directory") != f"{day}.parts"):
            raise StorageError(f"Raw index identity/path mismatch: {pair}")
        parts = index.get("parts")
        if not isinstance(parts, list) or len(parts) != 9:
            raise StorageError(f"Raw index must reference nine parts: {pair}")
        names = set()
        for part in parts:
            if (not isinstance(part, dict) or not isinstance(part.get("name"), str)
                    or part["name"] not in _PARTS or part["name"] in names):
                raise StorageError(f"Invalid/duplicate raw part name: {pair}")
            names.add(part["name"])
            path = index_name[:-5] + ".parts/" + part["name"]
            entry = entries.get(path)
            if (entry is None or type(part.get("size")) is not int or part["size"] != entry["size"]
                    or part.get("sha256") != entry["sha256"]):
                raise StorageError(f"Raw part size/hash mismatch: {path}")
        if names != _PARTS or shards.get(pair, set()) != names:
            raise StorageError(f"Missing/orphan raw part: {pair}")


def _regular(path: Path) -> None:
    if path.is_symlink() or not path.is_file() or any(parent.is_symlink() for parent in path.parents):
        raise StorageError(f"Archive input must be a regular file: {path}")


def _validate_files(files: dict[str, Path], period: str) -> None:
    entries, indices = {}, {}
    for name, path in files.items():
        role, _, _ = member_info(name, period)
        _regular(path)
        entries[name] = {"size": path.stat().st_size, "sha256": sha256(path)}
        if role == "raw/history" and ".parts/" not in name and path.stat().st_size <= _INDEX_LIMIT:
            index = _raw_index(path.read_bytes())
            if index is not None:
                indices[name] = index
    _validate_groups(entries, indices, period)


def _verified_manifest(artifact: Artifact) -> dict[str, dict]:
    volumes = artifact.volumes
    if not volumes:
        raise StorageError(f"Missing archive: {artifact.archive}")
    for path in artifact.files:
        _regular(path)
    checksums = {}
    for line in artifact.checksums.read_text(encoding="ascii").splitlines():
        fields = line.split("  ", 1)
        if len(fields) != 2 or not re.fullmatch(r"[a-f0-9]{64}", fields[0]) or fields[1] in checksums:
            raise StorageError(f"Invalid checksum file: {artifact.checksums}")
        checksums[fields[1]] = fields[0]
    expected = {path.name: sha256(path) for path in (*volumes, artifact.manifest)}
    if checksums != expected:
        raise StorageError(f"Archive/manifest checksum mismatch: {artifact.archive}")
    try:
        manifest = json.loads(artifact.manifest.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise StorageError(f"Malformed manifest: {artifact.manifest}") from exc
    if (not isinstance(manifest, dict) or manifest.get("format") != "exchange-rates-archive-v1"
            or manifest.get("period") != artifact.period):
        raise StorageError(f"Invalid manifest: {artifact.manifest}")
    declared = manifest.get("volumes")
    if declared is not None:
        actual = [{"filename": path.name, "size": path.stat().st_size, "sha256": expected[path.name]}
                  for path in volumes]
        if declared != actual:
            raise StorageError(f"Archive volume manifest mismatch: {artifact.archive}")
        digest = hashlib.sha256()
        for path in volumes:
            with path.open("rb") as stream:
                while body := stream.read(_CHUNK):
                    digest.update(body)
        if digest.hexdigest() != manifest.get("stream_sha256"):
            raise StorageError(f"Archive stream checksum mismatch: {artifact.archive}")
    elif len(volumes) != 1 or volumes[0] != artifact.archive:
        raise StorageError("Legacy archives must have one tar.gz asset")
    files = manifest.get("files")
    if not isinstance(files, list):
        raise StorageError("Missing manifest entries")
    indexed = {}
    for entry in files:
        if (not isinstance(entry, dict) or not isinstance(entry.get("path"), str)
                or type(entry.get("size")) is not int or entry["size"] < 0
                or not isinstance(entry.get("sha256"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", entry["sha256"])):
            raise StorageError("Malformed manifest entry")
        member_info(entry["path"], artifact.period)
        if entry["path"] in indexed:
            raise StorageError(f"Duplicate manifest path: {entry['path']}")
        indexed[entry["path"]] = entry
    return indexed


def _archive_members(artifact: Artifact) -> Iterator[tuple[str, int, _HashedMember]]:
    indexed = _verified_manifest(artifact)
    seen, indices = {}, {}
    try:
        with io.BufferedReader(_VolumesReader(artifact.volumes), buffer_size=_CHUNK) as source:
            with tarfile.open(fileobj=source, mode="r|gz") as archive:
                for member in archive:
                    role, _, _ = member_info(member.name, artifact.period)
                    if not member.isfile() or member.name in seen or member.name not in indexed:
                        raise StorageError(f"Unexpected/duplicate archive member: {member.name}")
                    entry = indexed[member.name]
                    if member.size != entry["size"]:
                        raise StorageError(f"Archive member size mismatch: {member.name}")
                    incoming = archive.extractfile(member)
                    if incoming is None:
                        raise StorageError(f"Missing archive member: {member.name}")
                    capture = role == "raw/history" and ".parts/" not in member.name
                    stream = _HashedMember(incoming, capture)
                    yield member.name, member.size, stream
                    stream.drain()
                    incoming.close()
                    if stream.size != member.size or stream.digest.hexdigest() != entry["sha256"]:
                        raise StorageError(f"Archive member checksum mismatch: {member.name}")
                    seen[member.name] = entry
                    if capture:
                        index = _raw_index(stream.body, stream.overflow)
                        if index is not None:
                            indices[member.name] = index
    except (tarfile.TarError, EOFError, OSError) as exc:
        raise StorageError(f"Invalid archive stream: {artifact.archive}") from exc
    if seen.keys() != indexed.keys():
        raise StorageError(f"Archive does not contain all manifest files: {artifact.archive}")
    _validate_groups(seen, indices, artifact.period)


def _verify(artifact: Artifact) -> None:
    for _, _, stream in _archive_members(artifact):
        stream.drain()


def _extract_verified(artifact: Artifact, destination: Path) -> dict[str, Path]:
    # Compatibility helper; production packing and verification never extract.
    files = {}
    for name, _, stream in _archive_members(artifact):
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as output:
            shutil.copyfileobj(stream, output, length=_CHUNK)
        files[name] = path
    return files


def _pack_stream(sources: Iterable[Artifact], loose: dict[str, Path], artifact: Artifact,
                 volume_bytes: int, replace_groups: set[tuple[str, str]] | None = None) -> None:
    replacements = replace_groups or set()
    if loose:
        _validate_files(loose, artifact.period)
    artifact.archive.parent.mkdir(parents=True, exist_ok=True)
    entries, indices = {}, {}
    writer = _VolumeWriter(artifact.archive, volume_bytes)

    def add(archive, name, size, stream):
        role, _, _ = member_info(name, artifact.period)
        if name in entries:
            raise StorageError(f"Duplicate archive input: {name}")
        info = tarfile.TarInfo(name)
        info.size, info.mode, info.mtime = size, 0o644, 0
        archive.addfile(info, stream)
        entries[name] = {"path": name, "size": size, "sha256": stream.digest.hexdigest()}
        if role == "raw/history" and ".parts/" not in name:
            index = _raw_index(stream.body, stream.overflow)
            if index is not None:
                indices[name] = index

    try:
        with gzip.GzipFile(filename="", mode="wb", fileobj=writer, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
                for source in sources:
                    for name, size, stream in _archive_members(source):
                        _, provider, day = member_info(name, artifact.period)
                        if (provider, day) not in replacements:
                            add(archive, name, size, stream)
                for name, path in sorted(loose.items()):
                    with path.open("rb") as incoming:
                        capture = name.startswith("raw/history/") and ".parts/" not in name
                        stream = _HashedMember(incoming, capture)
                        add(archive, name, path.stat().st_size, stream)
        volumes = writer.finish()
    finally:
        if writer.current is not None:
            writer.current.close()
    _validate_groups(entries, indices, artifact.period)
    pairs = [member_info(name, artifact.period)[1:] for name in entries]
    artifact.manifest.write_bytes(json_bytes({
        "format": "exchange-rates-archive-v1", "period": artifact.period,
        "providers": sorted({provider for provider, _ in pairs}),
        "dates": sorted({day for _, day in pairs}), "files": [entries[name] for name in sorted(entries)],
        "volumes": volumes, "stream_sha256": writer.stream_digest.hexdigest(),
    }))
    artifact.checksums.write_text("".join(
        f"{sha256(path)}  {path.name}\n" for path in (*artifact.volumes, artifact.manifest)), encoding="ascii")
    _verify(artifact)


class DataStore:
    def __init__(self, root: Path, releases: ArchiveBackend | None = None, *,
                 retain_months: int = 3, volume_bytes: int = 1900 * 1024**2):
        if root.is_symlink():
            raise StorageError("Data root must not be a symlink")
        if type(retain_months) is not int or retain_months < 0 or type(volume_bytes) is not int or volume_bytes < 1:
            raise ValueError("Invalid retention/volume size")
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        if any(path.is_symlink() for path in self.root.rglob("*")):
            raise StorageError("Data tree must not contain symlinks")
        self.releases = releases if releases is not None else DirectoryArchiveBackend(self.root.parent / "release-output")
        self.retain_months = retain_months
        self.volume_bytes = min(volume_bytes, 1900 * 1024**2)

    def month_artifact(self, year: int, month: int) -> Artifact:
        date(year, month, 1)
        period = f"{year:04d}-{month:02d}"
        directory = self.root / "history" / f"{year:04d}" / f"{month:02d}"
        return self._artifact(directory, period)

    def year_artifact(self, year: int) -> Artifact:
        # Legacy location, used only to verify/migrate existing repositories.
        date(year, 1, 1)
        return self._artifact(self.root / "history", f"{year:04d}")

    @staticmethod
    def _artifact(directory: Path, period: str) -> Artifact:
        monthly = len(period) == 7
        return Artifact(directory / f"{period}.tar.gz",
                        directory / ("manifest.json" if monthly else f"{period}.manifest.json"),
                        directory / ("SHA256SUMS" if monthly else f"{period}.sha256"), period)

    def _state(self) -> dict:
        path = self.root / "maintenance.json"
        state = json.loads(path.read_bytes()) if path.exists() else {}
        state.setdefault("published_months", [])
        state.setdefault("published_years", [])
        return state

    def _remember(self, period: str) -> None:
        state = self._state()
        key = "published_months" if len(period) == 7 else "published_years"
        state[key] = sorted(set(state[key]) | {period})
        atomic_write(self.root / "maintenance.json", json_bytes(state))

    def _materialize(self, period: str, destination: Path) -> Artifact | None:
        destination.mkdir(parents=True, exist_ok=True)
        mapping = self.releases.materialize(period, destination)
        if mapping is None:
            return None
        artifact = self._artifact(destination, period)
        for filename, path in mapping.items():
            if (Path(filename).name != filename or filename in (".", "..")
                    or path.is_symlink() or destination.resolve() not in path.resolve().parents):
                raise StorageError(f"Unexpected backend asset: {filename}")
            _regular(path)
            target = destination / filename
            if path != target:
                with path.open("rb") as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output, length=_CHUNK)
        if set(mapping) != {path.name for path in artifact.files}:
            raise StorageError(f"Missing/unexpected backend assets: {period}")
        _verify(artifact)
        return artifact

    def _source(self, period: str, destination: Path) -> Artifact | None:
        if len(period) == 7:
            artifact = self.month_artifact(int(period[:4]), int(period[5:]))
        else:
            artifact = self.year_artifact(int(period))
        if artifact.exists:
            _verify(artifact)
            return artifact
        return self._materialize(period, destination)

    def _paths(self, result: DayResult) -> dict[str, bytes]:
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", result.provider) or type(result.requested_date) is not date:
            raise StorageError("Invalid provider/date")
        day = result.requested_date
        suffix = f"{day.year:04d}/{day.month:02d}/{result.provider}/{day.isoformat()}"
        metadata = dict(result.metadata)
        metadata.update({"provider": result.provider, "requested_date": day.isoformat()})
        raw = result.raw
        parts = getattr(result, "raw_parts", {})
        content = {f"history/{suffix}.json": result.normalized,
                   f"metadata/history/{suffix}.json": json_bytes(metadata)}
        if parts:
            if set(parts) != _PARTS:
                raise StorageError("Raw response must contain exactly nine named parts")
            index = _raw_index(raw)
            if index is None:
                raise StorageError("Raw parts require shard index")
            index["parts_directory"] = f"{day.isoformat()}.parts"
            raw = json_bytes(index)
            for name, body in parts.items():
                content[f"raw/history/{suffix}.parts/{name}"] = body
        content[f"raw/history/{suffix}.json"] = raw
        entries = {name: {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                   for name, body in content.items()}
        index = _raw_index(raw)
        _validate_groups(entries, {f"raw/history/{suffix}.json": index} if index is not None else {}, f"{day.year:04d}-{day.month:02d}")
        return content

    def _remove(self, path: Path) -> None:
        resolved = path.resolve()
        if resolved == self.root or self.root not in resolved.parents or path.is_symlink():
            raise StorageError(f"Refusing removal outside data root: {path}")
        if path.exists():
            path.unlink()
        parent = path.parent
        while parent != self.root and parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
            parent = parent.parent

    def _install(self, staged: Artifact, target: Artifact) -> None:
        old = set(target.files) if target.exists else set()
        new = set()
        for source in staged.files:
            path = target.archive.parent / source.name
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
            pending = Path(name)
            try:
                with source.open("rb") as incoming, os.fdopen(fd, "wb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing, length=_CHUNK)
                pending.replace(path)
            finally:
                pending.unlink(missing_ok=True)
            new.add(path)
        for path in old - new:
            self._remove(path)

    def _keep_month(self, period: str) -> bool:
        as_of = self._state().get("retention_as_of")
        if as_of is None:
            return self.retain_months > 0
        return self._retained(period, date.fromisoformat(as_of))

    def _retained(self, period: str, as_of: date) -> bool:
        latest = as_of.year * 12 + as_of.month - 1 - (1 if as_of.day >= 3 else 2)
        serial = int(period[:4]) * 12 + int(period[5:]) - 1
        return latest - self.retain_months < serial <= latest

    def _publish(self, artifacts: list[Artifact]) -> None:
        # Prepare and verify every output before the first remotely visible mutation.
        for artifact in artifacts:
            _verify(artifact)
        published = []
        try:
            for artifact in artifacts:
                self.releases.publish(artifact.period, artifact.files)
                published.append(artifact.period)
                self._remember(artifact.period)
        except Exception as exc:
            state = self._state()
            state["last_error"] = {"prepared": [item.period for item in artifacts],
                                   "published": published, "error": str(exc)}
            atomic_write(self.root / "maintenance.json", json_bytes(state))
            raise
        state = self._state()
        error = state.get("last_error")
        if isinstance(error, dict) and set(error.get("prepared", [])) <= set(error.get("published", []) + published):
            state.pop("last_error", None)
            atomic_write(self.root / "maintenance.json", json_bytes(state))

    def _latest(self, result: DayResult, content: dict[str, bytes]) -> None:
        metadata = self.root / "metadata/latest" / f"{result.provider}.json"
        if metadata.exists():
            previous = date.fromisoformat(json.loads(metadata.read_bytes())["requested_date"])
            if result.requested_date < previous:
                return
        for name, body in content.items():
            role, _, _ = member_info(name)
            prefix = role.split("history", 1)[0]
            if ".parts/" in name:
                path = self.root / "raw/latest" / f"{result.provider}.parts" / name.rsplit("/", 1)[1]
            else:
                path = self.root / prefix / "latest" / f"{result.provider}.json"
                if role == "raw/history" and getattr(result, "raw_parts", {}):
                    index = json.loads(body)
                    index["parts_directory"] = f"{result.provider}.parts"
                    body = json_bytes(index)
            atomic_write(path, body)
        expected = set(getattr(result, "raw_parts", {}))
        for path in (self.root / "raw/latest" / f"{result.provider}.parts").glob("*"):
            if path.name not in expected:
                self._remove(path)

    def save_day(self, result: DayResult) -> None:
        content = self._paths(result)
        day = result.requested_date
        month, year = f"{day.year:04d}-{day.month:02d}", f"{day.year:04d}"
        monthly = self.month_artifact(day.year, day.month)
        sealed_year = self.releases.has(year) or self.year_artifact(day.year).exists
        sealed_month = monthly.exists or self.releases.has(month)
        if sealed_year or sealed_month:
            with tempfile.TemporaryDirectory(prefix="rates-supplement-") as temporary:
                work = Path(temporary)
                loose = {}
                for name, body in content.items():
                    path = work / "replacement" / name
                    atomic_write(path, body)
                    loose[name] = path
                outputs = []
                for period in ([month] if sealed_month else []) + ([year] if sealed_year else []):
                    source = self._source(period, work / "sources" / period)
                    if source is None:
                        raise StorageError(f"Missing sealed archive: {period}")
                    staged = self._artifact(work / "outputs" / period, period)
                    _pack_stream([source], loose, staged, self.volume_bytes, {(result.provider, day.isoformat())})
                    outputs.append(staged)
                self._publish(outputs)
                for artifact in outputs:
                    if len(artifact.period) == 7 and self._keep_month(month):
                        self._install(artifact, monthly)
                # Existing details are retired only after every required release succeeds.
                for name in self._loose_files(day.year, day.month):
                    _, provider, loose_day = member_info(name)
                    if (provider, loose_day) == (result.provider, day.isoformat()):
                        self._remove(self.root / name)
                if monthly.exists and not self._keep_month(month):
                    for path in monthly.files:
                        self._remove(path)
                legacy_year = self.year_artifact(day.year)
                if sealed_year and legacy_year.exists:
                    for path in legacy_year.files:
                        self._remove(path)
        else:
            for name, body in content.items():
                atomic_write(self.root / name, body)
            directory = self.root / f"raw/history/{day.year:04d}/{day.month:02d}/{result.provider}/{day.isoformat()}.parts"
            for path in directory.glob("*"):
                if path.relative_to(self.root).as_posix() not in content:
                    self._remove(path)
        self._latest(result, content)

    def _loose_files(self, year: int, month: int) -> dict[str, Path]:
        files = {}
        period = f"{year:04d}-{month:02d}"
        for role in sorted(_ROLES):
            directory = self.root / role / f"{year:04d}" / f"{month:02d}"
            if directory.is_symlink():
                raise StorageError(f"Symlink month directory: {directory}")
            if directory.exists():
                for provider in directory.iterdir():
                    if provider.is_symlink():
                        raise StorageError(f"Symlink provider directory: {provider}")
                    if not provider.is_dir():
                        continue
                    for path in provider.rglob("*"):
                        if path.is_symlink():
                            raise StorageError(f"Symlink input: {path}")
                        if path.is_dir():
                            if role != "raw/history" or not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.parts", path.name):
                                raise StorageError(f"Unexpected detail directory: {path}")
                            member_info((path / "shard-01.json").relative_to(self.root).as_posix(), period)
                            if not any(path.iterdir()):
                                raise StorageError(f"Empty/orphan raw parts directory: {path}")
                            continue
                        name = path.relative_to(self.root).as_posix()
                        member_info(name, period)
                        _regular(path)
                        files[name] = path
        return files

    def archive_month(self, year: int, month: int) -> bool:
        target = self.month_artifact(year, month)
        loose = self._loose_files(year, month)
        if not loose:
            if target.exists and not self.releases.has(target.period):
                _verify(target)
                self._publish([target])
                return True
            if target.exists:
                self._remember(target.period)
            return False
        with tempfile.TemporaryDirectory(prefix="rates-month-") as temporary:
            work = Path(temporary)
            source = self._source(target.period, work / "source")
            replacements = {member_info(name)[1:] for name in loose}
            staged = self._artifact(work / "output", target.period)
            _pack_stream([source] if source else [], loose, staged, self.volume_bytes, replacements)
            outputs = [staged]
            legacy_year = self.year_artifact(year)
            if self.releases.has(f"{year:04d}") or legacy_year.exists:
                annual_source = self._source(f"{year:04d}", work / "year-source")
                if annual_source is None:
                    raise StorageError(f"Missing sealed year: {year}")
                annual = self._artifact(work / "year-output", f"{year:04d}")
                _pack_stream([annual_source], loose, annual, self.volume_bytes, replacements)
                outputs.append(annual)
            self._publish(outputs)
            if self._keep_month(target.period):
                self._install(staged, target)
            elif target.exists:
                for path in target.files:
                    self._remove(path)
            for path in loose.values():
                self._remove(path)
            if legacy_year.exists and len(outputs) == 2:
                for path in legacy_year.files:
                    self._remove(path)
        return True

    def archive_year(self, year: int) -> bool:
        period = f"{year:04d}"
        if self.releases.has(period):
            self._remember(period)
            return False
        months = {value for value in self._state()["published_months"] if value.startswith(period + "-")}
        for month in range(1, 13):
            artifact = self.month_artifact(year, month)
            if artifact.exists:
                if not self.releases.has(artifact.period):
                    _verify(artifact)
                    self._publish([artifact])
                months.add(artifact.period)
        legacy = self.year_artifact(year)
        if not months and not legacy.exists:
            return False
        with tempfile.TemporaryDirectory(prefix="rates-year-") as temporary:
            work = Path(temporary)
            staged = self._artifact(work / "output", period)
            if legacy.exists:
                _verify(legacy)
                self._publish([legacy])
            else:
                self._pack_year(sorted(months), work, staged)
                self._publish([staged])
            if legacy.exists:
                for path in legacy.files:
                    self._remove(path)
        return True

    def _pack_year(self, months: list[str], work: Path, staged: Artifact) -> None:
        # Keep one compressed source month at a time, never a year's extracted data.
        def sources():
            for period in months:
                with tempfile.TemporaryDirectory(prefix="month-", dir=work) as temporary:
                    source = self._materialize(period, Path(temporary))
                    if source is None:
                        raise StorageError(f"Missing monthly release: {period}")
                    yield source
        _pack_stream(sources(), {}, staged, self.volume_bytes)

    def _repo_months(self) -> dict[str, Artifact]:
        found = {}
        for directory in (self.root / "history").glob("????/??"):
            if re.fullmatch(r"\d{4}", directory.parent.name) and re.fullmatch(r"\d{2}", directory.name):
                artifact = self.month_artifact(int(directory.parent.name), int(directory.name))
                if artifact.exists or artifact.manifest.exists() or artifact.checksums.exists():
                    found[artifact.period] = artifact
        return found

    def verify_archives(self) -> int:
        artifacts = list(self._repo_months().values())
        years = set()
        for path in (self.root / "history").glob("????.*"):
            if re.fullmatch(r"\d{4}\.(?:tar\.gz(?:\.part\d+)?|manifest\.json|sha256)", path.name):
                years.add(int(path.name[:4]))
        artifacts.extend(self.year_artifact(year) for year in sorted(years))
        for artifact in artifacts:
            _verify(artifact)
        return len(artifacts)

    def _prune(self, as_of: date) -> None:
        for period, artifact in self._repo_months().items():
            if self._retained(period, as_of):
                continue
            _verify(artifact)
            if not self.releases.has(period):
                raise StorageError(f"Retained archive has no release: {period}")
            with tempfile.TemporaryDirectory(prefix="rates-prune-") as temporary:
                remote = self._materialize(period, Path(temporary))
                if remote is None:
                    raise StorageError(f"Missing release before pruning: {period}")
                if _verified_manifest(artifact) != _verified_manifest(remote):
                    raise StorageError(f"Repo/release conflict before pruning: {period}")
            for path in artifact.files:
                self._remove(path)

    def archive_due(self, as_of: date) -> tuple[list[str], bool]:
        error = self._state().get("last_error")
        if isinstance(error, dict) and len(error.get("prepared", [])) > 1 and error.get("published"):
            raise StorageError(f"Partial archive update needs replay of its original supplement: {error}")
        current = as_of.year * 12 + as_of.month - 1
        last_due = current - (1 if as_of.day >= 3 else 2)
        periods = set(self._repo_months())
        for role in _ROLES:
            for path in (self.root / role).glob("????/??"):
                if re.fullmatch(r"\d{4}", path.parent.name) and re.fullmatch(r"\d{2}", path.name):
                    date(int(path.parent.name), int(path.name), 1)
                    periods.add(f"{path.parent.name}-{path.name}")
        state = self._state()
        state["retention_as_of"] = as_of.isoformat()
        atomic_write(self.root / "maintenance.json", json_bytes(state))
        changed = []
        for period in sorted(periods):
            year, month = int(period[:4]), int(period[5:])
            if year * 12 + month - 1 <= last_due and self.archive_month(year, month):
                changed.append(period)
        last_year = as_of.year - (1 if as_of.month > 1 or as_of.day >= 3 else 2)
        state = self._state()
        years = {int(period[:4]) for period in state["published_months"]}
        years.update(int(path.name[:4]) for path in (self.root / "history").glob("????.manifest.json"))
        for year in sorted(years):
            if year <= last_year and self.archive_year(year):
                changed.append(f"{year:04d}")
        self._prune(as_of)
        state = self._state()
        newest = max((int(year) for year in state["published_years"] if int(year) <= last_year), default=0)
        snapshot = newest > state.get("last_snapshot_year", 0)
        if snapshot:
            self.verify_archives()
            state["last_snapshot_year"] = newest
            atomic_write(self.root / "maintenance.json", json_bytes(state))
        return changed, snapshot
