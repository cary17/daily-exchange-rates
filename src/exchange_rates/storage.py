from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .models import DayResult, json_bytes

_MEMBER = re.compile(
    r"(?P<role>history|raw/history|metadata/history)/(?P<year>\d{4})/"
    r"(?P<month>\d{2})/(?P<provider>[a-z][a-z0-9_-]*)/"
    r"(?P<day>\d{4}-\d{2}-\d{2})\.json\Z"
)
_ROLES = ("history", "raw/history", "metadata/history")


class StorageError(RuntimeError):
    pass


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
        temporary_path.replace(path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def member_info(name: str, period: str | None = None) -> tuple[str, str, str]:
    match = _MEMBER.fullmatch(name)
    if not match:
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


def _validate_files(files: dict[str, Path], period: str) -> None:
    groups: dict[tuple[str, str], set[str]] = {}
    for name, path in files.items():
        role, provider, day = member_info(name, period)
        if path.is_symlink() or not path.is_file():
            raise StorageError(f"Archive input must be a regular file: {name}")
        groups.setdefault((provider, day), set()).add(role)
    for pair, roles in groups.items():
        if roles != set(_ROLES):
            raise StorageError(f"Incomplete normalized/raw/metadata group: {pair}")
    if not files:
        raise StorageError(f"Empty archive: {period}")


def _extract_verified(artifact: Artifact, destination: Path) -> dict[str, Path]:
    expected = {
        artifact.archive.name: sha256(artifact.archive),
        artifact.manifest.name: sha256(artifact.manifest),
    }
    checksums: dict[str, str] = {}
    for line in artifact.checksums.read_text(encoding="ascii").splitlines():
        parts = line.split("  ", 1)
        if len(parts) != 2 or not re.fullmatch(r"[a-f0-9]{64}", parts[0]):
            raise StorageError(f"Invalid checksum file: {artifact.checksums}")
        digest, name = parts
        if name in checksums:
            raise StorageError(f"Duplicate checksum entry: {name}")
        checksums[name] = digest
    if expected != checksums:
        raise StorageError(f"Archive/manifest checksum mismatch: {artifact.archive}")
    manifest = json.loads(artifact.manifest.read_bytes())
    if manifest.get("format") != "exchange-rates-archive-v1" or manifest.get("period") != artifact.period:
        raise StorageError(f"Invalid manifest: {artifact.manifest}")
    entries = manifest.get("files")
    if not isinstance(entries, list):
        raise StorageError(f"Missing manifest entries: {artifact.manifest}")
    indexed = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise StorageError("Malformed manifest entry")
        name = entry["path"]
        member_info(name, artifact.period)
        if name in indexed:
            raise StorageError(f"Duplicate manifest path: {name}")
        indexed[name] = entry
    extracted = {}
    with tarfile.open(artifact.archive, "r:gz") as archive:
        for member in archive:
            member_info(member.name, artifact.period)
            if not member.isfile() or member.name not in indexed or member.name in extracted:
                raise StorageError(f"Unexpected/duplicate archive member: {member.name}")
            entry = indexed[member.name]
            if member.size != entry.get("size"):
                raise StorageError(f"Archive member size mismatch: {member.name}")
            path = destination / member.name
            path.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise StorageError(f"Missing archive member: {member.name}")
            with source, path.open("xb") as target:
                shutil.copyfileobj(source, target)
            if sha256(path) != entry.get("sha256"):
                raise StorageError(f"Archive member checksum mismatch: {member.name}")
            extracted[member.name] = path
    if set(extracted) != set(indexed):
        raise StorageError(f"Archive does not contain all manifest files: {artifact.archive}")
    _validate_files(extracted, artifact.period)
    return extracted


def _pack_verified(files: dict[str, Path], artifact: Artifact) -> None:
    _validate_files(files, artifact.period)
    with tempfile.TemporaryDirectory(prefix="rates-pack-") as temporary:
        work = Path(temporary)
        staged = Artifact(work / artifact.archive.name, work / artifact.manifest.name,
                          work / artifact.checksums.name, artifact.period)
        entries = []
        providers, dates = set(), set()
        with staged.archive.open("wb") as output:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                    for name, path in sorted(files.items()):
                        _, provider, day = member_info(name, artifact.period)
                        providers.add(provider)
                        dates.add(day)
                        size = path.stat().st_size
                        digest = sha256(path)
                        entries.append({"path": name, "size": size, "sha256": digest})
                        info = tarfile.TarInfo(name)
                        info.size = size
                        info.mode = 0o644
                        info.mtime = 0
                        with path.open("rb") as stream:
                            archive.addfile(info, stream)
        staged.manifest.write_bytes(json_bytes({
            "format": "exchange-rates-archive-v1", "period": artifact.period,
            "providers": sorted(providers), "dates": sorted(dates), "files": entries,
        }))
        staged.checksums.write_text(
            f"{sha256(staged.archive)}  {staged.archive.name}\n"
            f"{sha256(staged.manifest)}  {staged.manifest.name}\n", encoding="ascii",
        )
        _extract_verified(staged, work / "verification")
        for source, target in ((staged.archive, artifact.archive), (staged.manifest, artifact.manifest),
                               (staged.checksums, artifact.checksums)):
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix=".pending-", dir=target.parent)
            pending = Path(name)
            try:
                with source.open("rb") as incoming, os.fdopen(fd, "wb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                pending.replace(target)
            finally:
                if pending.exists():
                    pending.unlink()


class DataStore:
    def __init__(self, root: Path):
        if root.is_symlink():
            raise StorageError("Data root must not be a symlink")
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        if any(path.is_symlink() for path in self.root.rglob("*")):
            raise StorageError("Data tree must not contain symlinks")

    def month_artifact(self, year: int, month: int) -> Artifact:
        period = f"{year:04d}-{month:02d}"
        directory = self.root / "history" / f"{year:04d}" / f"{month:02d}"
        return Artifact(directory / f"{period}.tar.gz", directory / "manifest.json",
                        directory / "SHA256SUMS", period)

    def year_artifact(self, year: int) -> Artifact:
        directory = self.root / "history"
        return Artifact(directory / f"{year:04d}.tar.gz", directory / f"{year:04d}.manifest.json",
                        directory / f"{year:04d}.sha256", f"{year:04d}")

    def _paths(self, result: DayResult) -> dict[str, bytes]:
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", result.provider):
            raise StorageError(f"Invalid provider id: {result.provider}")
        day = result.requested_date
        suffix = f"{day.year:04d}/{day.month:02d}/{result.provider}/{day.isoformat()}.json"
        metadata = dict(result.metadata)
        metadata.update({"provider": result.provider, "requested_date": day.isoformat()})
        return {
            f"history/{suffix}": result.normalized,
            f"raw/history/{suffix}": result.raw,
            f"metadata/history/{suffix}": json_bytes(metadata),
        }

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

    def save_day(self, result: DayResult) -> None:
        content = self._paths(result)
        day = result.requested_date
        yearly = self.year_artifact(day.year)
        monthly = self.month_artifact(day.year, day.month)
        artifact = yearly if yearly.archive.exists() else monthly if monthly.archive.exists() else None
        if artifact is not None:
            with tempfile.TemporaryDirectory(prefix="rates-supplement-") as temporary:
                work = Path(temporary)
                files = _extract_verified(artifact, work)
                for name, body in content.items():
                    path = work / name
                    atomic_write(path, body)
                    files[name] = path
                _pack_verified(files, artifact)
        else:
            for name, body in content.items():
                atomic_write(self.root / name, body)
        latest_metadata = self.root / "metadata" / "latest" / f"{result.provider}.json"
        previous = None
        if latest_metadata.exists():
            previous = date.fromisoformat(json.loads(latest_metadata.read_bytes())["requested_date"])
        if previous is None or day >= previous:
            for role, body in content.items():
                prefix = role.split("history/", 1)[0]
                atomic_write(self.root / prefix / "latest" / f"{result.provider}.json", body)

    def _loose_files(self, year: int, month: int) -> dict[str, Path]:
        files = {}
        for role in _ROLES:
            directory = self.root / role / f"{year:04d}" / f"{month:02d}"
            if directory.exists():
                for path in directory.glob("*/*.json"):
                    name = path.relative_to(self.root).as_posix()
                    member_info(name, f"{year:04d}-{month:02d}")
                    files[name] = path
        return files

    def archive_month(self, year: int, month: int) -> bool:
        artifact = self.month_artifact(year, month)
        loose = self._loose_files(year, month)
        if not loose:
            return False
        if self.year_artifact(year).archive.exists():
            raise StorageError(f"Loose files conflict with sealed year: {year}")
        with tempfile.TemporaryDirectory(prefix="rates-month-") as temporary:
            files = _extract_verified(artifact, Path(temporary)) if artifact.archive.exists() else {}
            if set(files) & set(loose):
                raise StorageError(f"Loose files conflict with sealed month: {artifact.period}")
            files.update(loose)
            _pack_verified(files, artifact)
        for path in loose.values():
            self._remove(path)
        return True

    def archive_year(self, year: int) -> bool:
        artifact = self.year_artifact(year)
        months = [self.month_artifact(year, month) for month in range(1, 13)]
        months = [month for month in months if month.archive.exists()]
        if not months:
            return False
        with tempfile.TemporaryDirectory(prefix="rates-year-") as temporary:
            work = Path(temporary)
            files = _extract_verified(artifact, work) if artifact.archive.exists() else {}
            for month in months:
                entries = _extract_verified(month, work)
                if set(files) & set(entries):
                    raise StorageError(f"Monthly/yearly data conflict: {month.period}")
                files.update(entries)
            _pack_verified(files, artifact)
        for month in months:
            for path in (month.archive, month.manifest, month.checksums):
                self._remove(path)
        return True

    def verify_archives(self) -> int:
        artifacts = []
        history = self.root / "history"
        for path in history.glob("????.tar.gz"):
            if re.fullmatch(r"\d{4}\.tar\.gz", path.name):
                artifacts.append(self.year_artifact(int(path.name[:4])))
        for path in history.glob("????/??/????-??.tar.gz"):
            year, month = int(path.parent.parent.name), int(path.parent.name)
            expected = self.month_artifact(year, month)
            if expected.archive != path:
                raise StorageError(f"Unexpected monthly archive: {path}")
            artifacts.append(expected)
        for artifact in artifacts:
            with tempfile.TemporaryDirectory(prefix="rates-verify-") as temporary:
                _extract_verified(artifact, Path(temporary))
        return len(artifacts)

    def archive_due(self, as_of: date) -> tuple[list[str], bool]:
        # A period becomes due on the third Beijing-calendar day after it ends.
        current_month = as_of.year * 12 + as_of.month - 1
        last_due_month = current_month - (1 if as_of.day >= 3 else 2)
        periods = set()
        for role in _ROLES:
            for path in (self.root / role).glob("????/??"):
                if re.fullmatch(r"\d{4}", path.parent.name) and re.fullmatch(r"\d{2}", path.name):
                    year, month = int(path.parent.name), int(path.name)
                    if not 1 <= month <= 12:
                        raise StorageError(f"Invalid month directory: {path}")
                    if year * 12 + month - 1 <= last_due_month:
                        periods.add((year, month))
        changed = []
        for year, month in sorted(periods):
            if self.archive_month(year, month):
                changed.append(f"{year:04d}-{month:02d}")
        last_due_year = as_of.year - (1 if as_of.month > 1 or as_of.day >= 3 else 2)
        years = set()
        for path in (self.root / "history").glob("????/??/????-??.tar.gz"):
            if re.fullmatch(r"\d{4}", path.parent.parent.name):
                year = int(path.parent.parent.name)
                if year <= last_due_year:
                    years.add(year)
        for year in sorted(years):
            if self.archive_year(year):
                changed.append(f"{year:04d}")
        state_file = self.root / "maintenance.json"
        state = json.loads(state_file.read_bytes()) if state_file.exists() else {}
        archived_years = [int(path.name[:4]) for path in (self.root / "history").glob("????.tar.gz")
                          if re.fullmatch(r"\d{4}\.tar\.gz", path.name) and int(path.name[:4]) <= last_due_year]
        newest_year = max(archived_years, default=0)
        snapshot = newest_year > state.get("last_snapshot_year", 0)
        if snapshot:
            self.verify_archives()
            atomic_write(state_file, json_bytes({"last_snapshot_year": newest_year}))
        return changed, snapshot
