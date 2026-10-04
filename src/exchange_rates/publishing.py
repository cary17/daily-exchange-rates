from __future__ import annotations

import os
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

GIT_FILE_LIMIT = 100 * 1024 ** 2
LFS_FILE_LIMIT = 2_000_000_000


class PublishError(RuntimeError):
    pass


def git(cwd: Path, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["git", *arguments], cwd=cwd, capture_output=True, text=True)
    if check and result.returncode:
        # Do not include command arguments: a config value may contain authentication.
        raise PublishError(f"Git operation {arguments[0]} failed (exit {result.returncode}): {result.stderr.strip()}")
    return result


class BranchPublisher:
    def __init__(self, source: Path, data_dir: Path, remote: str = "origin", branch: str = "data", *,
                 lfs_threshold: int = GIT_FILE_LIMIT):
        if isinstance(lfs_threshold, bool) or not isinstance(lfs_threshold, int) or lfs_threshold <= 0:
            raise PublishError("LFS threshold must be a positive integer")
        self.lfs_threshold = lfs_threshold
        self._lfs_enabled = False
        self.source = source.resolve()
        self.data_dir = data_dir.resolve()
        self.remote = remote
        self.branch = branch
        self.expected_tip = ""
        git(self.source, "check-ref-format", f"refs/heads/{branch}")
        if self.data_dir == self.source or self.data_dir in self.source.parents:
            raise PublishError("Data directory must not contain the source checkout")
        if self.data_dir.exists() and any(self.data_dir.iterdir()):
            raise PublishError("Publishing needs an empty data directory; existing local files are untouched")

    def prepare(self) -> None:
        url = git(self.source, "remote", "get-url", self.remote).stdout.strip()
        if ":" not in url and not Path(url).is_absolute():
            url = str((self.source / url).resolve())
        tip = git(self.source, "ls-remote", "--exit-code", self.remote,
                  f"refs/heads/{self.branch}", check=False)
        if tip.returncode == 0:
            self.expected_tip = tip.stdout.split()[0]
        elif tip.returncode != 2:
            raise PublishError(f"Reading remote data branch failed: {tip.stderr.strip()}")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        git(self.data_dir, "init", "--initial-branch", self.branch)
        git(self.data_dir, "remote", "add", "origin", url)
        # checkout's repository-local HTTP header is not inherited by a separate data repo.
        headers = git(self.source, "config", "--local", "--get-regexp",
                      r"^http\..*\.extraheader$", check=False)
        if headers.returncode not in (0, 1):
            raise PublishError("Reading checkout authentication failed")
        host = urlsplit(url).netloc
        for line in headers.stdout.splitlines():
            key, value = line.split(" ", 1)
            if host and key.startswith(f"http.https://{host}/."):
                git(self.data_dir, "config", "--local", key, value)
        git(self.data_dir, "config", "user.name", "github-actions[bot]")
        git(self.data_dir, "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
        if self.expected_tip:
            git(self.data_dir, "fetch", "--no-tags", "--depth=1", "origin", f"refs/heads/{self.branch}")
            fetched = git(self.data_dir, "rev-parse", "FETCH_HEAD").stdout.strip()
            if fetched != self.expected_tip:
                raise PublishError("Data branch changed during initialization; rerun the task")
            attributes = git(self.data_dir, "show", "FETCH_HEAD:.gitattributes", check=False)
            if attributes.returncode == 0 and "filter=lfs" in attributes.stdout:
                self._enable_lfs()
            git(self.data_dir, "checkout", "--detach", "FETCH_HEAD")
            if self._lfs_enabled:
                git(self.data_dir, "lfs", "pull", "origin")

    def _enable_lfs(self) -> None:
        if self._lfs_enabled:
            return
        available = git(self.data_dir, "lfs", "version", check=False)
        if available.returncode:
            raise PublishError("Git LFS is required for large data files; install git-lfs")
        git(self.data_dir, "lfs", "install", "--local")
        self._lfs_enabled = True

    def _track_large_files(self) -> None:
        listing = git(self.data_dir, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
        for name in filter(None, listing.stdout.split("\0")):
            path = self.data_dir / name
            if path.is_symlink() or self.data_dir not in path.resolve().parents:
                raise PublishError(f"Invalid data path: {name}")
            if not path.is_file() or name == ".gitattributes":
                continue
            size = path.stat().st_size
            if size > LFS_FILE_LIMIT:
                raise PublishError(f"File exceeds the supported Git LFS size: {name}")
            if size > self.lfs_threshold:
                self._enable_lfs()
                git(self.data_dir, "lfs", "track", "--filename", name)

    def publish(self, message: str, *, snapshot: bool = False) -> bool:
        self._track_large_files()
        git(self.data_dir, "add", "--all")
        diff = git(self.data_dir, "diff", "--cached", "--quiet", check=False)
        if diff.returncode not in (0, 1):
            raise PublishError("Checking staged data failed")
        if diff.returncode == 0 and self.expected_tip and not snapshot:
            return False
        current = git(self.source, "ls-remote", "--exit-code", self.remote,
                      f"refs/heads/{self.branch}", check=False)
        actual_tip = current.stdout.split()[0] if current.returncode == 0 else ""
        if current.returncode not in (0, 2) or actual_tip != self.expected_tip:
            raise PublishError("Remote data branch changed; no overwrite was attempted")
        if snapshot:
            tree = git(self.data_dir, "write-tree").stdout.strip()
            commit = git(self.data_dir, "commit-tree", tree, "-m", message).stdout.strip()
            git(self.data_dir, "update-ref", "HEAD", commit)
            git(self.data_dir, "push", f"--force-with-lease=refs/heads/{self.branch}:{self.expected_tip}",
                "origin", f"HEAD:refs/heads/{self.branch}")
        else:
            git(self.data_dir, "commit", "--allow-empty", "-m", message)
            git(self.data_dir, "push", "origin", f"HEAD:refs/heads/{self.branch}")
        return True


def write_summary(text: str) -> None:
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        path = Path(destination)
        limit = 512 * 1024
        marker = b"\n[Summary truncated; full details are in diagnostic report files.]\n"
        previous = path.read_bytes() if path.exists() else b""
        combined = previous + (text + "\n").encode("utf-8")
        if len(combined) > limit:
            combined = combined[:limit - len(marker)].decode("utf-8", errors="ignore").encode("utf-8") + marker
        path.write_bytes(combined)
