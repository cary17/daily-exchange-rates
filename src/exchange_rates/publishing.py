from __future__ import annotations

import os
import subprocess
from pathlib import Path
from urllib.parse import urlsplit


class PublishError(RuntimeError):
    pass


def git(cwd: Path, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["git", *arguments], cwd=cwd, capture_output=True, text=True)
    if check and result.returncode:
        # Do not include command arguments: a config value may contain authentication.
        raise PublishError(f"Git operation {arguments[0]} failed (exit {result.returncode}): {result.stderr.strip()}")
    return result


class BranchPublisher:
    def __init__(self, source: Path, data_dir: Path, remote: str = "origin", branch: str = "data"):
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
            git(self.data_dir, "checkout", "--detach", "FETCH_HEAD")

    def publish(self, message: str, *, snapshot: bool = False) -> bool:
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
        with Path(destination).open("a", encoding="utf-8") as stream:
            stream.write(text + "\n")
