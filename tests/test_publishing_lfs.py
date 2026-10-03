import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from exchange_rates.publishing import BranchPublisher, PublishError


def run(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()


class LfsPolicyTests(unittest.TestCase):
    def test_large_file_without_lfs_fails_before_commit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            run(source, "init", "--initial-branch=main")
            publisher = BranchPublisher(source, root / "data", lfs_threshold=100)
            publisher.data_dir.mkdir()
            run(publisher.data_dir, "init", "--initial-branch=data")
            (publisher.data_dir / "day.json").write_bytes(b"x" * 101)
            with patch.object(publisher, "_enable_lfs", side_effect=PublishError("Missing git-lfs")):
                with self.assertRaises(PublishError):
                    publisher.publish("Not published")
            self.assertEqual((publisher.data_dir / "day.json").read_bytes(), b"x" * 101)
            self.assertNotEqual(subprocess.run(["git", "rev-parse", "HEAD"], cwd=publisher.data_dir,
                                              capture_output=True).returncode, 0)

    @unittest.skipUnless(shutil.which("git-lfs"), "Install git-lfs to run its local integration")
    def test_pointer_upload_and_restore_with_local_lfs_remote(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, remote = root / "source", root / "remote.git"
            source.mkdir()
            run(root, "init", "--bare", str(remote))
            run(source, "init", "--initial-branch=main")
            run(source, "config", "user.name", "Test")
            run(source, "config", "user.email", "test@example.invalid")
            (source / "source.txt").write_text("source only\n")
            run(source, "add", ".")
            run(source, "commit", "-m", "Source")
            run(source, "remote", "add", "origin", str(remote))
            run(source, "push", "origin", "main")
            first = BranchPublisher(source, root / "first", lfs_threshold=100)
            first.prepare()
            payload = bytes(range(256)) * 4
            (first.data_dir / "day.json").write_bytes(payload)
            (first.data_dir / "small.json").write_text("{}\n")
            self.assertTrue(first.publish("Large day"))
            pointer = run(remote, "show", "data:day.json")
            self.assertTrue(pointer.startswith("version https://git-lfs.github.com/spec/v1\n"))
            self.assertIn("size 1024", pointer)
            self.assertEqual(run(remote, "show", "data:small.json"), "{}")
            self.assertIn("day.json filter=lfs", run(remote, "show", "data:.gitattributes"))
            second = BranchPublisher(source, root / "second", lfs_threshold=100)
            second.prepare()
            self.assertEqual((second.data_dir / "day.json").read_bytes(), payload)
            self.assertFalse(second.publish("Unchanged"))
            self.assertEqual(run(remote, "rev-list", "--count", "main"), "1")


if __name__ == "__main__":
    unittest.main()
