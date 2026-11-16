import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path

from cryptography.exceptions import InvalidTag

spec = importlib.util.spec_from_file_location(
    "transfer", Path(__file__).parents[1] / "transfer.py"
)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class TransferTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.root = self.base / "payload"
        self.root.mkdir()
        (self.root / ".env").write_text("PRIVATE=value\n")
        self.manifest = {
            "files": [{"path": ".env", "sha256": m.sha256(self.root / ".env")}]
        }
        (self.root / "TRANSFER_MANIFEST.json").write_text(json.dumps(self.manifest))
        self.key = os.urandom(32)
        self.archive = self.base / "bundle.aesgcm"

    def tearDown(self):
        self.temp.cleanup()

    def test_roundtrip_preserves_hidden_files(self):
        m.encrypt_tree(self.root, self.archive, self.key)
        dest = self.base / "restored"
        self.assertEqual(m.restore_tree(self.archive, self.key, dest), self.manifest)
        self.assertEqual((dest / ".env").read_text(), "PRIVATE=value\n")

    def test_tamper_or_wrong_key_never_publishes_plaintext(self):
        m.encrypt_tree(self.root, self.archive, self.key)
        dest = self.base / "restored"
        with self.assertRaises(InvalidTag):
            m.restore_tree(self.archive, os.urandom(32), dest)
        self.assertFalse(dest.exists())
        payload = bytearray(self.archive.read_bytes())
        payload[-25] ^= 1
        self.archive.write_bytes(payload)
        with self.assertRaises(InvalidTag):
            m.restore_tree(self.archive, self.key, dest)
        self.assertFalse(dest.exists())

    def test_existing_destination_and_archive_are_preserved(self):
        m.encrypt_tree(self.root, self.archive, self.key)
        with self.assertRaises(FileExistsError):
            m.encrypt_tree(self.root, self.archive, self.key)
        dest = self.base / "existing"
        dest.mkdir()
        with self.assertRaises(FileExistsError):
            m.restore_tree(self.archive, self.key, dest)

    def test_manifest_mismatch_never_publishes(self):
        (self.root / ".env").write_text("changed")
        m.encrypt_tree(self.root, self.archive, self.key)
        dest = self.base / "restored"
        with self.assertRaises(ValueError):
            m.restore_tree(self.archive, self.key, dest)
        self.assertFalse(dest.exists())

    def test_symlink_is_refused(self):
        (self.root / "link").symlink_to(self.root / ".env")
        with self.assertRaises(ValueError):
            m.encrypt_tree(self.root, self.archive, self.key)
        self.assertFalse(self.archive.exists())


if __name__ == "__main__":
    unittest.main()
