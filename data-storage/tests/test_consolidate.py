import importlib.util
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography.exceptions import InvalidTag

SPEC = importlib.util.spec_from_file_location(
    "consolidate", Path(__file__).parents[1] / "consolidate.py"
)
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.identity = patch.object(m, "VOLUME_UUID", "TEST-VOLUME-UUID")
        self.identity.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "target"
        self.root.mkdir()
        self.run = self.root / "migration" / "test"
        self.src = self.base / "source"
        self.src.mkdir()
        self.mount = patch.object(m, "validate_volume", return_value={})
        self.mount.start()

    def tearDown(self):
        self.mount.stop()
        self.identity.stop()
        self.tmp.cleanup()

    def inventory(self):
        m.scan(
            {
                "sources": [
                    {"path": str(self.src), "destination": "raw/test", "origin": "test"}
                ]
            },
            self.root,
            self.run,
        )

    def test_copy_deduplicates_bytes_preserves_names_and_resumes(self):
        (self.src / "one").write_bytes(b"same")
        (self.src / "two").write_bytes(b"same")
        (self.src / "different").write_bytes(b"different")
        self.inventory()
        m.copy_entries(self.run)
        one, two = self.root / "raw/test/one", self.root / "raw/test/two"
        self.assertEqual(one.stat().st_ino, two.stat().st_ino)
        self.assertNotEqual(
            one.stat().st_ino, (self.root / "raw/test/different").stat().st_ino
        )
        m.copy_entries(self.run)
        self.assertTrue(m.verify(self.run)["passed"])

    def test_conflicting_existing_target_is_not_overwritten(self):
        (self.src / "one").write_bytes(b"new")
        self.inventory()
        target = self.root / "raw/test/one"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"old")
        with self.assertRaises(RuntimeError):
            m.copy_entries(self.run)
        self.assertEqual(target.read_bytes(), b"old")

    def test_changed_source_refuses_copy(self):
        (self.src / "one").write_bytes(b"old")
        self.inventory()
        (self.src / "one").write_bytes(b"new")
        with self.assertRaises(RuntimeError):
            m.copy_entries(self.run)

    def test_target_tamper_fails_verification(self):
        (self.src / "one").write_bytes(b"old")
        self.inventory()
        m.copy_entries(self.run)
        (self.root / "raw/test/one").write_bytes(b"bad")
        self.assertFalse(m.verify(self.run)["passed"])

    def test_symlink_and_credentials_are_not_migrated(self):
        (self.src / "one").write_bytes(b"payload")
        (self.src / "link").symlink_to(self.src / "one")
        (self.src / ".env").write_text("secret")
        self.inventory()
        manifest = m.load(self.run)
        self.assertEqual(len(manifest["entries"]), 1)
        self.assertEqual(len(manifest["review"]), 2)

    def test_path_traversal_refused(self):
        for rel in ("../outside", "/outside"):
            with self.assertRaises(ValueError):
                m.safe_target(self.root, rel)
        (self.root / "escape").symlink_to(self.base)
        with self.assertRaises(ValueError):
            m.safe_target(self.root, "escape/outside")

    def test_missing_volume_fails_before_creation(self):
        self.mount.stop()
        with (
            patch.object(m, "volume_info", return_value={"Mounted": False}),
            self.assertRaises(RuntimeError),
        ):
            m.validate_volume(m.DEFAULT_ROOT)
        self.mount.start()

    def test_real_macos_volume_fields_accepted(self):
        self.mount.stop()
        info = {
            "VolumeUUID": m.VOLUME_UUID,
            "MountPoint": "/Volumes/MoErMedia",
            "WritableVolume": True,
        }
        with patch.object(m, "volume_info", return_value=info):
            self.assertEqual(m.validate_volume(m.DEFAULT_ROOT), info)
        self.mount.start()

    def test_wrong_volume_uuid_refused(self):
        self.mount.stop()
        with (
            patch.object(
                m,
                "volume_info",
                return_value={
                    "Mounted": True,
                    "MountPoint": "/Volumes/MoErMedia",
                    "VolumeUUID": "wrong",
                },
            ),
            self.assertRaises(RuntimeError),
        ):
            m.validate_volume(m.DEFAULT_ROOT)
        self.mount.start()

    def test_cleanup_requires_verified_backup(self):
        (self.src / "one").write_bytes(b"old")
        self.inventory()
        m.copy_entries(self.run)
        m.verify(self.run)
        m.save(
            self.run / "backup.json",
            {
                "restore_verified": False,
                "manifest_sha256": m.digest(self.run / "manifest.json"),
            },
        )
        with self.assertRaises(RuntimeError):
            m.cleanup(self.run)
        self.assertTrue((self.src / "one").exists())

    def test_cleanup_refuses_changed_source_before_any_deletion(self):
        for name in ("one", "two"):
            (self.src / name).write_bytes(b"old")
        self.inventory()
        m.copy_entries(self.run)
        m.verify(self.run)
        archive = self.base / "backup"
        archive.write_bytes(b"verified")
        m.save(
            self.run / "backup.json",
            {
                "archive": str(archive),
                "sha256": m.digest(archive),
                "restore_verified": True,
                "manifest_sha256": m.digest(self.run / "manifest.json"),
            },
        )
        (self.src / "two").write_bytes(b"changed")
        with self.assertRaises(RuntimeError):
            m.cleanup(self.run)
        self.assertTrue((self.src / "one").exists())

    def test_derived_catalog_tamper_refuses_cleanup(self):
        (self.src / "one").write_bytes(b"payload")
        self.inventory()
        m.copy_entries(self.run)
        catalog = self.root / "catalog.json"
        catalog.write_text("{}")
        m.save(
            self.run / "derived.json",
            [{"destination": "catalog.json", "sha256": m.digest(catalog)}],
        )
        m.verify(self.run)
        archive = self.base / "backup"
        archive.write_bytes(b"verified")
        m.save(
            self.run / "backup.json",
            {
                "archive": str(archive),
                "sha256": m.digest(archive),
                "restore_verified": True,
                "manifest_sha256": m.digest(self.run / "manifest.json"),
                "derived_sha256": m.digest(self.run / "derived.json"),
            },
        )
        catalog.write_text('{"changed":true}')
        with self.assertRaises(RuntimeError):
            m.cleanup(self.run)
        self.assertTrue((self.src / "one").exists())

    def test_cleanup_only_removes_manifest_files_and_resumes(self):
        (self.src / "one").write_bytes(b"payload")
        self.inventory()
        m.copy_entries(self.run)
        m.verify(self.run)
        archive = self.base / "backup"
        archive.write_bytes(b"verified")
        m.save(
            self.run / "backup.json",
            {
                "archive": str(archive),
                "sha256": m.digest(archive),
                "restore_verified": True,
                "manifest_sha256": m.digest(self.run / "manifest.json"),
            },
        )
        (self.src / "unmatched").write_bytes(b"keep")
        m.cleanup(self.run)
        m.cleanup(self.run)
        self.assertFalse((self.src / "one").exists())
        self.assertEqual((self.src / "unmatched").read_bytes(), b"keep")
        self.assertEqual(
            len(json.loads((self.run / "cleanup.json").read_text())["deleted"]), 1
        )

    def test_authenticated_restore_and_tamper_detection(self):
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        (self.src / "one").write_bytes(b"payload")
        self.inventory()
        m.copy_entries(self.run)
        key, nonce = os.urandom(32), os.urandom(12)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as t:
            t.add(self.src / "one", arcname="raw/test/one")
        enc = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
        enc.authenticate_additional_data(m.MAGIC)
        archive = self.base / "backup.aesgcm"
        archive.write_bytes(
            m.MAGIC + nonce + enc.update(buf.getvalue()) + enc.finalize() + enc.tag
        )
        receipt = {
            "archive": str(archive),
            "sha256": m.digest(archive),
            "keychain_service": "test",
            "restore_verified": False,
        }
        m.save(self.run / "backup.json", receipt)
        with patch.object(m.subprocess, "check_output", return_value=key.hex()):
            m.restore_test(self.run)
        self.assertTrue(
            json.loads((self.run / "backup.json").read_text())["restore_verified"]
        )
        data = bytearray(archive.read_bytes())
        data[-20] ^= 1
        archive.write_bytes(data)
        receipt["sha256"] = m.digest(archive)
        m.save(self.run / "backup.json", receipt)
        with (
            patch.object(m.subprocess, "check_output", return_value=key.hex()),
            self.assertRaises(InvalidTag),
        ):
            m.restore_test(self.run)
        self.assertFalse(
            json.loads((self.run / "backup.json").read_text())["restore_verified"]
        )


if __name__ == "__main__":
    unittest.main()
