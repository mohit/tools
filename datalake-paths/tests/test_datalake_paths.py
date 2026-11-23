import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import datalake_paths as dp

UUID = "00000000-0000-4000-8000-00000000AB12"


class RootVerificationTest(unittest.TestCase):
    def setUp(self):
        dp._verified.clear()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve() / "datalake.me"
        self.root.mkdir()
        self.write_identity(str(self.root), UUID)
        self.env = patch.dict(os.environ, {"DATALAKE_ROOT": str(self.root)}, clear=False)
        self.env.start()
        os.environ.pop("MOER_MEDIA_VOLUME_UUID", None)
        self.mount = patch.object(dp, "_mount_point", return_value=Path("/Volumes/Test"))
        self.mount.start()
        self.uuid = patch.object(dp, "volume_uuid", return_value=UUID)
        self.uuid_mock = self.uuid.start()

    def tearDown(self):
        patch.stopall()
        self.tmp.cleanup()
        dp._verified.clear()

    def write_identity(self, canonical, uuid):
        (self.root / dp.IDENTITY_FILE).write_text(
            json.dumps({"volume_uuid": uuid, "canonical_root": canonical})
        )

    def test_verified_root_builds_area_paths(self):
        self.assertEqual(dp.state("strava"), self.root / "state" / "strava")
        self.assertEqual(dp.databases("reflection", "r.duckdb"), self.root / "databases" / "reflection" / "r.duckdb")
        self.assertFalse((self.root / "state").exists(), "resolving a path must not create it")

    def test_missing_root_fails_without_creating(self):
        missing = self.root.parent / "absent"
        os.environ["DATALAKE_ROOT"] = str(missing)
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.raw("strava")
        self.assertFalse(missing.exists())

    def test_missing_identity_fails(self):
        (self.root / dp.IDENTITY_FILE).unlink()
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.require_root()

    def test_wrong_volume_fails(self):
        self.uuid_mock.return_value = "00000000-0000-0000-0000-000000000000"
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.require_root()

    def test_unreadable_volume_fails(self):
        self.uuid_mock.return_value = None
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.require_root()

    def test_root_on_startup_disk_fails(self):
        self.mount.stop()
        with patch.object(dp, "_mount_point", return_value=Path("/")):
            with self.assertRaises(dp.DatalakeUnavailable):
                dp.require_root()

    def test_canonical_root_mismatch_fails(self):
        self.write_identity("/Volumes/MoErMedia/datalake.me", UUID)
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.require_root()

    def test_symlinked_root_fails(self):
        link = self.root.parent / "link"
        link.symlink_to(self.root)
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.require_root(link)

    def test_within_root_rejects_outside_paths(self):
        self.assertEqual(dp.within_root(self.root / "raw" / "x"), self.root / "raw" / "x")
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.within_root(self.root.parent / "elsewhere")
        with self.assertRaises(dp.DatalakeUnavailable):
            dp.within_root(self.root / ".." / "escape")


class AtomicWriteTest(unittest.TestCase):
    def test_replacing_hard_linked_file_preserves_alias(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            active = tmp / "curated.parquet"
            alias = tmp / "versions.parquet"
            active.write_bytes(b"old")
            os.link(active, alias)
            dp.atomic_write_bytes(active, b"new")
            self.assertEqual(active.read_bytes(), b"new")
            self.assertEqual(alias.read_bytes(), b"old")

    def test_append_and_publish_preserve_alias(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            log = tmp / "a.ndjson"
            log.write_text("1\n")
            os.link(log, tmp / "alias.ndjson")
            dp.atomic_append_text(log, "2\n")
            self.assertEqual(log.read_text(), "1\n2\n")
            self.assertEqual((tmp / "alias.ndjson").read_text(), "1\n")
            src = tmp / "src"
            src.write_text("fresh")
            dp.publish_file(src, log)
            self.assertEqual(log.read_text(), "fresh")
            self.assertEqual((tmp / "alias.ndjson").read_text(), "1\n")

    def test_failed_writer_leaves_target_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "t.json"
            target.write_text("keep")
            with self.assertRaises(RuntimeError):
                with dp.atomic_output(target) as out:
                    out.write_text("partial")
                    raise RuntimeError("boom")
            self.assertEqual(target.read_text(), "keep")
            self.assertEqual(sorted(p.name for p in Path(tmp).iterdir()), ["t.json"])


class RealDiskTest(unittest.TestCase):
    @unittest.skipUnless(Path("/Volumes/MoErMedia/datalake.me/.volume-identity.json").exists(), "MoErMedia not mounted")
    def test_production_root_verifies(self):
        dp._verified.clear()
        with patch.dict(os.environ, {"DATALAKE_ROOT": ""}):
            self.assertEqual(dp.require_root(), dp.DEFAULT_ROOT)


if __name__ == "__main__":
    unittest.main()
