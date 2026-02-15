import tempfile
import unittest
import zipfile
from pathlib import Path

import apple_music_export_helper as helper


class AppleMusicExportHelperTests(unittest.TestCase):
    def test_extract_play_activity_accepts_underscore_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            zip_path = tmp_path / "export.zip"
            output_root = tmp_path / "raw"

            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr(
                    "Apple_Music_Play_Activity.csv",
                    "Event Start Timestamp\n2024-01-01T00:00:00Z\n",
                )

            extracted = helper._extract_play_activity(zip_path, output_root)
            self.assertTrue(extracted.exists())
            self.assertEqual(extracted.name, "Apple_Music_Play_Activity.csv")


if __name__ == "__main__":
    unittest.main()
