import importlib.util
import json
import sys
import tempfile
import types
import unittest
import datetime as dt
from pathlib import Path


def load_module():
    # Tests exercise file/merge helpers only; provide a tiny requests stub for import.
    if "requests" not in sys.modules:
        stub = types.ModuleType("requests")
        def _missing(*_args, **_kwargs):
            raise RuntimeError("requests.get stub called unexpectedly")
        stub.get = _missing
        sys.modules["requests"] = stub

    module_path = Path(__file__).resolve().parents[1] / "scripts" / "lastfm_ingest.py"
    spec = importlib.util.spec_from_file_location("lastfm_ingest", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write_jsonl(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


class LastfmIngestTests(unittest.TestCase):
    def test_find_latest_uts_in_jsonl_reads_mixed_shapes(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_jsonl(
                root / "year=2024" / "month=01" / "scrobbles.jsonl",
                [
                    {"uts": 1705000000, "artist": "A", "track": "T1", "album": None},
                    {"date": {"uts": "1706000000"}, "artist": {"#text": "B"}, "name": "T2"},
                    {"bad": "row"},
                ],
            )

            write_jsonl(
                root / "year=2024" / "month=02" / "scrobbles.jsonl",
                [{"uts": 1707000000, "artist": "C", "track": "T3", "album": "X"}],
            )

            self.assertEqual(mod.find_latest_uts_in_jsonl(root), 1707000000)

    def test_merge_into_monthly_jsonl_appends_only_new_rows(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)

            existing_file = output_dir / "year=2024" / "month=03" / "scrobbles.jsonl"
            write_jsonl(
                existing_file,
                [
                    {"uts": 1710100000, "artist": "A", "track": "Song", "album": "Alpha"},
                ],
            )

            rows = [
                {"uts": 1710100000, "artist": "A", "track": "Song", "album": "Alpha"},
                {"uts": 1710200000, "artist": "A", "track": "Song 2", "album": "Alpha"},
                {"uts": 1712800000, "artist": "B", "track": "Song 3", "album": None},
            ]

            summary = mod.merge_into_monthly_jsonl(rows, output_dir)
            self.assertEqual(summary["inserted"], 2)
            self.assertEqual(summary["deduped"], 1)

            march_rows = [json.loads(line) for line in existing_file.read_text().splitlines()]
            self.assertEqual(len(march_rows), 2)

            april_file = output_dir / "year=2024" / "month=04" / "scrobbles.jsonl"
            april_rows = [json.loads(line) for line in april_file.read_text().splitlines()]
            self.assertEqual(len(april_rows), 1)

    def test_merge_rows_dedupes_and_sorts(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            jan_uts_existing = int(
                dt.datetime(2024, 1, 1, 0, 0, 20, tzinfo=dt.timezone.utc).timestamp()
            )
            jan_uts_new = int(
                dt.datetime(2024, 1, 1, 0, 0, 10, tzinfo=dt.timezone.utc).timestamp()
            )

            jan_file = output_dir / "year=2024" / "month=01" / "scrobbles.jsonl"
            write_jsonl(
                jan_file,
                [
                    {"uts": jan_uts_existing, "artist": "A", "track": "Song 2", "album": "Alpha"},
                ],
            )

            rows = [
                # Duplicate of existing row in January 2024 partition.
                {"uts": jan_uts_existing, "artist": "A", "track": "Song 2", "album": "Alpha"},
                # New row in same partition; arrives out of order and should be appended sorted.
                {"uts": jan_uts_new, "artist": "A", "track": "Song 1", "album": "Alpha"},
            ]

            summary = mod.merge_into_monthly_jsonl(rows, output_dir)
            added_rows = summary["inserted"]
            deduped_rows = summary["deduped"]
            self.assertEqual(added_rows, 1)
            self.assertEqual(deduped_rows, 1)

            jan_rows = [json.loads(line) for line in jan_file.read_text().splitlines()]
            self.assertEqual(len(jan_rows), 2)
            self.assertEqual(jan_rows[0]["uts"], jan_uts_existing)
            self.assertEqual(jan_rows[1]["uts"], jan_uts_new)


if __name__ == "__main__":
    unittest.main()
