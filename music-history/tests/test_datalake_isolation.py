"""Regression tests: music-history tests must never touch the real datalake.

On 2026-11-15 a test wrote a fake Last.fm cursor (401) into the real
state/music-history/lastfm_last_uts.txt because ``save_last_uts`` bound its
default path at import time, so patching ``STATE_FILE`` had no effect.
"""

import importlib.util
import os
from pathlib import Path

import pytest

# The probe directory does not exist, so even a broken guard could not write here.
PROBE = Path("/Volumes/MoErMedia/__guard_probe_does_not_exist__/cursor.txt")
spec = importlib.util.spec_from_file_location(
    "lastfm_ingest_isolation", Path(__file__).resolve().parents[1] / "scripts" / "lastfm_ingest.py"
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_module_paths_point_at_temporary_root():
    assert not str(mod.STATE_FILE).startswith("/Volumes/MoErMedia"), mod.STATE_FILE
    assert Path(os.environ["DATALAKE_ROOT"]) == mod.DATALAKE_ROOT


@pytest.mark.parametrize(
    "action",
    [
        lambda: mod.save_last_uts(5, state_file=PROBE),
        lambda: PROBE.write_text("x"),
        lambda: open(PROBE, "w"),
        lambda: PROBE.parent.mkdir(),
        lambda: os.replace("/nonexistent-src", PROBE),
    ],
)
def test_guard_blocks_real_volume(action):
    with pytest.raises(AssertionError, match="real datalake volume"):
        action()


def test_patched_constant_now_redirects_default(tmp_path, monkeypatch):
    target = tmp_path / "state.txt"
    monkeypatch.setattr(mod, "STATE_FILE", target)
    mod.save_last_uts(401)
    assert target.read_text() == "401"
