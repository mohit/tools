"""Keep music-history tests away from the real datalake.

Modules in this tool derive their paths from ``DATALAKE_ROOT`` at import time,
so the variable is pointed at a throwaway directory here, before pytest imports
any test module. Every test also runs with a guard that fails it if anything
tries to write, move, create or delete a path on the MoErMedia volume.
"""

from __future__ import annotations

import builtins
import io
import os
import tempfile
from pathlib import Path

import pytest

PROTECTED_ROOT = Path("/Volumes/MoErMedia")
TEST_ROOT = Path(tempfile.mkdtemp(prefix="music-history-test-lake-"))
os.environ["DATALAKE_ROOT"] = str(TEST_ROOT)

import datalake_paths as dp  # noqa: E402  (must follow the environment override)

_WRITE_MODE_CHARS = set("wax+")


def _is_protected(path: object) -> bool:
    if isinstance(path, int):  # file descriptor
        return False
    try:
        candidate = Path(os.fsdecode(path)).expanduser().absolute()
    except TypeError:
        return False
    return candidate == PROTECTED_ROOT or PROTECTED_ROOT in candidate.parents


def _refuse(action: str, path: object) -> None:
    if _is_protected(path):
        raise AssertionError(
            f"Test tried to {action} {path}, which is on the real datalake volume. "
            "Point the code under test at tmp_path instead."
        )


@pytest.fixture(autouse=True)
def _isolate_datalake(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATALAKE_ROOT", str(TEST_ROOT))

    def fake_require_root(root: Path | None = None) -> Path:
        resolved = Path(root) if root is not None else TEST_ROOT
        _refuse("verify and use", resolved)
        return resolved

    monkeypatch.setattr(dp, "require_root", fake_require_root)

    real_open = builtins.open
    real_replace = os.replace
    real_rename = os.rename
    real_unlink = os.unlink
    real_remove = os.remove
    real_mkdir = os.mkdir

    def guarded_open(file, mode="r", *args, **kwargs):
        if _WRITE_MODE_CHARS & set(mode):
            _refuse("open for writing", file)
        return real_open(file, mode, *args, **kwargs)

    def guarded_replace(src, dst, *args, **kwargs):
        _refuse("replace", dst)
        return real_replace(src, dst, *args, **kwargs)

    def guarded_rename(src, dst, *args, **kwargs):
        _refuse("rename onto", dst)
        return real_rename(src, dst, *args, **kwargs)

    def guarded_unlink(path, *args, **kwargs):
        _refuse("delete", path)
        return real_unlink(path, *args, **kwargs)

    def guarded_remove(path, *args, **kwargs):
        _refuse("delete", path)
        return real_remove(path, *args, **kwargs)

    def guarded_mkdir(path, *args, **kwargs):
        _refuse("create directory", path)
        return real_mkdir(path, *args, **kwargs)

    # Path.write_text/open/unlink/mkdir/rename/replace all route through these
    # (pathlib opens files via io.open, so both names are guarded).
    monkeypatch.setattr(builtins, "open", guarded_open)
    monkeypatch.setattr(io, "open", guarded_open)
    monkeypatch.setattr(os, "replace", guarded_replace)
    monkeypatch.setattr(os, "rename", guarded_rename)
    monkeypatch.setattr(os, "unlink", guarded_unlink)
    monkeypatch.setattr(os, "remove", guarded_remove)
    monkeypatch.setattr(os, "mkdir", guarded_mkdir)
