"""Verified storage locations for the personal datalake.

Every tool resolves its data, database, state, report and metadata paths
through this module. Paths are derived from ``DATALAKE_ROOT`` (default
``/Volumes/MoErMedia/datalake.me``) and are only handed out after the root is
proven to be the expected disk:

* the root is an existing, writable, non-symlink directory;
* ``<root>/.volume-identity.json`` exists and its ``canonical_root`` equals the
  configured root;
* the root's mount point is a separate volume whose UUID matches the identity
  file (or ``MOER_MEDIA_VOLUME_UUID`` when set).

If any check fails, :class:`DatalakeUnavailable` is raised before anything is
created, so a missing or wrong disk can never receive replacement data (for
example an empty ``/Volumes/MoErMedia`` folder on the internal disk).

Migrated files can share inodes with their historical aliases. Writers must
never modify an existing file in place; use :func:`atomic_write_bytes`,
:func:`atomic_write_text`, :func:`publish_file` or :func:`atomic_output`, which
write a sibling temporary file and atomically rename it over the target.

Standard library only, so each tool can depend on it without extra packages.
"""

from __future__ import annotations

import contextlib
import json
import os
import plistlib
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

DEFAULT_ROOT = Path("/Volumes/MoErMedia/datalake.me")
IDENTITY_FILE = ".volume-identity.json"
_AREAS = ("raw", "curated", "catalog", "databases", "artifacts", "state", "versions")

_verified: dict[Path, Path] = {}


class DatalakeUnavailable(RuntimeError):
    """The configured datalake root is missing, unverified or not writable."""


def configured_root() -> Path:
    """Return the configured root without verifying it."""
    value = os.environ.get("DATALAKE_ROOT", "").strip()
    return Path(value).expanduser() if value else DEFAULT_ROOT


def _mount_point(path: Path) -> Path:
    current = path
    while not os.path.ismount(current):
        if current.parent == current:
            break
        current = current.parent
    return current


def volume_uuid(mount: Path) -> str | None:
    """Return the APFS/HFS volume UUID for a mount point (macOS ``diskutil``)."""
    try:
        out = subprocess.run(
            ["diskutil", "info", "-plist", str(mount)],
            capture_output=True,
            check=True,
            timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    value = plistlib.loads(out).get("VolumeUUID")
    return value.upper() if isinstance(value, str) else None


def require_root(root: Path | None = None) -> Path:
    """Verify and return the datalake root, raising before any write."""
    root = Path(root).expanduser() if root is not None else configured_root()
    if not root.is_absolute():
        raise DatalakeUnavailable(f"DATALAKE_ROOT must be absolute: {root}")
    if root in _verified:
        return _verified[root]

    def fail(reason: str) -> DatalakeUnavailable:
        return DatalakeUnavailable(
            f"Datalake storage at {root} is unavailable: {reason}. "
            "Attach and mount the MoErMedia disk (or fix DATALAKE_ROOT); "
            "no data was written."
        )

    if root.is_symlink():
        raise fail("the root must not be a symlink")
    if not root.is_dir():
        raise fail("the directory does not exist")
    identity_path = root / IDENTITY_FILE
    try:
        identity = json.loads(identity_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise fail(f"{IDENTITY_FILE} is missing or unreadable ({exc.__class__.__name__})") from None
    canonical = identity.get("canonical_root")
    if not canonical or Path(canonical) != root:
        raise fail(f"{IDENTITY_FILE} names canonical_root {canonical!r}")
    expected = (os.environ.get("MOER_MEDIA_VOLUME_UUID") or identity.get("volume_uuid") or "").upper()
    if not expected:
        raise fail(f"{IDENTITY_FILE} has no volume_uuid")
    mount = _mount_point(root.resolve())
    if mount == Path("/"):
        raise fail("the root is on the startup disk, not a separate volume")
    actual = volume_uuid(mount)
    if actual != expected:
        raise fail(f"mounted volume UUID {actual} does not match expected {expected}")
    if not os.access(root, os.W_OK):
        raise fail("the volume is not writable")
    _verified[root] = root
    return root


def within_root(path: Path | str, root: Path | None = None) -> Path:
    """Return *path* resolved, requiring it to be beneath the verified root."""
    base = require_root(root)
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    resolved = candidate.resolve()
    if resolved != base.resolve() and base.resolve() not in resolved.parents:
        raise DatalakeUnavailable(
            f"Refusing to write outside the datalake: {resolved} is not beneath {base}"
        )
    return resolved


def area(name: str, *parts: str, root: Path | None = None) -> Path:
    if name not in _AREAS:
        raise ValueError(f"Unknown datalake area {name!r}")
    return require_root(root).joinpath(name, *parts)


def raw(source: str, *parts: str) -> Path:
    return area("raw", source, *parts)


def curated(source: str, *parts: str) -> Path:
    return area("curated", source, *parts)


def catalog(*parts: str) -> Path:
    return area("catalog", *parts)


def databases(tool: str, *parts: str) -> Path:
    return area("databases", tool, *parts)


def artifacts(tool: str, *parts: str) -> Path:
    return area("artifacts", tool, *parts)


def state(tool: str, *parts: str) -> Path:
    return area("state", tool, *parts)


@contextlib.contextmanager
def atomic_output(target: Path | str, suffix: str = "") -> Iterator[Path]:
    """Yield a temporary sibling path; on success rename it over *target*.

    Use for writers that need a filename (DuckDB ``COPY``, ``pq.write_table``).
    The existing inode at *target*, and any hard-linked historical alias, is
    never modified.
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=f".tmp{suffix or target.suffix}", dir=target.parent
    )
    os.close(fd)
    tmp = Path(tmp_name)
    tmp.unlink()  # let the writer create it fresh
    try:
        yield tmp
        if not tmp.exists():
            raise RuntimeError(f"Writer did not create {tmp}")
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


def atomic_write_bytes(target: Path | str, data: bytes) -> None:
    with atomic_output(target) as tmp:
        tmp.write_bytes(data)


def atomic_write_text(target: Path | str, text: str, encoding: str = "utf-8") -> None:
    atomic_write_bytes(target, text.encode(encoding))


def atomic_append_text(target: Path | str, text: str, encoding: str = "utf-8") -> None:
    """Append by publishing a new file: existing bytes plus *text*."""
    target = Path(target)
    with atomic_output(target) as tmp, tmp.open("wb") as out:
        if target.exists():
            with target.open("rb") as src:
                shutil.copyfileobj(src, out)
        out.write(text.encode(encoding))


def publish_file(src: Path | str, target: Path | str) -> None:
    """Copy *src* to *target* without modifying an existing *target* inode."""
    with atomic_output(target) as tmp:
        shutil.copy2(src, tmp)
