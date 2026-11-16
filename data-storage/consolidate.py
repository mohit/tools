#!/usr/bin/env python3
"""Manifest-driven local consolidation. No API calls or row-level transformations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import secrets
import shutil
import stat
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

CHUNK = 4 * 1024 * 1024
DEFAULT_ROOT = Path("/Volumes/MoErMedia/datalake.me")
VOLUME_UUID = os.environ.get("MOER_MEDIA_VOLUME_UUID")
MAGIC = b"MOERBACKUP1\n"
SKIP = {".DS_Store", "__pycache__", ".git", ".venv", "node_modules"}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def save(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def volume_info(path):
    return plistlib.loads(
        subprocess.check_output(["diskutil", "info", "-plist", str(path)])
    )


def validate_volume(root):
    root = Path(root)
    mount = Path("/Volumes/MoErMedia")
    if root != DEFAULT_ROOT:
        raise ValueError(
            "Production destination must be /Volumes/MoErMedia/datalake.me"
        )
    info = volume_info(mount)
    identity = root / ".volume-identity.json"
    expected = VOLUME_UUID or (
        json.loads(identity.read_text())["volume_uuid"] if identity.is_file() else None
    )
    if (
        not expected
        or info.get("VolumeUUID", "").upper() != expected.upper()
        or info.get("MountPoint") != str(mount)
        or not info.get("WritableVolume", False)
    ):
        raise RuntimeError("Expected writable MoErMedia volume is not mounted")
    if root.is_symlink():
        raise RuntimeError("Destination root must not be a symlink")
    return info


def safe_target(root, relative):
    relative = Path(relative)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Unsafe destination")
    target = Path(root) / relative
    if not target.resolve().is_relative_to(Path(root).resolve()):
        raise ValueError("Destination escapes root")
    return target


def stable_hash(path):
    before = path.stat()
    result = digest(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (
        after.st_size,
        after.st_mtime_ns,
        after.st_ino,
    ):
        raise RuntimeError(f"Source changed while hashing: {path}")
    return before, result


def scan(spec, root, run):
    identity = validate_volume(root)
    if (run / "manifest.json").exists():
        raise FileExistsError("Manifest already exists; resume copy/verify instead")
    entries, review, missing = [], [], []
    seen = set()
    for source in spec["sources"]:
        base = Path(source["path"])
        if not base.exists():
            missing.append(str(base))
            continue
        paths = sorted(base.rglob("*")) if base.is_dir() else [base]
        for p in paths:
            rel = p.relative_to(base) if base.is_dir() else Path(p.name)
            if any(part in SKIP for part in rel.parts) or p.name in SKIP:
                continue
            if p.is_symlink():
                review.append(
                    {"source": str(p), "reason": "symlink; target not followed"}
                )
                continue
            if not p.is_file() or str(p) in seen:
                continue
            seen.add(str(p))
            if p.name.startswith(".env") or any(
                t in p.name.lower()
                for t in ("1password", "credentials", "client_secret", "oauth_token")
            ):
                review.append(
                    {
                        "source": str(p),
                        "reason": "possible credential material; left in place",
                    }
                )
                continue
            if source.get("single"):
                destination = source["destination"]
            else:
                destination = str(Path(source["destination"]) / rel)
            safe_target(root, destination)
            try:
                s, sha = stable_hash(p)
                entry = {
                    "source": str(p),
                    "destination": destination,
                    "size": s.st_size,
                    "mtime_ns": s.st_mtime_ns,
                    "sha256": sha,
                    "cleanup": source.get("cleanup", True),
                    "origin": source["origin"],
                    "status": "inventoried",
                }
                entries.append(entry)
            except OSError as e:
                review.append(
                    {"source": str(p), "reason": f"unreadable: {type(e).__name__}"}
                )
        print(f"Inventoried {source['origin']}: {len(entries)} files", flush=True)
    destinations = {}
    for e in entries:
        old = destinations.get(e["destination"])
        if old and old != e["sha256"]:
            raise RuntimeError(f"Conflicting destination: {e['destination']}")
        destinations[e["destination"]] = e["sha256"]
    manifest = {
        "format": 1,
        "run_id": run.name,
        "root": str(root),
        "volume_uuid": identity.get("VolumeUUID", VOLUME_UUID),
        "created_utc": datetime.now(UTC).isoformat(),
        "spec": spec,
        "entries": entries,
        "review": review,
        "missing_optional_paths": missing,
    }
    save(run / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "files": len(entries),
                "bytes": sum(e["size"] for e in entries),
                "unique_bytes": sum({e["sha256"]: e["size"] for e in entries}.values()),
                "review": len(review),
            }
        ),
        flush=True,
    )


def load(run):
    return json.loads((Path(run) / "manifest.json").read_text())


def unchanged(entry):
    path = Path(entry["source"])
    if path.is_symlink():
        return False
    try:
        s, sha = stable_hash(path)
    except OSError:
        return False
    return (
        s.st_size == entry["size"]
        and s.st_mtime_ns == entry["mtime_ns"]
        and sha == entry["sha256"]
    )


def copy_entries(run):
    m = load(run)
    root = Path(m["root"])
    validate_volume(root)
    canonical, copied, linked = {}, 0, 0
    for i, e in enumerate(m["entries"]):
        target = safe_target(root, e["destination"])
        if target.exists():
            if not target.is_file() or digest(target) != e["sha256"]:
                raise RuntimeError(f"Existing destination differs: {target}")
        else:
            if not unchanged(e):
                raise RuntimeError(f"Source changed: {e['source']}")
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name + ".migration-part")
            if temporary.exists():
                temporary.unlink()
            previous = canonical.get(e["sha256"])
            if previous:
                os.link(previous, temporary)
                linked += 1
            else:
                with Path(e["source"]).open("rb") as src, temporary.open("xb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK)
                    dst.flush()
                    os.fsync(dst.fileno())
                os.chmod(
                    temporary, stat.S_IMODE(Path(e["source"]).stat().st_mode) & 0o777
                )
                os.utime(temporary, ns=(e["mtime_ns"], e["mtime_ns"]))
                copied += 1
            if digest(temporary) != e["sha256"]:
                raise RuntimeError(f"Copy verification failed: {target}")
            os.replace(temporary, target)
        canonical.setdefault(e["sha256"], target)
        if i % 250 == 0:
            print(f"Copied/verified {i + 1}/{len(m['entries'])}", flush=True)
    save(
        Path(run) / "copy.json",
        {
            "files": len(m["entries"]),
            "copied": copied,
            "hardlinked": linked,
            "manifest_sha256": digest(Path(run) / "manifest.json"),
        },
    )


def check_format(path):
    suffix = path.suffix.lower()
    if suffix == ".zip":
        import zipfile

        with zipfile.ZipFile(path) as z:
            encrypted = [i.filename for i in z.infolist() if i.flag_bits & 1]
            if encrypted:
                raise RuntimeError(
                    "Encrypted source archive needs its existing password"
                )
            bad = z.testzip()
            if bad:
                raise RuntimeError(f"ZIP CRC failure: {bad}")
        return {"type": "zip", "members": len(z.infolist())}
    if suffix == ".parquet":
        import pyarrow.parquet as pq

        f = pq.ParquetFile(path)
        rows = sum(batch.num_rows for batch in f.iter_batches(batch_size=65536))
        return {"type": "parquet", "rows": rows, "schema": str(f.schema_arrow)}
    if suffix == ".duckdb":
        import duckdb

        c = duckdb.connect(str(path), read_only=True)
        try:
            result = {"type": "duckdb", "tables": {}, "views": []}
            for schema, name, kind in c.execute(
                "SELECT table_schema,table_name,table_type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY 1,2"
            ).fetchall():
                quoted = (
                    '"'
                    + schema.replace('"', '""')
                    + '"."'
                    + name.replace('"', '""')
                    + '"'
                )
                columns = c.execute("DESCRIBE SELECT * FROM " + quoted).fetchall()
                try:
                    count = c.execute("SELECT count(*) FROM " + quoted).fetchone()[0]
                    result["tables"][schema + "." + name] = {
                        "rows": count,
                        "columns": columns,
                        "kind": kind,
                    }
                except Exception as ex:  # noqa: BLE001 -- record arbitrary reader failures without deleting originals.
                    result["tables"][schema + "." + name] = {
                        "error": str(ex),
                        "kind": kind,
                        "columns": columns,
                    }
            result["views"] = c.execute(
                "SELECT schema_name,view_name,sql FROM duckdb_views() WHERE NOT internal"
            ).fetchall()
            return result
        finally:
            c.close()
    if suffix in (".gz", ".tgz", ".bz2", ".xz", ".tar") and tarfile.is_tarfile(path):
        with tarfile.open(path) as t:
            count = 0
            for member in t:
                if member.isfile():
                    f = t.extractfile(member)
                    while f.read(CHUNK):
                        pass
                count += 1
        return {"type": "tar", "members": count}
    return {"type": "opaque", "sha256_verified": True}


def derived_files(run):
    path = Path(run) / "derived.json"
    return json.loads(path.read_text()) if path.exists() else []


def verify_derived(run):
    root = Path(load(run)["root"])
    for entry in derived_files(run):
        p = safe_target(root, entry["destination"])
        if not p.is_file() or digest(p) != entry["sha256"]:
            raise RuntimeError("Derived catalog/configuration changed: " + str(p))


def verify(run):
    run = Path(run)
    m = load(run)
    validate_volume(m["root"])
    failures, formats, checked = [], {}, set()
    verify_derived(run)
    for i, e in enumerate(m["entries"]):
        p = safe_target(m["root"], e["destination"])
        if not p.is_file() or digest(p) != e["sha256"]:
            failures.append(
                {
                    "source": e["source"],
                    "destination": str(p),
                    "reason": "destination hash mismatch",
                }
            )
            continue
        if not unchanged(e):
            failures.append(
                {"source": e["source"], "reason": "source changed since inventory"}
            )
        if e["sha256"] not in checked:
            checked.add(e["sha256"])
            try:
                formats[e["sha256"]] = check_format(p)
            except Exception as ex:  # noqa: BLE001 -- record arbitrary reader failures without deleting originals.
                try:
                    check_format(Path(e["source"]))
                    preexisting = False
                except Exception:  # noqa: BLE001 -- compare source readability after a destination reader failure.
                    preexisting = True
                failures.append(
                    {
                        "source": e["source"],
                        "destination": str(p),
                        "reason": str(ex),
                        "preexisting": preexisting,
                    }
                )
        if i % 500 == 0:
            print(f"Verified {i + 1}/{len(m['entries'])}", flush=True)
    receipt = {
        "manifest_sha256": digest(run / "manifest.json"),
        "verified_files": len(m["entries"]),
        "failures": failures,
        "formats": formats,
        "passed": not failures,
        "derived_sha256": digest(run / "derived.json")
        if (run / "derived.json").exists()
        else None,
    }
    save(run / "verification.json", receipt)
    print(
        json.dumps(
            {
                "passed": receipt["passed"],
                "failures": len(failures),
                "format_checks": len(formats),
            }
        ),
        flush=True,
    )
    return receipt


def backup(run, destination):
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    run, destination = Path(run), Path(destination)
    m = load(run)
    validate_volume(m["root"])
    v = json.loads((run / "verification.json").read_text())
    if not v["passed"] or v["manifest_sha256"] != digest(run / "manifest.json"):
        raise RuntimeError("Successful current verification required before backup")
    info = volume_info("/Volumes/edicot300")
    if (
        info.get("MountPoint") != "/Volumes/edicot300"
        or info.get("VolumeUUID") == m["volume_uuid"]
    ):
        raise RuntimeError("Independent backup volume not mounted")
    if not destination.resolve().is_relative_to(Path("/Volumes/edicot300")):
        raise ValueError("Backup must be on edicot300")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(destination)
    service = "org.moer.datalake.backup." + run.name
    existing = subprocess.run(
        ["security", "find-generic-password", "-a", "mohit", "-s", service, "-w"],
        capture_output=True,
        text=True,
        check=False,
    )
    if existing.returncode:
        password = secrets.token_hex(32)
        subprocess.run(
            [
                "security",
                "add-generic-password",
                "-a",
                "mohit",
                "-s",
                service,
                "-w",
                password,
            ],
            check=True,
            capture_output=True,
        )
    # Read back before writing anything encrypted. Never print the key.
    key = bytes.fromhex(
        subprocess.check_output(
            ["security", "find-generic-password", "-a", "mohit", "-s", service, "-w"],
            text=True,
        ).strip()
    )
    nonce = secrets.token_bytes(12)
    enc = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
    enc.authenticate_additional_data(MAGIC)
    tmp = destination.with_suffix(destination.suffix + ".part")
    if tmp.exists():
        tmp.unlink()  # Restart only this command's incomplete generated archive.

    class EncryptWriter:
        def __init__(self, output):
            self.output = output

        def write(self, data):
            self.output.write(enc.update(data))
            return len(data)

        def flush(self):
            self.output.flush()

    root = Path(m["root"])
    verify_derived(run)
    derived_sha = (
        digest(run / "derived.json") if (run / "derived.json").exists() else None
    )
    if v.get("derived_sha256") != derived_sha:
        raise RuntimeError("Derived catalog verification is stale")
    payloads = sorted(
        {e["destination"] for e in m["entries"]}
        | {e["destination"] for e in derived_files(run)}
    )
    with tmp.open("xb") as out:
        out.write(MAGIC + nonce)
        with tarfile.open(fileobj=EncryptWriter(out), mode="w|gz") as archive:
            for i, relative in enumerate(payloads):
                archive.add(
                    safe_target(root, relative), arcname=relative, recursive=False
                )
                if i % 500 == 0:
                    print(f"Backing up {i + 1}/{len(payloads)}", flush=True)
            for name in (
                "manifest.json",
                "verification.json",
                "copy.json",
                "source-spec.json",
                "decisions.json",
                "derived.json",
            ):
                p = run / name
                if p.exists():
                    archive.add(p, arcname="migration/" + run.name + "/" + name)
        out.write(enc.finalize())
        out.write(enc.tag)
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, destination)
    receipt = {
        "archive": str(destination),
        "sha256": digest(destination),
        "keychain_service": service,
        "encryption": "AES-256-GCM",
        "manifest_sha256": digest(run / "manifest.json"),
        "backup_volume_uuid": info.get("VolumeUUID"),
        "restore_verified": False,
        "derived_sha256": derived_sha,
    }
    save(run / "backup.json", receipt)
    restore_test(run)


def restore_test(run):
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    run = Path(run)
    m = load(run)
    receipt = json.loads((run / "backup.json").read_text())
    archive = Path(receipt["archive"])
    if digest(archive) != receipt["sha256"]:
        raise RuntimeError("Backup hash changed")
    key = bytes.fromhex(
        subprocess.check_output(
            [
                "security",
                "find-generic-password",
                "-a",
                "mohit",
                "-s",
                receipt["keychain_service"],
                "-w",
            ],
            text=True,
        ).strip()
    )
    with tempfile.TemporaryDirectory(prefix="restore-", dir=run) as temporary:
        plain = Path(temporary) / "verified.tar.gz"
        with archive.open("rb") as src, plain.open("xb") as out:
            if src.read(len(MAGIC)) != MAGIC:
                raise RuntimeError("Invalid backup header")
            nonce = src.read(12)
            size = archive.stat().st_size - len(MAGIC) - 12 - 16
            src.seek(-16, 2)
            tag = src.read(16)
            src.seek(len(MAGIC) + 12)
            dec = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
            dec.authenticate_additional_data(MAGIC)
            while size:
                data = src.read(min(CHUNK, size))
                if not data:
                    raise RuntimeError("Truncated backup")
                size -= len(data)
                out.write(dec.update(data))
            out.write(dec.finalize())  # Authenticate before opening/extracting the tar.
        restore = Path(temporary) / "files"
        with tarfile.open(plain) as t:
            t.extractall(restore, filter="data")
        for i, e in enumerate(m["entries"] + derived_files(run)):
            if digest(safe_target(restore, e["destination"])) != e["sha256"]:
                raise RuntimeError(f"Restore mismatch: {e['destination']}")
            if i % 1000 == 0:
                print(f"Restore-verified {i + 1}/{len(m['entries'])}", flush=True)
        receipt["restore_verified"] = True
        receipt["restore_files"] = len(m["entries"])
        receipt["restore_verified_utc"] = datetime.now(UTC).isoformat()
    save(run / "backup.json", receipt)


def cleanup(run):
    run = Path(run)
    m = load(run)
    validate_volume(m["root"])
    v = json.loads((run / "verification.json").read_text())
    b = json.loads((run / "backup.json").read_text())
    expected = digest(run / "manifest.json")
    if (
        not v["passed"]
        or not b["restore_verified"]
        or v["manifest_sha256"] != expected
        or b["manifest_sha256"] != expected
    ):
        raise RuntimeError(
            "Verified migration and restored independent backup required"
        )
    verify_derived(run)
    derived_sha = (
        digest(run / "derived.json") if (run / "derived.json").exists() else None
    )
    if v.get("derived_sha256") != derived_sha or b.get("derived_sha256") != derived_sha:
        raise RuntimeError("Derived catalogs changed since verification or backup")
    if digest(b["archive"]) != b["sha256"]:
        raise RuntimeError("Independent backup unavailable or changed")
    receipt_path = run / "cleanup.json"
    receipt = (
        json.loads(receipt_path.read_text())
        if receipt_path.exists()
        else {
            "deleted": [],
            "retained": [],
            "icloud_warning": "iCloud source deletions propagate to synced devices.",
        }
    )
    journal = run / "cleanup-journal.jsonl"
    if journal.exists():
        receipt["deleted"] = [
            json.loads(line)
            for line in journal.read_text().splitlines()
            if line.strip()
        ]
    done = {e["source"] for e in receipt["deleted"]}
    # Preflight every remaining source and every destination before any new deletion.
    for e in m["entries"]:
        if digest(safe_target(m["root"], e["destination"])) != e["sha256"]:
            raise RuntimeError("Destination changed; cleanup refused")
        if e["cleanup"] and e["source"] not in done and not unchanged(e):
            raise RuntimeError(f"Source changed; cleanup refused: {e['source']}")
    for i, e in enumerate(m["entries"]):
        if e["source"] in done:
            continue
        if not e["cleanup"]:
            receipt["retained"].append(
                {
                    "source": e["source"],
                    "reason": "explicitly retained for review or code",
                }
            )
            continue
        if not unchanged(e):
            raise RuntimeError("Source changed immediately before deletion")
        Path(e["source"]).unlink()
        record = {"source": e["source"], "sha256": e["sha256"]}
        receipt["deleted"].append(record)
        with journal.open("a") as log:
            log.write(json.dumps(record) + "\n")
            log.flush()
            os.fsync(log.fileno())
        if i % 500 == 0:
            print(f"Cleanup {i + 1}/{len(m['entries'])}", flush=True)
    receipt["finished_utc"] = datetime.now(UTC).isoformat()
    save(receipt_path, receipt)
    print(
        json.dumps(
            {"deleted": len(receipt["deleted"]), "retained": len(receipt["retained"])}
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=["inventory", "copy", "verify", "backup", "restore-test", "cleanup"],
    )
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--spec", type=Path)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--backup", type=Path)
    args = parser.parse_args()
    if args.command == "inventory":
        scan(json.loads(args.spec.read_text()), args.root, args.run)
    elif args.command == "copy":
        copy_entries(args.run)
    elif args.command == "verify":
        if not verify(args.run)["passed"]:
            raise SystemExit(1)
    elif args.command == "backup":
        backup(args.run, args.backup)
    elif args.command == "restore-test":
        restore_test(args.run)
    elif args.command == "cleanup":
        cleanup(args.run)


if __name__ == "__main__":
    main()
