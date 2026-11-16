#!/usr/bin/env python3
"""Encrypt a private transfer tree; authenticate and verify it before restoration."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tarfile
import tempfile
from pathlib import Path

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

MAGIC = b"MOERBACKUP1\n"
CHUNK = 4 * 1024 * 1024


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def encrypt_tree(root, archive, key):
    root, archive = Path(root), Path(archive)
    if len(key) != 32:
        raise ValueError("A 256-bit key is required")
    if archive.exists():
        raise FileExistsError(archive)
    archive.parent.mkdir(parents=True, exist_ok=True)
    nonce = os.urandom(12)
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
    cipher.authenticate_additional_data(MAGIC)

    class Writer:
        def __init__(self, stream):
            self.stream = stream

        def write(self, data):
            self.stream.write(cipher.update(data))
            return len(data)

        def flush(self):
            self.stream.flush()

    partial = archive.with_name(archive.name + ".part")
    try:
        with partial.open("xb") as stream:
            os.chmod(partial, 0o600)
            stream.write(MAGIC + nonce)
            with tarfile.open(fileobj=Writer(stream), mode="w|gz") as bundle:
                for path in sorted(root.rglob("*")):
                    if path.is_symlink():
                        raise ValueError(
                            "Symlinks are not accepted in transfer packages"
                        )
                    if path.is_file():
                        bundle.add(
                            path, arcname=str(path.relative_to(root)), recursive=False
                        )
            stream.write(cipher.finalize())
            stream.write(cipher.tag)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, archive)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return sha256(archive)


def restore_tree(archive, key, destination):
    archive, destination = Path(archive), Path(destination)
    if destination.exists():
        raise FileExistsError(
            "Choose a new destination; existing files are never overwritten"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".moer-restore-", dir=destination.parent
    ) as temp:
        plain = Path(temp) / "authenticated.tar.gz"
        with archive.open("rb") as source, plain.open("xb") as output:
            if source.read(len(MAGIC)) != MAGIC:
                raise ValueError("Invalid transfer header")
            nonce = source.read(12)
            size = archive.stat().st_size - len(MAGIC) - 12 - 16
            if size < 0:
                raise ValueError("Truncated transfer archive")
            source.seek(-16, 2)
            tag = source.read(16)
            source.seek(len(MAGIC) + 12)
            cipher = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
            cipher.authenticate_additional_data(MAGIC)
            while size:
                block = source.read(min(CHUNK, size))
                if not block:
                    raise ValueError("Truncated transfer archive")
                size -= len(block)
                output.write(cipher.update(block))
            output.write(cipher.finalize())
        files = Path(temp) / "files"
        files.mkdir(mode=0o700)
        with tarfile.open(plain) as bundle:
            for entry in bundle.getmembers():
                path = Path(entry.name)
                if path.is_absolute() or ".." in path.parts or entry.issym():
                    raise ValueError("Unsafe archive member")
            bundle.extractall(files, filter="data")
        manifest = json.loads((files / "TRANSFER_MANIFEST.json").read_text())
        expected = {entry["path"]: entry["sha256"] for entry in manifest["files"]}
        actual = {str(p.relative_to(files)) for p in files.rglob("*") if p.is_file()}
        if actual != set(expected) | {"TRANSFER_MANIFEST.json"}:
            raise ValueError("Archive file coverage differs from its manifest")
        for relative, digest in expected.items():
            path = files / relative
            if path.is_symlink() or sha256(path) != digest:
                raise ValueError("Transfer file checksum mismatch")
        os.replace(files, destination)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--key-file", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args()
    key = bytes.fromhex(args.key_file.read_text().strip())
    manifest = restore_tree(args.archive, key, args.destination)
    print(
        f"Restored and verified {len(manifest['files'])} private files. Nothing was installed or executed."
    )


if __name__ == "__main__":
    main()
