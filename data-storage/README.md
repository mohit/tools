# Personal data storage and migration

The authoritative data root is `/Volumes/MoErMedia/datalake.me`. Code remains in
`/Users/mohit/code/tools`; data is not a Git-managed asset. moer-mini will become
the production writer in a separate deployment phase. Until then, ingestion is
paused and legacy entrypoints must not be used.

## Storage contract

- `raw/<source>/`: immutable original payloads and dated export bundles.
- `curated/<source>/`: active processed data; publish replacements atomically.
- `catalog/`: active descriptions and DuckDB catalogs.
- `databases/<tool>/`: application databases.
- `artifacts/<tool>/`: reports, charts and playlist exports.
- `state/<tool>/`: ingestion cursors and metadata.
- `versions/<origin>/`: historical or conflicting versions; do not glob these into active data.
- `migration/<run-id>/`: source inventory, hashes, provenance, decisions and receipts.

Identical immutable files share an inode using hard links. **Never edit migrated
raw or curated files in place**: atomically replace a logical file to avoid
changing its historical aliases. Distinct byte streams are retained. Record-level
reconciliation is explicitly deferred.

Existing ingestion scripts still require a separate runtime configuration rollout.
Do not assume their old defaults honor this contract yet. The migration does not
create schedules, configure SSH or start dashboards.

## Commands

Use an isolated environment with DuckDB, PyArrow and cryptography:

```sh
uv run --no-project --with-requirements data-storage/requirements.txt python \
  data-storage/consolidate.py copy --run /Volumes/MoErMedia/datalake.me/migration/RUN_ID
```

Available commands: `inventory --spec FILE`, `copy`, `verify`,
`backup --backup /Volumes/edicot300/Backups/Datalake/RUN_ID.tar.gz.aesgcm`,
`restore-test`, and `cleanup`. `--run` is required. The expected volume UUID is read from the data disk’s `.volume-identity.json`
(or explicit `MOER_MEDIA_VOLUME_UUID`); that machine-specific value is not in Git.
Inventory paths are explicit;
this utility never guesses which directories to delete. Inventory is finalized
before copy, and receipts bind to its SHA-256. A destination collision with
different content is an error rather than an overwrite.

`verify` reads every original and destination, checks ZIP CRCs and Parquet pages,
and inspects DuckDB schemas/counts and external views. Pre-existing damaged
archives or databases are reported; affected originals must remain for review.

`cleanup` requires a current verification receipt and a successfully restored
independent encrypted backup. It checks all remaining originals and destination
hashes before any new deletion, then checks each original again immediately
before unlinking it. It never recursively deletes source directories. A durable
per-file journal records successful deletions. If interrupted between unlink and
journal persistence, it stops for manual reconciliation rather than guessing.
Changed files are never removed. Empty directories and ignored system files may
remain; they contain no migrated personal payloads.

## Recovery

The backup is a gzip-compressed tar encrypted with AES-256-GCM. Its format is
`MOERBACKUP1\n`, 12-byte nonce, ciphertext, then a 16-byte authentication tag.
The magic header is authenticated as additional data. A random 256-bit key is
stored in the macOS login Keychain under `org.moer.datalake.backup.<run-id>` for
account `mohit`. It is never stored on either data disk or printed by the utility.
The key must remain available on the laptop; transfer it securely before retiring
that Mac. `backup.json` identifies the archive and Keychain service.

`restore-test` authenticates the entire archive before opening its contents,
extracts into a temporary folder on MoErMedia, compares every restored payload
and derived catalog hash, then removes only that temporary folder. Keep the
independent backup on edicot300 after source cleanup.

Deleting originals from iCloud Drive propagates to other synced devices.
Credential material and ambiguous/unrelated exports are excluded and listed in
the run's review decisions. Git history is retained; it may still contain data
that was previously tracked.

## Tests

Tests use Python unittest, temporary directories and mocked volume/Keychain
lookups. They cover interrupted-copy resumption, exact-byte deduplication,
conflicting versions, changed sources, path traversal, missing/wrong disks,
cleanup gates, authenticated restore and tamper detection.

```sh
uv run --no-project --with-requirements data-storage/requirements.txt python \
  -m unittest discover -s data-storage/tests -v
```

## Portable private transfer

`transfer.py` restores an authenticated private package into a **new** directory:

```sh
uv run --no-project --with-requirements data-storage/requirements.txt python \
  data-storage/transfer.py --archive /path/to/moer-mini-private.tar.gz.aesgcm \
  --key-file /path/to/separately-transferred.key --destination /path/to/new/private-staging
```

The transfer key stays separate from the archive, in the laptop login Keychain
and a local mode-600 key file outside this repository. The archive contains only
selected tool credentials/configuration, data-backup recovery material, and a
reference snapshot of the retired Documents checkout. It does not export the
user's whole Keychain, SSH keys, or unrelated account credentials. The included
inventory identifies credentials that still require provisioning in step 3.

Restoration authenticates the complete ciphertext before extraction, checks all
payload hashes and exact file coverage, and never overwrites an existing folder.
It does not install credentials, run scripts, start services, or configure the mini.
Keep restored private files outside Git and restrict their directory permissions.
After step 5, pin the final handoff and recovery instructions in the app as requested.
