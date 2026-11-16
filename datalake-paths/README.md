# datalake-paths

Shared, standard-library-only module that every tool uses to find and verify
datalake storage on moer-mini.

## Layout

All locations derive from `DATALAKE_ROOT` (default `/Volumes/MoErMedia/datalake.me`):

| Area | Path | Used for |
| --- | --- | --- |
| raw | `raw/<source>/` | immutable source payloads |
| curated | `curated/<source>/` | active processed Parquet |
| catalog | `catalog/` | dataset metadata and DuckDB catalogs |
| databases | `databases/<tool>/` | application databases (Reflection, location) |
| artifacts | `artifacts/<tool>/` | reports, charts, playlists |
| state | `state/<tool>/` | cursors, checkpoints, staleness metadata |

`DATALAKE_RAW_ROOT`, `DATALAKE_CURATED_ROOT`, `DATALAKE_STRAVA_CURATED_ROOT`,
`LASTFM_CATALOG_FILE`, `~/datalake.me`, iCloud “Data Exports”, `./data` and
`~/.local/share/datalake` are no longer used.

## Fail before writing

`require_root()` (called by every entry point before any network request or
write) refuses the root unless:

1. it is an existing, writable, non-symlink directory;
2. `.volume-identity.json` names it as `canonical_root`;
3. it sits on a separate mounted volume whose UUID matches that file
   (or `MOER_MEDIA_VOLUME_UUID`).

An unplugged disk, an empty `/Volumes/MoErMedia` folder on the startup disk, or
a different disk therefore stops the tool with “no data was written”.
`within_root()` additionally rejects explicit output paths outside the lake.

## Inode-safe writes

Migrated files share inodes with preserved versions. Use `atomic_write_text`,
`atomic_append_text`, `publish_file` or `atomic_output` (for DuckDB `COPY` and
`pq.write_table`): they write a sibling temp file and `os.replace` it, leaving
other hard links untouched.

## Tests

```sh
python3 -m unittest discover -s datalake-paths/tests -v
```
