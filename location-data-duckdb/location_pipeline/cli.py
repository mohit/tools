from __future__ import annotations

import argparse
from pathlib import Path

import datalake_paths as dp
import yaml

from .database import connect, init_db
from .runner import run_enrichment, run_with_audit


def _load_config(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _db_path(value: str | None) -> Path:
    """Database location inside the verified datalake (verified before any write)."""
    try:
        return dp.within_root(value or dp.databases("location-data-duckdb", "location.duckdb"))
    except dp.DatalakeUnavailable as exc:
        raise SystemExit(str(exc)) from None


def _verify_source_paths(cfg: dict) -> None:
    """Writable caches named in the config must also live inside the datalake."""
    for source_cfg in (cfg.get("sources") or {}).values():
        cache = (source_cfg or {}).get("places_cache_path")
        if cache:
            try:
                source_cfg["places_cache_path"] = str(dp.within_root(cache))
            except dp.DatalakeUnavailable as exc:
                raise SystemExit(str(exc)) from None


def main() -> None:
    parser = argparse.ArgumentParser(prog="location-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db")
    init_parser.add_argument(
        "--db-path",
        default=None,
        help="Default: $DATALAKE_ROOT/databases/location-data-duckdb/location.duckdb",
    )

    run_source_parser = subparsers.add_parser("run-source")
    run_source_parser.add_argument("--config", required=True)
    run_source_parser.add_argument("--source", required=True)

    run_all_parser = subparsers.add_parser("run-all")
    run_all_parser.add_argument("--config", required=True)

    args = parser.parse_args()

    if args.command == "init-db":
        db_path = _db_path(args.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect(str(db_path))
        init_db(conn)
        print(f"Initialized DB at {db_path}")
        return

    cfg = _load_config(args.config)
    db_path = _db_path(cfg.get("db_path"))
    _verify_source_paths(cfg)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(str(db_path))
    init_db(conn)

    if args.command == "run-source":
        source_cfg = cfg["sources"].get(args.source)
        if not source_cfg or not source_cfg.get("enabled", False):
            print(f"Source '{args.source}' disabled or missing")
            return
        raw_count, visit_count, saved_count, review_count = run_with_audit(conn, args.source, source_cfg)
        print(f"{args.source}: raw={raw_count} visits={visit_count} saved={saved_count} reviews={review_count}")
        enriched = run_enrichment(conn, cfg.get("enrichment", {}))
        if enriched:
            print(f"google_places enriched={enriched}")
        return

    if args.command == "run-all":
        for source_name, source_cfg in cfg.get("sources", {}).items():
            if not source_cfg.get("enabled", False):
                continue
            raw_count, visit_count, saved_count, review_count = run_with_audit(conn, source_name, source_cfg)
            print(f"{source_name}: raw={raw_count} visits={visit_count} saved={saved_count} reviews={review_count}")

        enriched = run_enrichment(conn, cfg.get("enrichment", {}))
        if enriched:
            print(f"google_places enriched={enriched}")


if __name__ == "__main__":
    main()
