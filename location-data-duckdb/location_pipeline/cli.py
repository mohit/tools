from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from .database import connect, init_db
from .runner import run_enrichment, run_with_audit


def _load_config(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(prog="location-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db")
    init_parser.add_argument("--db-path", required=True)

    run_source_parser = subparsers.add_parser("run-source")
    run_source_parser.add_argument("--config", required=True)
    run_source_parser.add_argument("--source", required=True)

    run_all_parser = subparsers.add_parser("run-all")
    run_all_parser.add_argument("--config", required=True)

    args = parser.parse_args()

    if args.command == "init-db":
        Path(args.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = connect(args.db_path)
        init_db(conn)
        print(f"Initialized DB at {args.db_path}")
        return

    cfg = _load_config(args.config)
    db_path = cfg["db_path"]
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
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
