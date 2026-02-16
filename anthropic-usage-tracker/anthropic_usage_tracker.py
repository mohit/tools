#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest
from typing import Any

import duckdb

API_BASE = "https://api.anthropic.com"
ANTHROPIC_VERSION = "2023-06-01"

DEFAULT_PRICING = {
    "claude-opus-4-1": {"input_per_million": Decimal("15"), "output_per_million": Decimal("75")},
    "claude-opus-4": {"input_per_million": Decimal("15"), "output_per_million": Decimal("75")},
    "claude-sonnet-4-5": {"input_per_million": Decimal("3"), "output_per_million": Decimal("15")},
    "claude-sonnet-4": {"input_per_million": Decimal("3"), "output_per_million": Decimal("15")},
    "claude-3-7-sonnet-latest": {"input_per_million": Decimal("3"), "output_per_million": Decimal("15")},
}


@dataclass
class UsageRow:
    snapshot_date: str
    model: str
    model_family: str
    api_key_id: str | None
    agent: str | None
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int
    estimated_cost_usd: Decimal
    api_reported_cost_usd: Decimal | None


class AnthropicAPIError(RuntimeError):
    def __init__(self, status_code: int | None, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Track Anthropic usage and estimated costs")
    parser.add_argument(
        "--date",
        default=datetime.now(UTC).date().isoformat(),
        help="Snapshot date YYYY-MM-DD (UTC)",
    )
    parser.add_argument(
        "--datalake-root",
        default="~/datalake.me",
        help="Root path for datalake outputs (default: ~/datalake.me)",
    )
    parser.add_argument(
        "--api-key-env",
        default="ANTHROPIC_ADMIN_API_KEY",
        help="Env var containing Anthropic Admin key",
    )
    parser.add_argument(
        "--pricing-json",
        help="Optional pricing overrides JSON file keyed by model",
    )
    parser.add_argument(
        "--agent-map-json",
        help="Optional JSON file mapping api_key_id to agent/session label",
    )
    parser.add_argument(
        "--console-csv",
        help="Fallback CSV export path from Anthropic console when Admin API is unavailable",
    )
    return parser.parse_args()


def parse_snapshot_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid --date '{value}'. Expected YYYY-MM-DD.") from exc


def load_json_map(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected JSON object in {path}")
    return payload


def load_agent_map(path: str | None) -> dict[str, str]:
    file_map: dict[str, str] = {}
    if path:
        raw = load_json_map(path)
        file_map = {str(k): str(v) for k, v in raw.items()}

    env_map_raw = os.getenv("ANTHROPIC_AGENT_MAP_JSON", "")
    env_map: dict[str, str] = {}
    if env_map_raw:
        try:
            payload = json.loads(env_map_raw)
        except json.JSONDecodeError as exc:
            raise SystemExit("ANTHROPIC_AGENT_MAP_JSON must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise SystemExit("ANTHROPIC_AGENT_MAP_JSON must be a JSON object")
        env_map = {str(k): str(v) for k, v in payload.items()}

    out = file_map.copy()
    out.update(env_map)
    return out


def load_pricing(path: str | None) -> dict[str, dict[str, Decimal]]:
    pricing = {k: dict(v) for k, v in DEFAULT_PRICING.items()}
    if not path:
        return pricing

    raw = load_json_map(path)
    for model, entry in raw.items():
        if not isinstance(entry, dict):
            raise SystemExit(f"Pricing for model '{model}' must be an object")
        in_price = decimal_from_any(entry.get("input_per_million"), Decimal("0"))
        out_price = decimal_from_any(entry.get("output_per_million"), Decimal("0"))
        pricing[str(model)] = {
            "input_per_million": in_price,
            "output_per_million": out_price,
        }
    return pricing


def build_time_bounds(day: date) -> tuple[str, str]:
    start = datetime.combine(day, time.min, tzinfo=UTC)
    end = datetime.combine(day, time.max.replace(microsecond=0), tzinfo=UTC)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def anthropic_headers(api_key: str) -> dict[str, str]:
    return {
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }


def fetch_paginated(
    api_key: str,
    endpoint: str,
    params: list[tuple[str, str]],
) -> dict[str, Any]:
    all_items: list[dict[str, Any]] = []
    next_page: str | None = None
    pages: list[dict[str, Any]] = []

    while True:
        request_params = list(params)
        if next_page:
            request_params.append(("page", next_page))

        query = urlparse.urlencode(request_params)
        req = urlrequest.Request(
            url=f"{API_BASE}{endpoint}?{query}",
            headers=anthropic_headers(api_key),
            method="GET",
        )
        try:
            with urlrequest.urlopen(req, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urlerror.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace")
            raise AnthropicAPIError(exc.code, message) from exc
        except urlerror.URLError as exc:
            raise AnthropicAPIError(None, str(exc.reason)) from exc

        pages.append(payload)

        items = payload.get("data", [])
        if isinstance(items, list):
            all_items.extend([item for item in items if isinstance(item, dict)])

        next_page = payload.get("next_page")
        if not next_page:
            break

    return {"data": all_items, "pages": pages}


def int_from_any(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def decimal_from_any(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


def optional_decimal_from_any(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def model_family(model: str) -> str:
    low = (model or "").lower()
    if "opus" in low:
        return "opus"
    if "sonnet" in low:
        return "sonnet"
    if "haiku" in low:
        return "haiku"
    return "other"


def flatten_usage_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for item in items:
        bucket_context = {
            "starting_at": item.get("starting_at"),
            "ending_at": item.get("ending_at"),
            "date": item.get("date"),
        }
        if isinstance(item.get("results"), list):
            for result in item["results"]:
                if isinstance(result, dict):
                    merged = bucket_context.copy()
                    merged.update(result)
                    rows.append(merged)
        else:
            merged = bucket_context.copy()
            merged.update(item)
            rows.append(merged)

    return rows


def extract_usage_rows(
    snapshot_day: date,
    raw_items: list[dict[str, Any]],
    pricing: dict[str, dict[str, Decimal]],
    agent_map: dict[str, str],
) -> list[UsageRow]:
    out: list[UsageRow] = []

    for raw in flatten_usage_items(raw_items):
        usage_obj = raw.get("usage") if isinstance(raw.get("usage"), dict) else raw

        model = str(raw.get("model") or "unknown")
        api_key_id = raw.get("api_key_id")
        if api_key_id is not None:
            api_key_id = str(api_key_id)

        input_tokens = int_from_any(usage_obj.get("input_tokens"))
        output_tokens = int_from_any(usage_obj.get("output_tokens"))
        cache_creation = int_from_any(usage_obj.get("cache_creation_input_tokens"))
        cache_read = int_from_any(usage_obj.get("cache_read_input_tokens"))

        if not (input_tokens or output_tokens or cache_creation or cache_read):
            continue

        model_price = pricing.get(model, {"input_per_million": Decimal("0"), "output_per_million": Decimal("0")})
        input_price = model_price.get("input_per_million", Decimal("0"))
        output_price = model_price.get("output_per_million", Decimal("0"))

        estimated = (
            (Decimal(input_tokens + cache_creation + cache_read) / Decimal("1000000")) * input_price
            + (Decimal(output_tokens) / Decimal("1000000")) * output_price
        )

        out.append(
            UsageRow(
                snapshot_date=snapshot_day.isoformat(),
                model=model,
                model_family=model_family(model),
                api_key_id=api_key_id,
                agent=agent_map.get(api_key_id) if api_key_id else None,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_creation_input_tokens=cache_creation,
                cache_read_input_tokens=cache_read,
                estimated_cost_usd=estimated,
                api_reported_cost_usd=None,
            )
        )

    return out


def flatten_cost_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return flatten_usage_items(items)


def extract_cost_map(raw_items: list[dict[str, Any]]) -> dict[tuple[str, str | None], Decimal]:
    out: dict[tuple[str, str | None], Decimal] = {}

    for raw in flatten_cost_items(raw_items):
        model = str(raw.get("model") or "unknown")
        api_key_id = raw.get("api_key_id")
        if api_key_id is not None:
            api_key_id = str(api_key_id)

        amount_obj = raw.get("amount") if isinstance(raw.get("amount"), dict) else None
        value: Decimal | None = None
        if amount_obj is not None:
            value = optional_decimal_from_any(amount_obj.get("value"))
        elif "cost_usd" in raw:
            value = optional_decimal_from_any(raw.get("cost_usd"))

        if value is None:
            continue

        key = (model, api_key_id)
        out[key] = out.get(key, Decimal("0")) + value

    return out


def parse_console_csv(path: Path, snapshot_day: date) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Console CSV not found: {path}")

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            lower = {k.lower().strip(): (v or "").strip() for k, v in row.items() if k}
            row_date = lower.get("date") or lower.get("day") or lower.get("snapshot_date")
            if row_date and row_date != snapshot_day.isoformat():
                continue

            model = lower.get("model") or "unknown"
            api_key_id = lower.get("api_key_id") or lower.get("api_key") or None

            rows.append(
                {
                    "model": model,
                    "api_key_id": api_key_id,
                    "usage": {
                        "input_tokens": int_from_any(lower.get("input_tokens")),
                        "output_tokens": int_from_any(lower.get("output_tokens")),
                        "cache_creation_input_tokens": int_from_any(lower.get("cache_creation_input_tokens")),
                        "cache_read_input_tokens": int_from_any(lower.get("cache_read_input_tokens")),
                    },
                }
            )

    return rows


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)


def write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False, encoding="utf-8") as tmp:
        tmp_path = Path(tmp.name)
        for row in rows:
            tmp.write(json.dumps(row, sort_keys=True, default=str))
            tmp.write("\n")

    con = duckdb.connect()
    try:
        in_path = str(tmp_path).replace("'", "''")
        out_path = str(path).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM read_json_auto('{in_path}')) TO '{out_path}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
        tmp_path.unlink(missing_ok=True)


def print_summary(snapshot_day: date, usage_rows: list[UsageRow], api_total_cost: Decimal | None) -> None:
    total_in = sum(row.input_tokens + row.cache_creation_input_tokens + row.cache_read_input_tokens for row in usage_rows)
    total_out = sum(row.output_tokens for row in usage_rows)
    est_total = sum((row.estimated_cost_usd for row in usage_rows), Decimal("0"))

    print(f"Date: {snapshot_day.isoformat()}")
    print(f"Total input tokens: {total_in}")
    print(f"Total output tokens: {total_out}")
    print(f"Estimated cost (USD): {est_total.quantize(Decimal('0.0001'))}")
    if api_total_cost is not None:
        print(f"API reported cost (USD): {api_total_cost.quantize(Decimal('0.0001'))}")

    by_model: dict[str, dict[str, Decimal | int]] = defaultdict(
        lambda: {"input": 0, "output": 0, "estimated": Decimal("0")}
    )
    for row in usage_rows:
        model = row.model
        by_model[model]["input"] = int(by_model[model]["input"]) + row.input_tokens + row.cache_creation_input_tokens + row.cache_read_input_tokens
        by_model[model]["output"] = int(by_model[model]["output"]) + row.output_tokens
        by_model[model]["estimated"] = Decimal(by_model[model]["estimated"]) + row.estimated_cost_usd

    print("Model breakdown:")
    for model, info in sorted(by_model.items(), key=lambda kv: Decimal(kv[1]["estimated"]), reverse=True):
        est = Decimal(info["estimated"]).quantize(Decimal("0.0001"))
        print(f"  - {model}: in={info['input']} out={info['output']} est_usd={est}")


def build_model_parquet_rows(snapshot_day: date, usage_rows: list[UsageRow]) -> list[dict[str, Any]]:
    model_rollup: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "snapshot_date": snapshot_day.isoformat(),
            "model": "",
            "model_family": "other",
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "estimated_cost_usd": Decimal("0"),
            "api_reported_cost_usd_sum": None,
            "ingested_at": datetime.now(UTC).isoformat(),
        }
    )

    for row in usage_rows:
        r = model_rollup[row.model]
        r["model"] = row.model
        r["model_family"] = row.model_family
        r["input_tokens"] += row.input_tokens
        r["output_tokens"] += row.output_tokens
        r["cache_creation_input_tokens"] += row.cache_creation_input_tokens
        r["cache_read_input_tokens"] += row.cache_read_input_tokens
        r["estimated_cost_usd"] = Decimal(r["estimated_cost_usd"]) + row.estimated_cost_usd
        if row.api_reported_cost_usd is not None:
            current_cost = r["api_reported_cost_usd_sum"]
            r["api_reported_cost_usd_sum"] = (
                row.api_reported_cost_usd
                if current_cost is None
                else Decimal(current_cost) + row.api_reported_cost_usd
            )

    model_parquet_rows = []
    for info in model_rollup.values():
        api_reported_cost_usd_sum = info["api_reported_cost_usd_sum"]
        api_reported_cost_usd = (
            None if api_reported_cost_usd_sum is None else float(Decimal(api_reported_cost_usd_sum))
        )
        model_parquet_rows.append(
            {
                "snapshot_date": info["snapshot_date"],
                "model": info["model"],
                "model_family": info["model_family"],
                "input_tokens": info["input_tokens"],
                "output_tokens": info["output_tokens"],
                "cache_creation_input_tokens": info["cache_creation_input_tokens"],
                "cache_read_input_tokens": info["cache_read_input_tokens"],
                "ingested_at": info["ingested_at"],
                "estimated_cost_usd": float(Decimal(info["estimated_cost_usd"])),
                "api_reported_cost_usd": api_reported_cost_usd,
            }
        )

    return model_parquet_rows


def main() -> None:
    args = parse_args()
    snapshot_day = parse_snapshot_date(args.date)
    start_iso, end_iso = build_time_bounds(snapshot_day)

    datalake_root = Path(args.datalake_root).expanduser()
    raw_usage_dir = datalake_root / "raw" / "anthropic" / "usage_report" / f"date={snapshot_day.isoformat()}"
    raw_cost_dir = datalake_root / "raw" / "anthropic" / "cost_report" / f"date={snapshot_day.isoformat()}"

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    raw_usage_path = raw_usage_dir / f"usage_{ts}.json"
    raw_cost_path = raw_cost_dir / f"cost_{ts}.json"

    pricing = load_pricing(args.pricing_json)
    agent_map = load_agent_map(args.agent_map_json)

    api_key = os.getenv(args.api_key_env)
    usage_payload: dict[str, Any] | None = None
    cost_payload: dict[str, Any] | None = None

    if api_key:
        usage_params = [
            ("starting_at", start_iso),
            ("ending_at", end_iso),
            ("bucket_width", "1d"),
            ("group_by[]", "model"),
            ("group_by[]", "api_key_id"),
            ("limit", "1000"),
        ]
        try:
            usage_payload = fetch_paginated(api_key, "/v1/organizations/usage_report/messages", usage_params)
            write_json(raw_usage_path, usage_payload)
        except AnthropicAPIError as exc:
            usage_payload = None
            if not args.console_csv:
                raise SystemExit(
                    f"Failed to pull usage report (HTTP {exc.status_code or 'unknown'}). "
                    "Provide --console-csv as fallback."
                ) from exc

        if usage_payload is not None:
            cost_params = [
                ("starting_at", start_iso),
                ("ending_at", end_iso),
                ("bucket_width", "1d"),
                ("group_by[]", "model"),
                ("group_by[]", "api_key_id"),
                ("limit", "1000"),
            ]
            try:
                cost_payload = fetch_paginated(api_key, "/v1/organizations/cost_report", cost_params)
                write_json(raw_cost_path, cost_payload)
            except AnthropicAPIError:
                fallback_cost_params = [
                    ("starting_at", start_iso),
                    ("ending_at", end_iso),
                    ("bucket_width", "1d"),
                    ("limit", "1000"),
                ]
                try:
                    cost_payload = fetch_paginated(api_key, "/v1/organizations/cost_report", fallback_cost_params)
                    write_json(raw_cost_path, cost_payload)
                except AnthropicAPIError:
                    cost_payload = None

    if usage_payload is None:
        if not args.console_csv:
            raise SystemExit(
                f"Env var {args.api_key_env} is missing and no --console-csv fallback was provided."
            )
        csv_rows = parse_console_csv(Path(args.console_csv).expanduser(), snapshot_day)
        usage_payload = {"data": csv_rows, "source": "console_csv", "path": str(Path(args.console_csv).expanduser())}
        write_json(raw_usage_path, usage_payload)

    usage_rows = extract_usage_rows(snapshot_day, usage_payload.get("data", []), pricing, agent_map)
    if not usage_rows:
        raise SystemExit("No usage rows found for the selected date")

    cost_map = extract_cost_map(cost_payload.get("data", [])) if cost_payload else {}
    for row in usage_rows:
        row.api_reported_cost_usd = cost_map.get((row.model, row.api_key_id))

    usage_parquet_rows: list[dict[str, Any]] = [
        {
            "snapshot_date": row.snapshot_date,
            "model": row.model,
            "model_family": row.model_family,
            "api_key_id": row.api_key_id,
            "agent": row.agent,
            "input_tokens": row.input_tokens,
            "output_tokens": row.output_tokens,
            "cache_creation_input_tokens": row.cache_creation_input_tokens,
            "cache_read_input_tokens": row.cache_read_input_tokens,
            "estimated_cost_usd": float(row.estimated_cost_usd),
            "api_reported_cost_usd": float(row.api_reported_cost_usd) if row.api_reported_cost_usd is not None else None,
            "ingested_at": datetime.now(UTC).isoformat(),
        }
        for row in usage_rows
    ]

    model_parquet_rows = build_model_parquet_rows(snapshot_day, usage_rows)

    usage_out = (
        datalake_root
        / "curated"
        / "anthropic"
        / "usage_daily"
        / f"year={snapshot_day.year:04d}"
        / f"month={snapshot_day.month:02d}"
        / f"day={snapshot_day.day:02d}"
        / f"usage_daily_{ts}.parquet"
    )
    model_out = (
        datalake_root
        / "curated"
        / "anthropic"
        / "model_daily"
        / f"year={snapshot_day.year:04d}"
        / f"month={snapshot_day.month:02d}"
        / f"day={snapshot_day.day:02d}"
        / f"model_daily_{ts}.parquet"
    )

    write_parquet(usage_out, usage_parquet_rows)
    write_parquet(model_out, model_parquet_rows)

    api_total_cost = None
    if cost_payload:
        api_total_cost = sum((extract_cost_map(cost_payload.get("data", [])).values()), Decimal("0"))

    print_summary(snapshot_day, usage_rows, api_total_cost)
    print(f"Usage parquet: {usage_out}")
    print(f"Model parquet: {model_out}")


if __name__ == "__main__":
    main()
