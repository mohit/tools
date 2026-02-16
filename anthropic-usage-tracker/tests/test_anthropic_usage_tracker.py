from __future__ import annotations

from datetime import date
from decimal import Decimal

from anthropic_usage_tracker import UsageRow, build_model_parquet_rows, extract_cost_map, extract_usage_rows


def test_extract_usage_rows_with_nested_results() -> None:
    raw_items = [
        {
            "starting_at": "2026-02-11T00:00:00Z",
            "ending_at": "2026-02-11T23:59:59Z",
            "results": [
                {
                    "model": "claude-opus-4-1",
                    "api_key_id": "key_opus",
                    "usage": {
                        "input_tokens": 1_000_000,
                        "output_tokens": 100_000,
                        "cache_creation_input_tokens": 0,
                        "cache_read_input_tokens": 0,
                    },
                },
                {
                    "model": "claude-sonnet-4-5",
                    "api_key_id": "key_sonnet",
                    "usage": {
                        "input_tokens": 2_000_000,
                        "output_tokens": 200_000,
                        "cache_creation_input_tokens": 100_000,
                        "cache_read_input_tokens": 50_000,
                    },
                },
            ],
        }
    ]

    pricing = {
        "claude-opus-4-1": {"input_per_million": Decimal("15"), "output_per_million": Decimal("75")},
        "claude-sonnet-4-5": {"input_per_million": Decimal("3"), "output_per_million": Decimal("15")},
    }
    agent_map = {"key_opus": "opus-main", "key_sonnet": "sonnet-silt"}

    rows = extract_usage_rows(date(2026, 2, 11), raw_items, pricing, agent_map)

    assert len(rows) == 2
    assert rows[0].agent == "opus-main"
    assert rows[0].model_family == "opus"
    assert rows[1].agent == "sonnet-silt"
    assert rows[1].model_family == "sonnet"

    # Opus: (1,000,000 in * $15) + (100,000 out * $75) = 15 + 7.5
    assert rows[0].estimated_cost_usd == Decimal("22.5")

    # Sonnet includes cache tokens in the input-side estimate.
    # Input side: 2,150,000 * $3/M = 6.45, Output side: 200,000 * $15/M = 3.0
    assert rows[1].estimated_cost_usd == Decimal("9.45")


def test_extract_cost_map_with_amount_objects() -> None:
    raw_items = [
        {
            "starting_at": "2026-02-11T00:00:00Z",
            "ending_at": "2026-02-11T23:59:59Z",
            "results": [
                {
                    "model": "claude-opus-4-1",
                    "api_key_id": "key_opus",
                    "amount": {"currency": "USD", "value": "10.25"},
                },
                {
                    "model": "claude-opus-4-1",
                    "api_key_id": "key_opus",
                    "amount": {"currency": "USD", "value": "1.75"},
                },
                {
                    "model": "claude-sonnet-4-5",
                    "api_key_id": "key_sonnet",
                    "amount": {"currency": "USD", "value": "3.00"},
                },
            ],
        }
    ]

    cost_map = extract_cost_map(raw_items)
    assert cost_map[("claude-opus-4-1", "key_opus")] == Decimal("12.00")
    assert cost_map[("claude-sonnet-4-5", "key_sonnet")] == Decimal("3.00")


def test_extract_cost_map_skips_unparseable_or_missing_cost_values() -> None:
    raw_items = [
        {
            "starting_at": "2026-02-11T00:00:00Z",
            "ending_at": "2026-02-11T23:59:59Z",
            "results": [
                {
                    "model": "claude-opus-4-1",
                    "api_key_id": "key_missing",
                    "amount": {"currency": "USD"},
                },
                {
                    "model": "claude-opus-4-1",
                    "api_key_id": "key_invalid",
                    "amount": {"currency": "USD", "value": "not-a-number"},
                },
                {
                    "model": "claude-opus-4-1",
                    "api_key_id": "key_valid",
                    "amount": {"currency": "USD", "value": "0"},
                },
            ],
        }
    ]

    cost_map = extract_cost_map(raw_items)

    assert ("claude-opus-4-1", "key_missing") not in cost_map
    assert ("claude-opus-4-1", "key_invalid") not in cost_map
    assert cost_map[("claude-opus-4-1", "key_valid")] == Decimal("0")


def test_build_model_parquet_rows_keeps_api_cost_nullable_without_mapped_costs() -> None:
    rows = [
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_1",
            agent="agent-a",
            input_tokens=100,
            output_tokens=50,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("1.25"),
            api_reported_cost_usd=None,
        ),
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_2",
            agent="agent-b",
            input_tokens=200,
            output_tokens=75,
            cache_creation_input_tokens=10,
            cache_read_input_tokens=5,
            estimated_cost_usd=Decimal("2.75"),
            api_reported_cost_usd=None,
        ),
    ]

    model_rows = build_model_parquet_rows(date(2026, 2, 11), rows)

    assert len(model_rows) == 1
    assert model_rows[0]["model"] == "claude-sonnet-4-5"
    assert model_rows[0]["estimated_cost_usd"] == 4.0
    assert model_rows[0]["api_reported_cost_usd"] is None


def test_build_model_parquet_rows_sets_api_cost_when_any_row_has_mapped_cost() -> None:
    rows = [
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_1",
            agent="agent-a",
            input_tokens=100,
            output_tokens=50,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("1.00"),
            api_reported_cost_usd=None,
        ),
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_2",
            agent="agent-b",
            input_tokens=120,
            output_tokens=60,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("1.50"),
            api_reported_cost_usd=Decimal("2.25"),
        ),
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_3",
            agent="agent-c",
            input_tokens=80,
            output_tokens=40,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("0.75"),
            api_reported_cost_usd=Decimal("0.75"),
        ),
    ]

    model_rows = build_model_parquet_rows(date(2026, 2, 11), rows)

    assert len(model_rows) == 1
    assert model_rows[0]["model"] == "claude-sonnet-4-5"
    assert model_rows[0]["estimated_cost_usd"] == 3.25
    assert model_rows[0]["api_reported_cost_usd"] == 3.0


def test_build_model_parquet_rows_keeps_mapped_zero_api_cost_as_real_value() -> None:
    rows = [
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_1",
            agent="agent-a",
            input_tokens=100,
            output_tokens=50,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("1.25"),
            api_reported_cost_usd=None,
        ),
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_2",
            agent="agent-b",
            input_tokens=100,
            output_tokens=50,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("1.25"),
            api_reported_cost_usd=Decimal("0"),
        ),
    ]

    model_rows = build_model_parquet_rows(date(2026, 2, 11), rows)

    assert len(model_rows) == 1
    assert model_rows[0]["api_reported_cost_usd"] == 0.0


def test_build_model_parquet_rows_keeps_unmapped_model_api_cost_nullable_when_other_models_have_costs() -> None:
    rows = [
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-sonnet-4-5",
            model_family="sonnet",
            api_key_id="key_1",
            agent="agent-a",
            input_tokens=100,
            output_tokens=50,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("1.25"),
            api_reported_cost_usd=None,
        ),
        UsageRow(
            snapshot_date="2026-02-11",
            model="claude-haiku-3-5",
            model_family="haiku",
            api_key_id="key_2",
            agent="agent-b",
            input_tokens=100,
            output_tokens=50,
            cache_creation_input_tokens=0,
            cache_read_input_tokens=0,
            estimated_cost_usd=Decimal("0.25"),
            api_reported_cost_usd=Decimal("0.75"),
        ),
    ]

    model_rows = build_model_parquet_rows(date(2026, 2, 11), rows)
    by_model = {row["model"]: row for row in model_rows}

    assert by_model["claude-sonnet-4-5"]["api_reported_cost_usd"] is None
    assert by_model["claude-haiku-3-5"]["api_reported_cost_usd"] == 0.75
