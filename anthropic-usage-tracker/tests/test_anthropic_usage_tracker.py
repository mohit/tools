from __future__ import annotations

from datetime import date
from decimal import Decimal

from anthropic_usage_tracker import extract_cost_map, extract_usage_rows


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
