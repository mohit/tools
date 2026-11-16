#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

try:
    import yaml
except ImportError as exc:  # pragma: no cover - handled for CLI users
    raise SystemExit(
        "PyYAML is required. Install it with `python3 -m pip install -r requirements.txt`."
    ) from exc


CANVAS_WIDTH = 1400
CANVAS_HEIGHT = 1080
MARGIN_LEFT = 140
MARGIN_RIGHT = 120
MARGIN_TOP = 64
MARGIN_BOTTOM = 184
PLOT_LEFT = MARGIN_LEFT
PLOT_TOP = MARGIN_TOP
PLOT_WIDTH = CANVAS_WIDTH - MARGIN_LEFT - MARGIN_RIGHT
PLOT_HEIGHT = CANVAS_HEIGHT - MARGIN_TOP - MARGIN_BOTTOM
PLOT_RIGHT = PLOT_LEFT + PLOT_WIDTH
PLOT_BOTTOM = PLOT_TOP + PLOT_HEIGHT

BACKGROUND = "#faf8f4"
PLOT_BACKGROUND = "#ffffff"
AXIS = "#4f5966"
GRID = "#e9e7e0"
REFERENCE = "#d6d3cb"
TEXT = "#1a1f29"
MUTED_TEXT = "#6f737b"
GREEN = "#2f8b3a"
GREEN_LIGHT = "#cfe3d1"
TREND = "#8db792"
GRAY = "#b7b8b6"
GRAY_LIGHT = "#d9dad7"

SECONDS = 1 / 60.0
MINUTE = 1.0
HOUR = 60.0
DAY = 24 * HOUR
WEEK = 7 * DAY
MONTH = 30 * DAY
ORIN_LABS_RELEASE_DATE = date(2026, 5, 15)


DISPLAY_NAMES = {
    "gpt2": "GPT-2",
    "davinci_002": "GPT-3",
    "gpt_3_5_turbo_instruct": "GPT-3.5",
    "gpt_4": "GPT-4",
    "gpt_4_1106_inspect": "GPT-4 Turbo",
    "claude_3_opus_inspect": "Claude 3 Opus",
    "gpt_4_turbo_inspect": "GPT-4 Turbo (Apr)",
    "gpt_4o_inspect": "GPT-4o",
    "claude_3_5_sonnet_20240620_inspect": "Claude 3.5 Sonnet",
    "o1_preview": "o1-preview",
    "claude_3_5_sonnet_20241022_inspect": "Claude 3.5 Sonnet (Oct)",
    "o1_inspect": "o1",
    "claude_3_7_sonnet_inspect": "Claude 3.7 Sonnet",
    "o3_inspect": "o3",
    "claude_4_opus_inspect": "Claude 4 Opus",
    "claude_4_1_opus_inspect": "Claude 4.1 Opus",
    "gpt_5_2025_08_07_inspect": "GPT-5",
    "gemini_3_pro": "Gemini 3 Pro",
    "gpt_5_1_codex_max_inspect": "GPT-5.1 Codex Max",
    "claude_opus_4_5_inspect": "Claude Opus 4.5",
    "gpt_5_2": "GPT-5.2 (high)",
    "claude_opus_4_6_inspect": "Claude\nOpus 4.6",
    "gpt_5_3_codex": "GPT-5.3 Codex",
}

LABEL_LAYOUTS = {
    "gpt2": (14, -10, "start"),
    "davinci_002": (-16, -10, "end"),
    "gpt_3_5_turbo_instruct": (-8, -12, "end"),
    "gpt_4": (-14, -10, "end"),
    "o3_inspect": (-18, -8, "end"),
    "gpt_5_2025_08_07_inspect": (-22, 18, "end"),
    "claude_opus_4_5_inspect": (-20, 2, "end"),
    "gpt_5_2": (-20, -18, "end"),
    "claude_opus_4_6_inspect": (20, 4, "start"),
    "orin_labs_harness": (18, 6, "start"),
}

TASK_LINES = [
    (1 * HOUR, "Fix bugs in small Python libraries"),
    (2.3 * HOUR, "Exploit a buffer overflow in libiec61850"),
    (4 * HOUR, "Train adversarially robust image model"),
    (8 * HOUR, "Exploit a vulnerable Ethereum smart contract"),
]

LOG_MAJOR_TICKS = [
    (1 * SECONDS, "1 sec"),
    (10 * SECONDS, "10 sec"),
    (1 * MINUTE, "1 min"),
    (10 * MINUTE, "10 min"),
    (30 * MINUTE, "30 min"),
    (1 * HOUR, "1 hour"),
    (3 * HOUR, "3 hours"),
    (10 * HOUR, "10 hours"),
    (1 * DAY, "1 day"),
    (3 * DAY, "3 days"),
    (1 * WEEK, "1 week"),
    (1 * MONTH, "1 month"),
    (3 * MONTH, "3 months"),
    (10 * MONTH, "10 months"),
]

LOG_MINOR_TICKS = [
    3 * SECONDS,
    3 * MINUTE,
    3 * HOUR,
    12 * HOUR,
    2 * DAY,
    2 * WEEK,
    2 * MONTH,
    6 * MONTH,
]

SUCCESS_METRICS = {
    "50": "p50_horizon_length",
    "80": "p80_horizon_length",
}


@dataclass(frozen=True)
class ModelPoint:
    key: str
    name: str
    release_date: date
    benchmark_name: str
    estimate_minutes: float
    ci_low_minutes: float
    ci_high_minutes: float
    is_sota: bool


def load_results(
    path: Path, metric_name: str = "p50_horizon_length"
) -> tuple[list[ModelPoint], float]:
    data = yaml.safe_load(path.read_text())
    doubling_days = data["doubling_time_in_days"]["from_2023_on"]["point_estimate"]
    results: list[ModelPoint] = []

    for key, item in data["results"].items():
        metric = item["metrics"].get(metric_name)
        if not metric:
            continue

        release_date = item["release_date"]
        if isinstance(release_date, str):
            release_date = date.fromisoformat(release_date)

        results.append(
            ModelPoint(
                key=key,
                name=DISPLAY_NAMES.get(key, key.replace("_", " ").title()),
                release_date=release_date,
                benchmark_name=item["benchmark_name"],
                estimate_minutes=float(metric["estimate"]),
                ci_low_minutes=float(metric["ci_low"]),
                ci_high_minutes=float(metric["ci_high"]),
                is_sota=bool(item["metrics"].get("is_sota")),
            )
        )

    results.sort(key=lambda point: point.release_date)
    return results, float(doubling_days)


def add_custom_points(points: Iterable[ModelPoint]) -> list[ModelPoint]:
    augmented = list(points)
    augmented.append(
        ModelPoint(
            key="orin_labs_harness",
            name="Orin Labs\nHarness",
            release_date=ORIN_LABS_RELEASE_DATE,
            benchmark_name="custom-overlay",
            estimate_minutes=10 * MONTH,
            ci_low_minutes=10 * MONTH,
            ci_high_minutes=10 * MONTH,
            is_sota=True,
        )
    )
    augmented.sort(key=lambda point: point.release_date)
    return augmented


def projection_frontier(points: Iterable[ModelPoint]) -> list[ModelPoint]:
    return [
        point
        for point in points
        if point.release_date.year >= 2023
        and point.is_sota
        and point.benchmark_name == "METR-Horizon-v1.1"
    ]


def fit_projection(
    points: Iterable[ModelPoint], doubling_days: float | None = None
) -> tuple[float, float]:
    frontier = [point for point in projection_frontier(points)]
    if doubling_days is None:
        xs = [point.release_date.toordinal() for point in frontier]
        ys = [math.log2(point.estimate_minutes) for point in frontier]
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / sum(
            (x - mean_x) ** 2 for x in xs
        )
    else:
        slope = 1.0 / doubling_days
    intercept = sum(
        math.log2(point.estimate_minutes) - point.release_date.toordinal() * slope
        for point in frontier
    ) / len(frontier)
    return slope, intercept


def projection_date_for_minutes(
    target_minutes: float, slope: float, intercept: float
) -> date:
    ordinal = round((math.log2(target_minutes) - intercept) / slope)
    return date.fromordinal(ordinal)


def predicted_minutes(day_value: date, slope: float, intercept: float) -> float:
    return 2 ** (intercept + day_value.toordinal() * slope)


def year_start(value: date) -> date:
    return date(value.year, 1, 1)


def x_position(value: date, start: date, end: date) -> float:
    span = end.toordinal() - start.toordinal()
    return PLOT_LEFT + (value.toordinal() - start.toordinal()) / span * PLOT_WIDTH


def log_y_position(
    value_minutes: float, min_minutes: float, max_minutes: float
) -> float:
    log_min = math.log10(min_minutes)
    log_max = math.log10(max_minutes)
    log_value = math.log10(value_minutes)
    fraction = (log_value - log_min) / (log_max - log_min)
    return PLOT_BOTTOM - fraction * PLOT_HEIGHT


def linear_y_position(value_minutes: float, max_minutes: float) -> float:
    fraction = value_minutes / max_minutes
    return PLOT_BOTTOM - fraction * PLOT_HEIGHT


def y_position(
    value_minutes: float, min_minutes: float, max_minutes: float, scale: str
) -> float:
    if scale == "linear":
        return linear_y_position(value_minutes, max_minutes)
    return log_y_position(value_minutes, min_minutes, max_minutes)


def build_ticks(
    scale: str, target_months: int
) -> tuple[list[tuple[float, str]], list[float]]:
    if scale == "linear":
        major = [(0.0, "0")]
        for month_index in range(1, target_months + 1):
            label = "1 month" if month_index == 1 else f"{month_index} months"
            major.append((month_index * MONTH, label))

        minor = [
            month_index * MONTH - (MONTH / 2)
            for month_index in range(1, target_months + 1)
        ]
        return major, minor

    return LOG_MAJOR_TICKS, LOG_MINOR_TICKS


def chart_end_with_padding(
    points: Iterable[ModelPoint],
    chart_start: date,
    projected_date: date,
    min_right_margin_px: float = 300.0,
    min_padding_days: int = 240,
) -> date:
    latest_release = max(point.release_date for point in points)
    base_end = max(latest_release, projected_date) + timedelta(days=min_padding_days)

    desired_fraction = (PLOT_WIDTH - min_right_margin_px) / PLOT_WIDTH
    projected_span = projected_date.toordinal() - chart_start.toordinal()
    required_total_span = math.ceil(projected_span / desired_fraction)
    required_end = chart_start + timedelta(days=required_total_span)
    return max(base_end, required_end)


def svg_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def build_text(
    x: float,
    y: float,
    text: str,
    css_class: str,
    anchor: str = "start",
) -> str:
    if "\n" not in text:
        return (
            f'<text x="{x:.2f}" y="{y:.2f}" class="{css_class}" '
            f'text-anchor="{anchor}">{svg_escape(text)}</text>'
        )

    lines = text.splitlines()
    line_height = 28
    parts = [
        f'<text x="{x:.2f}" y="{y:.2f}" class="{css_class}" text-anchor="{anchor}">'
    ]
    for index, line in enumerate(lines):
        dy = "0" if index == 0 else str(line_height)
        parts.append(f'<tspan x="{x:.2f}" dy="{dy}">{svg_escape(line)}</tspan>')
    parts.append("</text>")
    return "".join(parts)


def build_chart(
    points: list[ModelPoint],
    doubling_days: float,
    output_path: Path,
    target_months: int,
    scale: str,
    success_level: str,
) -> None:
    y_min_minutes = 1 * SECONDS
    target_minutes = target_months * MONTH
    y_max_minutes = target_minutes * 1.35

    projection_doubling_days = doubling_days if success_level == "50" else None
    slope, intercept = fit_projection(points, projection_doubling_days)
    projected_date = projection_date_for_minutes(target_minutes, slope, intercept)
    chart_start = date(2019, 1, 1)
    chart_end = chart_end_with_padding(points, chart_start, projected_date)
    major_ticks, minor_ticks = build_ticks(scale, target_months)

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{CANVAS_WIDTH}" height="{CANVAS_HEIGHT}" viewBox="0 0 {CANVAS_WIDTH} {CANVAS_HEIGHT}">',
        "<defs>",
        '<clipPath id="plot-clip">',
        f'<rect x="{PLOT_LEFT}" y="{PLOT_TOP}" width="{PLOT_WIDTH}" height="{PLOT_HEIGHT}" rx="18" ry="18" />',
        "</clipPath>",
        "<style>",
        ":root { color-scheme: light; }",
        f".bg {{ fill: {BACKGROUND}; }}",
        f".plot {{ fill: {PLOT_BACKGROUND}; }}",
        f".axis {{ stroke: {AXIS}; stroke-width: 2; }}",
        f".grid {{ stroke: {GRID}; stroke-width: 1.25; stroke-dasharray: 6 6; }}",
        f".minor-grid {{ stroke: {GRID}; stroke-width: 1; stroke-dasharray: 4 8; opacity: 0.7; }}",
        f".reference {{ stroke: {REFERENCE}; stroke-width: 1.5; }}",
        f".tick-label {{ fill: {AXIS}; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 19px; font-weight: 500; }}",
        f".x-label {{ fill: {AXIS}; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 19px; font-weight: 500; }}",
        f".model-label {{ fill: {TEXT}; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 20px; font-weight: 500; }}",
        f".reference-label {{ fill: {MUTED_TEXT}; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 18px; font-weight: 500; }}",
        f".caption {{ fill: {MUTED_TEXT}; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 18px; font-weight: 500; }}",
        "</style>",
        "</defs>",
        f'<rect class="bg" x="0" y="0" width="{CANVAS_WIDTH}" height="{CANVAS_HEIGHT}" rx="24" ry="24" />',
        f'<rect class="plot" x="{PLOT_LEFT}" y="{PLOT_TOP}" width="{PLOT_WIDTH}" height="{PLOT_HEIGHT}" rx="18" ry="18" />',
    ]

    for tick_value in minor_ticks:
        if y_min_minutes < tick_value <= target_minutes:
            y = y_position(tick_value, y_min_minutes, y_max_minutes, scale)
            svg.append(
                f'<line class="minor-grid" x1="{PLOT_LEFT}" y1="{y:.2f}" x2="{PLOT_RIGHT}" y2="{y:.2f}" />'
            )

    for tick_value, label in major_ticks:
        if scale == "linear":
            if not (0 <= tick_value <= target_minutes):
                continue
            y = y_position(tick_value, y_min_minutes, y_max_minutes, scale)
        elif not (y_min_minutes <= tick_value <= target_minutes):
            continue
        else:
            y = y_position(tick_value, y_min_minutes, y_max_minutes, scale)
        svg.append(
            f'<line class="grid" x1="{PLOT_LEFT}" y1="{y:.2f}" x2="{PLOT_RIGHT}" y2="{y:.2f}" />'
        )
        svg.append(
            f'<line class="axis" x1="{PLOT_LEFT - 12}" y1="{y:.2f}" x2="{PLOT_LEFT}" y2="{y:.2f}" />'
        )
        svg.append(
            f'<text x="{PLOT_LEFT - 18}" y="{y + 7:.2f}" class="tick-label" text-anchor="end">{svg_escape(label)}</text>'
        )

    year = chart_start.year + 1
    while date(year, 1, 1) <= chart_end:
        x = x_position(date(year, 1, 1), chart_start, chart_end)
        svg.append(
            f'<line class="grid" x1="{x:.2f}" y1="{PLOT_TOP}" x2="{x:.2f}" y2="{PLOT_BOTTOM}" />'
        )
        svg.append(
            f'<line class="axis" x1="{x:.2f}" y1="{PLOT_BOTTOM}" x2="{x:.2f}" y2="{PLOT_BOTTOM + 12}" />'
        )
        svg.append(
            f'<text x="{x:.2f}" y="{PLOT_BOTTOM + 40:.2f}" class="x-label" text-anchor="middle">{year}</text>'
        )
        year += 1

    if scale == "log":
        for task_minutes, task_label in TASK_LINES:
            y = y_position(task_minutes, y_min_minutes, y_max_minutes, scale)
            svg.append(
                f'<line class="reference" x1="{PLOT_LEFT}" y1="{y:.2f}" x2="{PLOT_LEFT + 18}" y2="{y:.2f}" />'
            )
            svg.append(build_text(PLOT_LEFT + 28, y + 6, task_label, "reference-label"))

    svg.append(
        f'<line class="axis" x1="{PLOT_LEFT}" y1="{PLOT_TOP}" x2="{PLOT_LEFT}" y2="{PLOT_BOTTOM}" />'
    )
    svg.append(
        f'<line class="axis" x1="{PLOT_LEFT}" y1="{PLOT_BOTTOM}" x2="{PLOT_RIGHT}" y2="{PLOT_BOTTOM}" />'
    )

    trend_start = date(2023, 1, 1)
    trend_points: list[tuple[float, float]] = []
    cursor = trend_start
    while cursor <= projected_date:
        trend_minutes = predicted_minutes(cursor, slope, intercept)
        trend_points.append(
            (
                x_position(cursor, chart_start, chart_end),
                y_position(trend_minutes, y_min_minutes, y_max_minutes, scale),
            )
        )
        cursor += timedelta(days=28)

    path_d = " ".join(
        f"{'M' if index == 0 else 'L'} {x:.2f} {y:.2f}"
        for index, (x, y) in enumerate(trend_points)
    )
    svg.append('<g clip-path="url(#plot-clip)">')
    svg.append(
        f'<path d="{path_d}" fill="none" stroke="{TREND}" stroke-width="4.5" stroke-dasharray="12 10" />'
    )

    projected_x = x_position(projected_date, chart_start, chart_end)
    projected_y = y_position(target_minutes, y_min_minutes, y_max_minutes, scale)
    svg.append(
        f'<line x1="{projected_x:.2f}" y1="{PLOT_TOP}" x2="{projected_x:.2f}" y2="{PLOT_BOTTOM}" '
        f'stroke="{TREND}" stroke-width="1.5" stroke-dasharray="4 8" opacity="0.8" />'
    )
    svg.append(
        f'<circle cx="{projected_x:.2f}" cy="{projected_y:.2f}" r="8" fill="{PLOT_BACKGROUND}" stroke="{TREND}" stroke-width="4" />'
    )
    svg.append(
        build_text(
            projected_x - 8,
            PLOT_TOP + 18,
            f"~ {projected_date.strftime('%b %Y')}",
            "caption",
            anchor="end",
        )
    )

    for point in points:
        color = GREEN if point.is_sota else GRAY
        light = GREEN_LIGHT if point.is_sota else GRAY_LIGHT
        x = x_position(point.release_date, chart_start, chart_end)
        y = y_position(point.estimate_minutes, y_min_minutes, y_max_minutes, scale)
        y_low = y_position(point.ci_low_minutes, y_min_minutes, y_max_minutes, scale)
        y_high = y_position(point.ci_high_minutes, y_min_minutes, y_max_minutes, scale)

        svg.append(
            f'<line x1="{x:.2f}" y1="{y_low:.2f}" x2="{x:.2f}" y2="{y_high:.2f}" '
            f'stroke="{light}" stroke-width="2.25" />'
        )
        svg.append(
            f'<line x1="{x - 9:.2f}" y1="{y_low:.2f}" x2="{x + 9:.2f}" y2="{y_low:.2f}" '
            f'stroke="{light}" stroke-width="2.25" />'
        )
        svg.append(
            f'<line x1="{x - 9:.2f}" y1="{y_high:.2f}" x2="{x + 9:.2f}" y2="{y_high:.2f}" '
            f'stroke="{light}" stroke-width="2.25" />'
        )
        svg.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="11" fill="{color}" stroke="{PLOT_BACKGROUND}" stroke-width="2" />'
        )
    svg.append("</g>")

    for point in points:
        x = x_position(point.release_date, chart_start, chart_end)
        y = y_position(point.estimate_minutes, y_min_minutes, y_max_minutes, scale)
        if point.key in LABEL_LAYOUTS:
            dx, dy, anchor = LABEL_LAYOUTS[point.key]
            svg.append(
                build_text(x + dx, y + dy, point.name, "model-label", anchor=anchor)
            )

    svg.append(
        build_text(
            PLOT_LEFT,
            CANVAS_HEIGHT - 26,
            f"Scale: {'Log Scale' if scale == 'log' else 'Linear Scale'} | Success: {success_level}%",
            "caption",
        )
    )

    svg.append("</svg>")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("".join(svg))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a METR-style log-scale time horizon chart from benchmark_results_1_1.yaml."
    )
    parser.add_argument(
        "--input", required=True, type=Path, help="Path to benchmark_results_1_1.yaml"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output SVG path",
    )
    parser.add_argument(
        "--success-level",
        choices=tuple(SUCCESS_METRICS.keys()),
        default="50",
        help="Benchmark success threshold to render",
    )
    parser.add_argument(
        "--target-months",
        default=10,
        type=int,
        help="Maximum projected horizon shown on the chart",
    )
    parser.add_argument(
        "--scale",
        choices=("log", "linear"),
        default="log",
        help="Y-axis scale to render",
    )
    return parser.parse_args()


def default_output_path(scale: str, target_months: int, success_level: str) -> Path:
    return Path(
        f"output/time-horizon-{scale}-{success_level}pct-{target_months}-months.svg"
    )


def main() -> None:
    args = parse_args()
    metric_name = SUCCESS_METRICS[args.success_level]
    points, doubling_days = load_results(args.input, metric_name=metric_name)
    points = add_custom_points(points)
    output_path = args.output or default_output_path(
        args.scale, args.target_months, args.success_level
    )
    build_chart(
        points,
        doubling_days,
        output_path,
        args.target_months,
        args.scale,
        args.success_level,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
