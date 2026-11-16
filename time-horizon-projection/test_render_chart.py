from __future__ import annotations

import importlib.util
import sys
import tempfile
import textwrap
import unittest
from datetime import date
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "render_chart", TOOL_DIR / "render_chart.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RenderChartTests(unittest.TestCase):
    def test_load_results_sorts_and_extracts_doubling_time(self) -> None:
        yaml_text = textwrap.dedent(
            """
            doubling_time_in_days:
              from_2023_on:
                point_estimate: 128.744
            results:
              later_model:
                benchmark_name: METR-Horizon-v1.1
                release_date: 2024-01-01
                metrics:
                  is_sota: false
                  p50_horizon_length:
                    estimate: 3.0
                    ci_low: 2.0
                    ci_high: 5.0
              earlier_model:
                benchmark_name: METR-Horizon-v1.0
                release_date: 2023-01-01
                metrics:
                  is_sota: true
                  p50_horizon_length:
                    estimate: 1.0
                    ci_low: 0.5
                    ci_high: 2.0
            """
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = Path(temp_dir) / "benchmark_results_1_1.yaml"
            yaml_path.write_text(yaml_text)
            points, doubling_days = MODULE.load_results(yaml_path)

        self.assertEqual(doubling_days, 128.744)
        self.assertEqual(
            [point.key for point in points], ["earlier_model", "later_model"]
        )
        self.assertEqual(points[0].release_date, date(2023, 1, 1))
        self.assertFalse(points[1].is_sota)

    def test_load_results_supports_p80_metric(self) -> None:
        yaml_text = textwrap.dedent(
            """
            doubling_time_in_days:
              from_2023_on:
                point_estimate: 128.744
            results:
              model:
                benchmark_name: METR-Horizon-v1.1
                release_date: 2024-01-01
                metrics:
                  is_sota: true
                  p50_horizon_length:
                    estimate: 30.0
                    ci_low: 20.0
                    ci_high: 40.0
                  p80_horizon_length:
                    estimate: 10.0
                    ci_low: 5.0
                    ci_high: 15.0
            """
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = Path(temp_dir) / "benchmark_results_1_1.yaml"
            yaml_path.write_text(yaml_text)
            points, _ = MODULE.load_results(yaml_path, metric_name="p80_horizon_length")

        self.assertEqual(points[0].estimate_minutes, 10.0)
        self.assertEqual(points[0].ci_low_minutes, 5.0)
        self.assertEqual(points[0].ci_high_minutes, 15.0)

    def test_projection_respects_fixed_doubling_time(self) -> None:
        points = [
            MODULE.ModelPoint(
                "first",
                "First",
                date(2023, 1, 1),
                "METR-Horizon-v1.1",
                10.0,
                8.0,
                12.0,
                True,
            ),
            MODULE.ModelPoint(
                "second",
                "Second",
                date(2023, 5, 1),
                "METR-Horizon-v1.1",
                20.0,
                18.0,
                22.0,
                True,
            ),
            MODULE.ModelPoint(
                "third",
                "Third",
                date(2023, 8, 29),
                "METR-Horizon-v1.1",
                40.0,
                35.0,
                45.0,
                True,
            ),
        ]

        slope, intercept = MODULE.fit_projection(points, 120.0)
        projected = MODULE.predicted_minutes(date(2023, 12, 27), slope, intercept)

        self.assertAlmostEqual(projected, 80.0, delta=2.0)

    def test_projection_target_date_moves_forward(self) -> None:
        points = [
            MODULE.ModelPoint(
                "first",
                "First",
                date(2023, 1, 1),
                "METR-Horizon-v1.1",
                10.0,
                8.0,
                12.0,
                True,
            ),
            MODULE.ModelPoint(
                "second",
                "Second",
                date(2023, 5, 1),
                "METR-Horizon-v1.1",
                20.0,
                18.0,
                22.0,
                True,
            ),
            MODULE.ModelPoint(
                "third",
                "Third",
                date(2023, 8, 29),
                "METR-Horizon-v1.1",
                40.0,
                35.0,
                45.0,
                True,
            ),
        ]

        slope, intercept = MODULE.fit_projection(points, 120.0)
        target_date = MODULE.projection_date_for_minutes(160.0, slope, intercept)

        self.assertGreater(target_date, date(2023, 8, 29))

    def test_add_custom_points_adds_orin_overlay_without_reordering(self) -> None:
        points = [
            MODULE.ModelPoint(
                "first",
                "First",
                date(2025, 1, 1),
                "METR-Horizon-v1.1",
                10.0,
                8.0,
                12.0,
                True,
            ),
        ]

        augmented = MODULE.add_custom_points(points)

        self.assertEqual(augmented[0].key, "first")
        self.assertEqual(augmented[1].key, "orin_labs_harness")
        self.assertEqual(augmented[1].release_date, date(2026, 5, 15))
        self.assertEqual(augmented[1].estimate_minutes, 10 * MODULE.MONTH)
        self.assertTrue(augmented[1].is_sota)

    def test_build_ticks_for_linear_scale_includes_zero_and_target(self) -> None:
        major, minor = MODULE.build_ticks("linear", 10)

        self.assertEqual(major[0], (0.0, "0"))
        self.assertEqual(major[-1], (10 * MODULE.MONTH, "10 months"))
        self.assertIn(0.5 * MODULE.MONTH, minor)

    def test_chart_end_with_padding_keeps_projection_inside_plot(self) -> None:
        points = [
            MODULE.ModelPoint(
                "first",
                "First",
                date(2026, 9, 1),
                "custom-overlay",
                10.0,
                10.0,
                10.0,
                True,
            ),
        ]

        chart_start = date(2019, 1, 1)
        projected_date = date(2029, 8, 18)
        chart_end = MODULE.chart_end_with_padding(
            points, chart_start, projected_date, min_right_margin_px=300.0
        )
        projected_x = MODULE.x_position(projected_date, chart_start, chart_end)

        self.assertLessEqual(projected_x, MODULE.PLOT_RIGHT - 300.0)
        self.assertGreater(chart_end, projected_date)

    def test_default_output_path_includes_success_level(self) -> None:
        path = MODULE.default_output_path("log", 10, "80")

        self.assertEqual(path, Path("output/time-horizon-log-80pct-10-months.svg"))


if __name__ == "__main__":
    unittest.main()
