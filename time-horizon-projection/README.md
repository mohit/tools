# Time Horizon Projection

Small Python tool that reads `benchmark_results_1_1.yaml` and renders METR-style charts with:

- Real model points and confidence intervals from the YAML
- Green frontier points and gray non-frontier points from `is_sota`
- A dashed projection using the YAML's `from_2023_on` doubling-time estimate for `50%` success, or a fit from the `80%` frontier points for `80%` success
- A manual `Orin Labs Harness` overlay on May 15, 2026 at a `10 months` horizon
- A configurable top-of-chart target, defaulting to `10 months`
- Both `log` and `linear` y-axis modes
- Both `50%` and `80%` success thresholds

## Requirements

- Python 3.10+
- `PyYAML`

## Installation

```bash
cd time-horizon-projection
python3 -m pip install -r requirements.txt
```

## Usage

Render the log-scale chart:

```bash
cd time-horizon-projection
python3 render_chart.py \
  --input /Users/mohit/Downloads/benchmark_results_1_1.yaml \
  --output output/time-horizon-log-10-months.svg \
  --target-months 10 \
  --success-level 50 \
  --scale log
```

Render the linear-scale chart:

```bash
cd time-horizon-projection
python3 render_chart.py \
  --input /Users/mohit/Downloads/benchmark_results_1_1.yaml \
  --output output/time-horizon-linear-10-months.svg \
  --target-months 10 \
  --success-level 50 \
  --scale linear
```

Render the 80% success versions:

```bash
cd time-horizon-projection
python3 render_chart.py \
  --input /Users/mohit/Downloads/benchmark_results_1_1.yaml \
  --target-months 10 \
  --success-level 80 \
  --scale log

python3 render_chart.py \
  --input /Users/mohit/Downloads/benchmark_results_1_1.yaml \
  --target-months 10 \
  --success-level 80 \
  --scale linear
```

## Testing

```bash
cd time-horizon-projection
python3 -m unittest test_render_chart.py
```

## Output

Current generated output paths:

`time-horizon-projection/output/time-horizon-log-50pct-10-months.svg`
`time-horizon-projection/output/time-horizon-linear-50pct-10-months.svg`
`time-horizon-projection/output/time-horizon-log-80pct-10-months.svg`
`time-horizon-projection/output/time-horizon-linear-80pct-10-months.svg`
