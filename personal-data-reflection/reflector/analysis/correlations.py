"""Correlation analysis between metrics."""

import duckdb
from typing import List, Dict, Tuple
from datetime import datetime, timedelta


class CorrelationAnalyzer:
    """Analyze correlations between different health metrics."""

    # Default metric pairs to analyze
    DEFAULT_PAIRS = [
        ('sleep_hours', 'resting_heart_rate'),
        ('sleep_hours', 'steps'),
        ('sleep_hours', 'active_energy_kcal'),
        ('steps', 'active_energy_kcal'),
        ('exercise_minutes', 'sleep_hours'),
        ('hrv_sdnn', 'sleep_hours'),
        ('steps', 'resting_heart_rate'),
    ]

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize analyzer with database connection."""
        self.con = db_connection

    def compute_correlations(
        self,
        start_date: str,
        end_date: str,
        metric_pairs: List[Tuple[str, str]] = None
    ) -> List[Dict]:
        """Compute correlations for specified metric pairs.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            metric_pairs: List of (metric_a, metric_b) tuples. Uses defaults if None.

        Returns:
            List of correlation results
        """
        if metric_pairs is None:
            metric_pairs = self.DEFAULT_PAIRS

        results = []

        for metric_a, metric_b in metric_pairs:
            correlation = self._compute_correlation(metric_a, metric_b, start_date, end_date)
            if correlation:
                results.append(correlation)
                self._save_correlation(correlation)

        return results

    def _compute_correlation(
        self,
        metric_a: str,
        metric_b: str,
        start_date: str,
        end_date: str
    ) -> Dict:
        """Compute correlation between two metrics."""
        try:
            # Query to get correlation using DuckDB's CORR function
            result = self.con.execute(f"""
                SELECT
                    CORR({metric_a}, {metric_b}) as correlation,
                    COUNT(*) as sample_size,
                    AVG({metric_a}) as avg_a,
                    AVG({metric_b}) as avg_b,
                    STDDEV({metric_a}) as std_a,
                    STDDEV({metric_b}) as std_b
                FROM health_metrics
                WHERE date BETWEEN ? AND ?
                  AND {metric_a} IS NOT NULL
                  AND {metric_b} IS NOT NULL
            """, [start_date, end_date]).fetchone()

            if not result or result[0] is None:
                return None

            corr, sample_size, avg_a, avg_b, std_a, std_b = result

            # Calculate approximate p-value (simplified - would need scipy for exact)
            # Using rough approximation: correlation is significant if |r| > 2/sqrt(n)
            significance_threshold = 2 / (sample_size ** 0.5) if sample_size > 0 else 1
            is_significant = abs(corr) > significance_threshold
            p_value = 0.05 if is_significant else 0.5  # Rough approximation

            # Generate description
            strength = self._describe_correlation_strength(corr)
            direction = "positive" if corr > 0 else "negative"

            description = f"{strength.capitalize()} {direction} correlation between {metric_a.replace('_', ' ')} and {metric_b.replace('_', ' ')}"

            if abs(corr) > 0.3:
                # Add practical interpretation
                if metric_a == 'sleep_hours' and metric_b == 'resting_heart_rate':
                    if corr < 0:
                        description += f". More sleep is associated with lower resting heart rate (avg: {avg_b:.1f} bpm)."
                elif metric_a == 'sleep_hours' and metric_b == 'steps':
                    if corr > 0:
                        description += f". Days with more sleep tend to have more steps (avg: {avg_b:.0f} steps)."
                elif metric_a == 'steps' and metric_b == 'active_energy_kcal':
                    if corr > 0:
                        description += f". More steps correlate with higher active energy (avg: {avg_b:.0f} kcal)."

            return {
                "metric_a": metric_a,
                "metric_b": metric_b,
                "correlation": round(corr, 3),
                "p_value": p_value,
                "sample_size": sample_size,
                "period_start": start_date,
                "period_end": end_date,
                "description": description,
                "strength": strength,
                "is_significant": is_significant,
                "avg_a": round(avg_a, 2) if avg_a else None,
                "avg_b": round(avg_b, 2) if avg_b else None,
            }

        except Exception as e:
            print(f"Error computing correlation for {metric_a} vs {metric_b}: {e}")
            return None

    def _describe_correlation_strength(self, r: float) -> str:
        """Describe the strength of a correlation coefficient."""
        abs_r = abs(r)
        if abs_r >= 0.7:
            return "strong"
        elif abs_r >= 0.5:
            return "moderate"
        elif abs_r >= 0.3:
            return "weak"
        else:
            return "very weak"

    def _save_correlation(self, correlation: Dict):
        """Save correlation result to database."""
        try:
            self.con.execute("""
                INSERT OR REPLACE INTO correlations (
                    metric_a, metric_b, period_start, period_end,
                    correlation_coefficient, p_value, sample_size, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                correlation['metric_a'],
                correlation['metric_b'],
                correlation['period_start'],
                correlation['period_end'],
                correlation['correlation'],
                correlation['p_value'],
                correlation['sample_size'],
                correlation['description']
            ])
        except Exception as e:
            print(f"Error saving correlation: {e}")

    def get_lagged_correlation(
        self,
        metric_a: str,
        metric_b: str,
        lag_days: int,
        start_date: str,
        end_date: str
    ) -> Dict:
        """Compute correlation with a time lag.

        Example: Does sleep today correlate with steps tomorrow?

        Args:
            metric_a: First metric
            metric_b: Second metric
            lag_days: Number of days to lag metric_b
            start_date: Start date
            end_date: End date

        Returns:
            Correlation result dict
        """
        try:
            result = self.con.execute(f"""
                SELECT
                    CORR(a.{metric_a}, b.{metric_b}) as correlation,
                    COUNT(*) as sample_size
                FROM health_metrics a
                JOIN health_metrics b ON b.date = a.date + INTERVAL '{lag_days} days'
                WHERE a.date BETWEEN ? AND ?
                  AND a.{metric_a} IS NOT NULL
                  AND b.{metric_b} IS NOT NULL
            """, [start_date, end_date]).fetchone()

            if not result or result[0] is None:
                return None

            corr, sample_size = result
            strength = self._describe_correlation_strength(corr)

            return {
                "metric_a": metric_a,
                "metric_b": f"{metric_b} (+{lag_days}d)",
                "correlation": round(corr, 3),
                "sample_size": sample_size,
                "lag_days": lag_days,
                "strength": strength,
                "description": f"{strength.capitalize()} correlation between {metric_a} and {metric_b} {lag_days} day(s) later"
            }

        except Exception as e:
            print(f"Error computing lagged correlation: {e}")
            return None

    def find_strongest_correlations(
        self,
        start_date: str,
        end_date: str,
        min_correlation: float = 0.3,
        limit: int = 10
    ) -> List[Dict]:
        """Find the strongest correlations in the data.

        Args:
            start_date: Start date
            end_date: End date
            min_correlation: Minimum absolute correlation to include
            limit: Maximum number of results

        Returns:
            List of top correlations
        """
        # List of all metrics to check
        metrics = [
            'steps', 'distance_km', 'active_energy_kcal', 'exercise_minutes',
            'sleep_hours', 'resting_heart_rate', 'hrv_sdnn'
        ]

        all_correlations = []

        # Check all pairs
        for i, metric_a in enumerate(metrics):
            for metric_b in metrics[i+1:]:  # Avoid duplicates
                corr = self._compute_correlation(metric_a, metric_b, start_date, end_date)
                if corr and abs(corr['correlation']) >= min_correlation:
                    all_correlations.append(corr)

        # Sort by absolute correlation strength
        all_correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)

        return all_correlations[:limit]
