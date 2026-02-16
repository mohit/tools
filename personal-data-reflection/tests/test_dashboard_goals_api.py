"""Regression tests for dashboard goals API validation."""

import os
import tempfile
import unittest

from reflector.dashboard.app import create_app


class DashboardGoalsApiTest(unittest.TestCase):
    """Validate PUT /api/goals payload handling."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".duckdb")
        os.close(fd)
        os.remove(self.db_path)  # DuckDB needs to create the file itself
        self.app = create_app(self.db_path)
        self.client = self.app.test_client()

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_put_goals_rejects_non_numeric_target(self):
        response = self.client.put(
            "/api/goals",
            json={"metric": "steps", "target": "abc", "period": "daily"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.get_json(),
            {
                "error": {
                    "code": "invalid_target",
                    "message": "Target must be a number",
                }
            },
        )

    def test_put_goals_accepts_numeric_target(self):
        response = self.client.put(
            "/api/goals",
            json={"metric": "steps", "target": "12000", "period": "daily"},
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("steps", data)
        self.assertEqual(data["steps"]["target"], 12000.0)
        self.assertEqual(data["steps"]["period"], "daily")


if __name__ == "__main__":
    unittest.main()
