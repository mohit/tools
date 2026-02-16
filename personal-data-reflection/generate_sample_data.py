import random
from datetime import datetime, timedelta
from pathlib import Path

import duckdb


def generate_sample_data(db_path="./data/reflection.duckdb"):
    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS health_metrics (
            date DATE PRIMARY KEY,
            steps INTEGER,
            distance_km DOUBLE,
            active_energy_kcal DOUBLE,
            resting_energy_kcal DOUBLE,
            exercise_minutes DOUBLE,
            flights_climbed INTEGER,
            resting_heart_rate DOUBLE,
            walking_heart_rate DOUBLE,
            hrv_sdnn DOUBLE,
            sleep_hours DOUBLE,
            sleep_quality VARCHAR,
            body_mass_kg DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS workouts (
            id VARCHAR PRIMARY KEY,
            source VARCHAR,
            workout_type VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_minutes DOUBLE,
            distance_km DOUBLE,
            elevation_gain_m DOUBLE,
            calories DOUBLE,
            avg_heart_rate DOUBLE,
            max_heart_rate DOUBLE,
            avg_pace_min_per_km DOUBLE,
            avg_speed_kmh DOUBLE,
            avg_power_watts DOUBLE,
            metadata JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Generate 60 days of data
    end_date = datetime.now().date()

    print("Generating sample data for 60 days...")

    for i in range(60):
        d = end_date - timedelta(days=i)
        date_str = d.strftime('%Y-%m-%d')

        # Health Metrics
        steps = random.randint(2000, 15000)
        sleep = random.uniform(5.5, 9.0)
        rhr = random.uniform(55, 75)
        hrv = random.uniform(30, 80)

        con.execute("""
            INSERT OR REPLACE INTO health_metrics
            (date, steps, distance_km, active_energy_kcal, resting_energy_kcal, exercise_minutes,
             resting_heart_rate, hrv_sdnn, sleep_hours, sleep_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            date_str, steps, steps * 0.0008, steps * 0.04, 1800,
            random.randint(0, 60), rhr, hrv, sleep,
            'good' if sleep > 7 else 'fair'
        ])

        # Workouts (occasional)
        if random.random() > 0.6:
            workout_id = f"sample_{date_str}"
            con.execute("""
                INSERT OR REPLACE INTO workouts
                (id, source, workout_type, start_time, duration_minutes, distance_km, calories)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                workout_id, 'sample', 'Run' if random.random() > 0.5 else 'Walk',
                f"{date_str} 08:00:00", 30 + random.randint(0, 60),
                3 + random.random() * 5, 200 + random.randint(0, 400)
            ])

    # Rebuild daily summary
    con.execute("DROP TABLE IF EXISTS daily_summary")
    con.execute("""
        CREATE TABLE daily_summary AS
        SELECT
            hm.date,
            hm.steps as total_steps,
            hm.distance_km as total_distance_km,
            hm.active_energy_kcal as total_active_energy,
            COALESCE(w.workout_count, 0) as workout_count,
            COALESCE(w.total_duration, 0) as workout_duration_minutes,
            hm.sleep_hours,
            hm.resting_heart_rate as avg_resting_hr,
            hm.hrv_sdnn as avg_hrv,
            DAYOFWEEK(hm.date) - 1 as day_of_week,
            DAYOFWEEK(hm.date) IN (1, 7) as is_weekend,
            NULL as mood_score,
            CASE
                WHEN hm.active_energy_kcal > 500 THEN 'high'
                WHEN hm.active_energy_kcal > 300 THEN 'medium'
                ELSE 'low'
            END as energy_level,
            NULL as notes,
            CURRENT_TIMESTAMP as created_at
        FROM health_metrics hm
        LEFT JOIN (
            SELECT
                CAST(start_time AS DATE) as workout_date,
                COUNT(*) as workout_count,
                SUM(duration_minutes) as total_duration
            FROM workouts
            GROUP BY CAST(start_time AS DATE)
        ) w ON hm.date = w.workout_date
    """)

    con.close()
    print("Sample data generated successfully!")

if __name__ == "__main__":
    generate_sample_data()
