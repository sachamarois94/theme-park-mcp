"""
Historical wait time storage and analysis.
Uses PostgreSQL for cloud deployment, falls back to SQLite for local.
"""

import os
import json
from datetime import datetime
from typing import Optional
from contextlib import contextmanager

# Check if we're using PostgreSQL (cloud) or SQLite (local)
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    # PostgreSQL mode
    import psycopg2
    from psycopg2.extras import RealDictCursor
    USE_POSTGRES = True
else:
    # SQLite mode (local development)
    import sqlite3
    from pathlib import Path
    USE_POSTGRES = False
    DEFAULT_DB_PATH = Path.home() / ".theme_park_mcp" / "history.db"


@contextmanager
def get_db_connection():
    """Get a database connection."""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            yield conn
        finally:
            conn.close()
    else:
        db_path = DEFAULT_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()


def init_database():
    """Create tables if they don't exist."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if USE_POSTGRES:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wait_times (
                    id SERIAL PRIMARY KEY,
                    park_id INTEGER NOT NULL,
                    ride_id INTEGER NOT NULL,
                    ride_name TEXT NOT NULL,
                    land_name TEXT,
                    wait_minutes INTEGER,
                    is_open BOOLEAN NOT NULL,
                    recorded_at TIMESTAMP NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    hour_of_day INTEGER NOT NULL
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_wait_times_park_ride 
                ON wait_times(park_id, ride_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_wait_times_recorded 
                ON wait_times(recorded_at)
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS collection_log (
                    id SERIAL PRIMARY KEY,
                    collected_at TIMESTAMP NOT NULL,
                    parks_collected TEXT NOT NULL,
                    total_rides INTEGER NOT NULL,
                    success BOOLEAN NOT NULL,
                    error_message TEXT
                )
            """)
        else:
            cursor.executescript("""
                CREATE TABLE IF NOT EXISTS wait_times (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    park_id INTEGER NOT NULL,
                    ride_id INTEGER NOT NULL,
                    ride_name TEXT NOT NULL,
                    land_name TEXT,
                    wait_minutes INTEGER,
                    is_open BOOLEAN NOT NULL,
                    recorded_at TIMESTAMP NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    hour_of_day INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wait_times_park_ride 
                    ON wait_times(park_id, ride_id);
                CREATE INDEX IF NOT EXISTS idx_wait_times_recorded 
                    ON wait_times(recorded_at);
                CREATE TABLE IF NOT EXISTS collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collected_at TIMESTAMP NOT NULL,
                    parks_collected TEXT NOT NULL,
                    total_rides INTEGER NOT NULL,
                    success BOOLEAN NOT NULL,
                    error_message TEXT
                );
            """)
        
        conn.commit()


def record_wait_times(park_id: int, rides: list[dict]) -> int:
    """Record current wait times to the database."""
    now = datetime.now()
    day_of_week = now.weekday()
    hour_of_day = now.hour
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        count = 0
        for ride in rides:
            if USE_POSTGRES:
                cursor.execute("""
                    INSERT INTO wait_times 
                    (park_id, ride_id, ride_name, land_name, wait_minutes, is_open, 
                     recorded_at, day_of_week, hour_of_day)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    park_id,
                    ride.get("id", 0),
                    ride["name"],
                    ride.get("land"),
                    ride.get("wait_time"),
                    ride.get("is_open", True),
                    now,
                    day_of_week,
                    hour_of_day
                ))
            else:
                cursor.execute("""
                    INSERT INTO wait_times 
                    (park_id, ride_id, ride_name, land_name, wait_minutes, is_open, 
                     recorded_at, day_of_week, hour_of_day)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    park_id,
                    ride.get("id", 0),
                    ride["name"],
                    ride.get("land"),
                    ride.get("wait_time"),
                    ride.get("is_open", True),
                    now.isoformat(),
                    day_of_week,
                    hour_of_day
                ))
            count += 1
        
        conn.commit()
    
    return count


def get_historical_average(
    park_id: int,
    ride_name: str,
    day_of_week: Optional[int] = None,
    hour_of_day: Optional[int] = None
) -> Optional[dict]:
    """Get historical average wait time for a ride."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if USE_POSTGRES:
            query = """
                SELECT 
                    AVG(wait_minutes) as avg_wait,
                    MIN(wait_minutes) as min_wait,
                    MAX(wait_minutes) as max_wait,
                    COUNT(*) as sample_count
                FROM wait_times
                WHERE park_id = %s
                AND LOWER(ride_name) LIKE %s
                AND is_open = true
                AND wait_minutes > 0
            """
            params = [park_id, f"%{ride_name.lower()}%"]
            
            if day_of_week is not None:
                query += " AND day_of_week = %s"
                params.append(day_of_week)
            
            if hour_of_day is not None:
                query += " AND hour_of_day = %s"
                params.append(hour_of_day)
        else:
            query = """
                SELECT 
                    AVG(wait_minutes) as avg_wait,
                    MIN(wait_minutes) as min_wait,
                    MAX(wait_minutes) as max_wait,
                    COUNT(*) as sample_count
                FROM wait_times
                WHERE park_id = ?
                AND LOWER(ride_name) LIKE ?
                AND is_open = 1
                AND wait_minutes > 0
            """
            params = [park_id, f"%{ride_name.lower()}%"]
            
            if day_of_week is not None:
                query += " AND day_of_week = ?"
                params.append(day_of_week)
            
            if hour_of_day is not None:
                query += " AND hour_of_day = ?"
                params.append(hour_of_day)
        
        cursor.execute(query, params)
        row = cursor.fetchone()
        
        if USE_POSTGRES:
            if row and row[3] > 0:  # sample_count > 0
                return {
                    "average": round(float(row[0]), 1),
                    "min": row[1],
                    "max": row[2],
                    "sample_count": row[3]
                }
        else:
            if row and row["sample_count"] > 0:
                return {
                    "average": round(row["avg_wait"], 1),
                    "min": row["min_wait"],
                    "max": row["max_wait"],
                    "sample_count": row["sample_count"]
                }
    
    return None


def get_ride_averages_for_park(
    park_id: int,
    day_of_week: Optional[int] = None,
    hour_of_day: Optional[int] = None
) -> dict[str, dict]:
    """Get historical averages for all rides in a park."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if USE_POSTGRES:
            query = """
                SELECT 
                    ride_name,
                    AVG(wait_minutes) as avg_wait,
                    MIN(wait_minutes) as min_wait,
                    MAX(wait_minutes) as max_wait,
                    COUNT(*) as sample_count
                FROM wait_times
                WHERE park_id = %s
                AND is_open = true
                AND wait_minutes > 0
            """
            params = [park_id]
            
            if day_of_week is not None:
                query += " AND day_of_week = %s"
                params.append(day_of_week)
            
            if hour_of_day is not None:
                query += " AND hour_of_day = %s"
                params.append(hour_of_day)
            
            query += " GROUP BY ride_name"
            cursor.execute(query, params)
            
            result = {}
            for row in cursor.fetchall():
                result[row[0]] = {
                    "average": round(float(row[1]), 1),
                    "min": row[2],
                    "max": row[3],
                    "sample_count": row[4]
                }
        else:
            query = """
                SELECT 
                    ride_name,
                    AVG(wait_minutes) as avg_wait,
                    MIN(wait_minutes) as min_wait,
                    MAX(wait_minutes) as max_wait,
                    COUNT(*) as sample_count
                FROM wait_times
                WHERE park_id = ?
                AND is_open = 1
                AND wait_minutes > 0
            """
            params = [park_id]
            
            if day_of_week is not None:
                query += " AND day_of_week = ?"
                params.append(day_of_week)
            
            if hour_of_day is not None:
                query += " AND hour_of_day = ?"
                params.append(hour_of_day)
            
            query += " GROUP BY ride_name"
            cursor.execute(query, params)
            
            result = {}
            for row in cursor.fetchall():
                result[row["ride_name"]] = {
                    "average": round(row["avg_wait"], 1),
                    "min": row["min_wait"],
                    "max": row["max_wait"],
                    "sample_count": row["sample_count"]
                }
    
    return result


def compare_to_average(current_wait: int, average: float) -> dict:
    """Compare current wait to historical average."""
    if average == 0:
        return {
            "difference": current_wait,
            "percent_diff": None,
            "status": "no_baseline"
        }
    
    difference = current_wait - average
    percent_diff = round((difference / average) * 100, 1)
    
    if percent_diff <= -20:
        status = "much_lower"
    elif percent_diff <= -10:
        status = "lower"
    elif percent_diff <= 10:
        status = "typical"
    elif percent_diff <= 20:
        status = "higher"
    else:
        status = "much_higher"
    
    return {
        "difference": round(difference, 1),
        "percent_diff": percent_diff,
        "status": status
    }


def format_comparison(current: int, comparison: dict) -> str:
    """Format a comparison for display."""
    if comparison["status"] == "no_baseline":
        return ""
    
    percent = comparison["percent_diff"]
    if percent > 0:
        return f"↑ {percent}% above average"
    elif percent < 0:
        return f"↓ {abs(percent)}% below average"
    else:
        return "→ At average"


def get_database_stats() -> dict:
    """Get statistics about the historical database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if USE_POSTGRES:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT park_id) as parks,
                    COUNT(DISTINCT ride_name) as rides,
                    MIN(recorded_at) as earliest,
                    MAX(recorded_at) as latest
                FROM wait_times
            """)
            row = cursor.fetchone()
            return {
                "total_records": row[0],
                "parks_tracked": row[1],
                "rides_tracked": row[2],
                "earliest_record": str(row[3]) if row[3] else None,
                "latest_record": str(row[4]) if row[4] else None
            }
        else:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT park_id) as parks,
                    COUNT(DISTINCT ride_name) as rides,
                    MIN(recorded_at) as earliest,
                    MAX(recorded_at) as latest
                FROM wait_times
            """)
            row = cursor.fetchone()
            return {
                "total_records": row["total_records"],
                "parks_tracked": row["parks"],
                "rides_tracked": row["rides"],
                "earliest_record": row["earliest"],
                "latest_record": row["latest"]
            }


def log_collection(
    parks_collected: list[int],
    total_rides: int,
    success: bool,
    error_message: Optional[str] = None
):
    """Log a data collection run."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if USE_POSTGRES:
            cursor.execute("""
                INSERT INTO collection_log 
                (collected_at, parks_collected, total_rides, success, error_message)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                datetime.now(),
                json.dumps(parks_collected),
                total_rides,
                success,
                error_message
            ))
        else:
            cursor.execute("""
                INSERT INTO collection_log 
                (collected_at, parks_collected, total_rides, success, error_message)
                VALUES (?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                json.dumps(parks_collected),
                total_rides,
                success,
                error_message
            ))
        
        conn.commit()
