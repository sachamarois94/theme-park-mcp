"""
Historical wait time storage and analysis.
Uses SQLite for persistence.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import json


# Default database location
DEFAULT_DB_PATH = Path.home() / ".theme_park_mcp" / "history.db"


def get_db_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a database connection, creating tables if needed."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    # Ensure directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _init_tables(conn)
    return conn


def _init_tables(conn: sqlite3.Connection):
    """Create tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS wait_times (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            park_id INTEGER NOT NULL,
            ride_id INTEGER NOT NULL,
            ride_name TEXT NOT NULL,
            land_name TEXT,
            wait_minutes INTEGER,
            is_open BOOLEAN NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            day_of_week INTEGER NOT NULL,  -- 0=Monday, 6=Sunday
            hour_of_day INTEGER NOT NULL   -- 0-23
        );
        
        CREATE INDEX IF NOT EXISTS idx_wait_times_park_ride 
            ON wait_times(park_id, ride_id);
        CREATE INDEX IF NOT EXISTS idx_wait_times_recorded 
            ON wait_times(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_wait_times_day_hour 
            ON wait_times(day_of_week, hour_of_day);
            
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TIMESTAMP NOT NULL,
            parks_collected TEXT NOT NULL,  -- JSON array of park IDs
            total_rides INTEGER NOT NULL,
            success BOOLEAN NOT NULL,
            error_message TEXT
        );
    """)
    conn.commit()


def record_wait_times(
    park_id: int,
    rides: list[dict],
    conn: Optional[sqlite3.Connection] = None
) -> int:
    """
    Record current wait times to the database.
    
    Args:
        park_id: The Queue-Times park ID
        rides: List of ride dicts from parse_wait_times()
        conn: Optional database connection
        
    Returns:
        Number of records inserted
    """
    should_close = conn is None
    if conn is None:
        conn = get_db_connection()
    
    now = datetime.now()
    day_of_week = now.weekday()  # 0=Monday
    hour_of_day = now.hour
    
    records = []
    for ride in rides:
        records.append((
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
    
    conn.executemany("""
        INSERT INTO wait_times 
        (park_id, ride_id, ride_name, land_name, wait_minutes, is_open, 
         recorded_at, day_of_week, hour_of_day)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    
    if should_close:
        conn.close()
    
    return len(records)


def get_historical_average(
    park_id: int,
    ride_name: str,
    day_of_week: Optional[int] = None,
    hour_of_day: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None
) -> Optional[dict]:
    """
    Get historical average wait time for a ride.
    
    Args:
        park_id: The Queue-Times park ID
        ride_name: Name of the ride (case-insensitive match)
        day_of_week: Optional filter by day (0=Monday, 6=Sunday)
        hour_of_day: Optional filter by hour (0-23)
        conn: Optional database connection
        
    Returns:
        Dict with average, min, max, sample_count, or None if no data
    """
    should_close = conn is None
    if conn is None:
        conn = get_db_connection()
    
    # Build query
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
    
    cursor = conn.execute(query, params)
    row = cursor.fetchone()
    
    if should_close:
        conn.close()
    
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
    hour_of_day: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None
) -> dict[str, dict]:
    """
    Get historical averages for all rides in a park.
    
    Returns:
        Dict mapping ride names to their average stats
    """
    should_close = conn is None
    if conn is None:
        conn = get_db_connection()
    
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
    
    cursor = conn.execute(query, params)
    
    result = {}
    for row in cursor:
        result[row["ride_name"]] = {
            "average": round(row["avg_wait"], 1),
            "min": row["min_wait"],
            "max": row["max_wait"],
            "sample_count": row["sample_count"]
        }
    
    if should_close:
        conn.close()
    
    return result


def compare_to_average(
    current_wait: int,
    average: float
) -> dict:
    """
    Compare current wait to historical average.
    
    Returns:
        Dict with difference, percent_diff, and status
    """
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


def get_database_stats(conn: Optional[sqlite3.Connection] = None) -> dict:
    """Get statistics about the historical database."""
    should_close = conn is None
    if conn is None:
        conn = get_db_connection()
    
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT park_id) as parks,
            COUNT(DISTINCT ride_name) as rides,
            MIN(recorded_at) as earliest,
            MAX(recorded_at) as latest
        FROM wait_times
    """)
    row = cursor.fetchone()
    
    if should_close:
        conn.close()
    
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
    error_message: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None
):
    """Log a data collection run."""
    should_close = conn is None
    if conn is None:
        conn = get_db_connection()
    
    conn.execute("""
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
    
    if should_close:
        conn.close()
