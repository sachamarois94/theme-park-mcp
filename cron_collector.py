#!/usr/bin/env python3
"""
Cloud data collector for Railway cron job.
Collects wait times from all parks and stores in PostgreSQL.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from theme_park_mcp.api.queue_times import get_park_wait_times, parse_wait_times
from theme_park_mcp.data.parks import PARKS, get_park_by_slug


# Import the cloud-compatible historical module
# This will use PostgreSQL if DATABASE_URL is set
from theme_park_mcp.data.historical import (
    init_database,
    record_wait_times,
    log_collection,
    get_database_stats
)


async def collect_park_data(park_slug: str) -> tuple[int, int]:
    """
    Collect and store data for a single park.
    Returns (park_id, ride_count) or (park_id, 0) on failure.
    """
    park = get_park_by_slug(park_slug)
    if not park:
        print(f"  ⚠️  Unknown park: {park_slug}")
        return (0, 0)
    
    try:
        raw_data = await get_park_wait_times(park["id"])
        rides = parse_wait_times(raw_data)
        
        count = record_wait_times(park["id"], rides)
        print(f"  ✓ {park['name']}: {count} rides recorded")
        return (park["id"], count)
        
    except Exception as e:
        print(f"  ✗ {park['name']}: {e}")
        return (park["id"], 0)


async def collect_all():
    """Collect data from all parks."""
    print(f"\n📊 Cloud Collector - {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"   Database: {'PostgreSQL' if os.environ.get('DATABASE_URL') else 'SQLite'}")
    print("-" * 50)
    
    # Initialize database tables if needed
    init_database()
    
    total_rides = 0
    parks_collected = []
    errors = []
    
    for slug in PARKS.keys():
        park_id, count = await collect_park_data(slug)
        if count > 0:
            total_rides += count
            parks_collected.append(park_id)
        elif park_id > 0:
            errors.append(f"Failed to collect from park {park_id}")
    
    # Log the collection
    success = total_rides > 0
    error_msg = "; ".join(errors) if errors else None
    log_collection(parks_collected, total_rides, success, error_msg)
    
    # Print summary
    print("-" * 50)
    print(f"✅ Total: {total_rides} ride records from {len(parks_collected)} parks")
    
    # Show database stats
    stats = get_database_stats()
    print(f"\n📈 Database stats:")
    print(f"   Total records: {stats['total_records']:,}")
    print(f"   Parks tracked: {stats['parks_tracked']}")
    print(f"   Rides tracked: {stats['rides_tracked']}")
    if stats['earliest_record']:
        print(f"   Date range: {stats['earliest_record'][:10]} to {stats['latest_record'][:10]}")
    
    return total_rides


def main():
    """Entry point for cron job."""
    print("🚀 Starting theme park data collection...")
    result = asyncio.run(collect_all())
    print(f"\n✨ Collection complete! Recorded {result} rides.")
    

if __name__ == "__main__":
    main()
