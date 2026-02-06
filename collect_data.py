#!/usr/bin/env python3
"""
Data collector for theme park wait times.
Run this periodically (e.g., via cron) to build historical data.

Usage:
    python collect_data.py              # Collect from all parks
    python collect_data.py --parks magic-kingdom epcot  # Specific parks
    python collect_data.py --daemon --interval 15       # Run every 15 minutes
"""

import asyncio
import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from theme_park_mcp.api.queue_times import get_park_wait_times, parse_wait_times
from theme_park_mcp.data.parks import PARKS, get_park_by_slug
from theme_park_mcp.data.historical import (
    record_wait_times,
    log_collection,
    get_db_connection,
    get_database_stats
)


async def collect_park_data(park_slug: str, conn) -> int:
    """Collect and store data for a single park."""
    park = get_park_by_slug(park_slug)
    if not park:
        print(f"  ⚠️  Unknown park: {park_slug}")
        return 0
    
    try:
        raw_data = await get_park_wait_times(park["id"])
        rides = parse_wait_times(raw_data)
        
        count = record_wait_times(park["id"], rides, conn)
        print(f"  ✓ {park['name']}: {count} rides recorded")
        return count
        
    except Exception as e:
        print(f"  ✗ {park['name']}: {e}")
        return 0


async def collect_all(park_slugs: list[str] = None):
    """Collect data from specified parks (or all parks if none specified)."""
    if park_slugs is None:
        park_slugs = list(PARKS.keys())
    
    print(f"\n📊 Collecting wait times at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Parks: {', '.join(park_slugs)}")
    print("-" * 50)
    
    conn = get_db_connection()
    total_rides = 0
    parks_collected = []
    errors = []
    
    for slug in park_slugs:
        park = get_park_by_slug(slug)
        if park:
            count = await collect_park_data(slug, conn)
            if count > 0:
                total_rides += count
                parks_collected.append(park["id"])
        else:
            errors.append(f"Unknown park: {slug}")
    
    # Log the collection
    success = total_rides > 0
    error_msg = "; ".join(errors) if errors else None
    log_collection(parks_collected, total_rides, success, error_msg, conn)
    
    # Print summary
    print("-" * 50)
    print(f"✅ Total: {total_rides} ride records from {len(parks_collected)} parks")
    
    # Show database stats
    stats = get_database_stats(conn)
    print(f"\n📈 Database stats:")
    print(f"   Total records: {stats['total_records']:,}")
    print(f"   Parks tracked: {stats['parks_tracked']}")
    print(f"   Rides tracked: {stats['rides_tracked']}")
    if stats['earliest_record']:
        print(f"   Date range: {stats['earliest_record'][:10]} to {stats['latest_record'][:10]}")
    
    conn.close()
    return total_rides


async def run_daemon(park_slugs: list[str], interval_minutes: int):
    """Run collector continuously at specified interval."""
    print(f"🔄 Starting daemon mode - collecting every {interval_minutes} minutes")
    print("   Press Ctrl+C to stop\n")
    
    while True:
        try:
            await collect_all(park_slugs)
            print(f"\n⏰ Next collection in {interval_minutes} minutes...\n")
            await asyncio.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            print("\n👋 Stopping daemon...")
            break


def main():
    parser = argparse.ArgumentParser(
        description="Collect theme park wait time data"
    )
    parser.add_argument(
        "--parks", 
        nargs="+",
        help="Specific parks to collect (slugs like 'magic-kingdom')"
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuously"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=15,
        help="Minutes between collections in daemon mode (default: 15)"
    )
    parser.add_argument(
        "--list-parks",
        action="store_true",
        help="List available park slugs"
    )
    
    args = parser.parse_args()
    
    if args.list_parks:
        print("Available parks:")
        for slug, park in PARKS.items():
            print(f"  {slug}: {park['name']} ({park['resort']})")
        return
    
    park_slugs = args.parks  # None means all parks
    
    if args.daemon:
        asyncio.run(run_daemon(park_slugs, args.interval))
    else:
        asyncio.run(collect_all(park_slugs))


if __name__ == "__main__":
    main()
