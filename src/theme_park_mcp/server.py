"""
Theme Park Wait Times MCP Server

Provides live wait times for Disney World and Universal Orlando parks.
Data powered by Queue-Times.com
"""

import sys
import logging
from datetime import datetime
from mcp.server.fastmcp import FastMCP

from .api.queue_times import (
    get_park_wait_times,
    parse_wait_times,
    format_wait_time,
)
from .data.parks import (
    get_park_by_slug,
    list_all_parks,
    PARKS,
)
from .data.historical import (
    get_ride_averages_for_park,
    compare_to_average,
    format_comparison,
    get_database_stats,
)
from .data.touring import (
    optimize_route,
    format_route,
    PARK_LAYOUTS,
)

# Configure logging to stderr for Claude Desktop debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger("theme-park-mcp")

# Initialize the MCP server
mcp = FastMCP("Theme Park Wait Times")


@mcp.tool()
async def list_parks() -> str:
    """
    List all available theme parks.
    
    Returns the parks you can query for wait times, including
    Walt Disney World and Universal Orlando parks.
    """
    logger.info("list_parks called")
    parks = list_all_parks()
    
    lines = ["# Available Theme Parks\n"]
    
    current_resort = None
    for park in parks:
        if park["resort"] != current_resort:
            current_resort = park["resort"]
            lines.append(f"\n## {current_resort}\n")
        lines.append(f"- **{park['name']}** (use: `{park['slug']}`)")
    
    lines.append("\n\n*Data powered by Queue-Times.com*")
    return "\n".join(lines)


@mcp.tool()
async def get_wait_times(park: str) -> str:
    """
    Get current wait times for all rides at a theme park.
    
    Args:
        park: Park slug (e.g., 'magic-kingdom', 'epcot', 'universal-studios')
    
    Returns a formatted list of wait times organized by land/area.
    """
    logger.info(f"get_wait_times called for park: {park}")
    
    park_info = get_park_by_slug(park)
    if not park_info:
        available = ", ".join(PARKS.keys())
        return f"Unknown park: {park}\n\nAvailable parks: {available}"
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        rides = parse_wait_times(raw_data)
    except Exception as e:
        logger.error(f"API error: {e}")
        return f"Error fetching wait times: {e}"
    
    # Organize by land
    lands: dict[str, list] = {}
    closed = []
    
    for ride in rides:
        if not ride["is_open"]:
            closed.append(ride["name"])
            continue
        
        land = ride.get("land", "Other")
        if land not in lands:
            lands[land] = []
        lands[land].append(ride)
    
    # Sort rides by wait time within each land
    for land in lands:
        lands[land].sort(key=lambda x: x.get("wait_time", 0) or 0, reverse=True)
    
    # Build output
    lines = [f"# {park_info['name']} - Current Wait Times\n"]
    
    for land, land_rides in lands.items():
        lines.append(f"## {land}")
        for ride in land_rides:
            wait = format_wait_time(ride.get("wait_time"))
            lines.append(f"- **{ride['name']}**: {wait}")
        lines.append("")
    
    if closed:
        lines.append("---")
        lines.append(f"*{len(closed)} attractions currently closed*")
    
    return "\n".join(lines)


@mcp.tool()
async def get_wait_times_with_history(park: str) -> str:
    """
    Get current wait times with historical comparison.
    
    Shows current wait times alongside historical averages for the same
    day of week and time, highlighting when waits are significantly
    above or below typical levels.
    
    Args:
        park: Park slug (e.g., 'magic-kingdom', 'epcot')
    
    Returns wait times with historical comparisons where available.
    """
    logger.info(f"get_wait_times_with_history called for park: {park}")
    
    park_info = get_park_by_slug(park)
    if not park_info:
        available = ", ".join(PARKS.keys())
        return f"Unknown park: {park}\n\nAvailable parks: {available}"
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        rides = parse_wait_times(raw_data)
    except Exception as e:
        logger.error(f"API error: {e}")
        return f"Error fetching wait times: {e}"
    
    # Get historical averages for current day/hour
    now = datetime.now()
    day_of_week = now.weekday()
    hour_of_day = now.hour
    
    try:
        averages = get_ride_averages_for_park(
            park_info["id"],
            day_of_week=day_of_week,
            hour_of_day=hour_of_day
        )
        has_history = len(averages) > 0
    except Exception as e:
        logger.warning(f"Could not load historical data: {e}")
        averages = {}
        has_history = False
    
    # Organize by land
    lands: dict[str, list] = {}
    closed = []
    
    for ride in rides:
        if not ride["is_open"]:
            closed.append(ride["name"])
            continue
        
        land = ride.get("land", "Other")
        if land not in lands:
            lands[land] = []
        
        # Add comparison if we have historical data
        ride_avg = averages.get(ride["name"])
        if ride_avg and ride.get("wait_time"):
            comparison = compare_to_average(ride["wait_time"], ride_avg["average"])
            ride["comparison"] = comparison
            ride["historical_avg"] = ride_avg["average"]
        
        lands[land].append(ride)
    
    # Sort rides by wait time
    for land in lands:
        lands[land].sort(key=lambda x: x.get("wait_time", 0) or 0, reverse=True)
    
    # Build output
    day_name = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][day_of_week]
    lines = [f"# {park_info['name']} - Current Wait Times"]
    lines.append(f"*{day_name} {now.strftime('%I:%M %p')}*\n")
    
    if has_history:
        lines.append("*Comparing to historical averages for this day/time*\n")
    
    for land, land_rides in lands.items():
        lines.append(f"## {land}")
        for ride in land_rides:
            wait = format_wait_time(ride.get("wait_time"))
            line = f"- **{ride['name']}**: {wait}"
            
            # Add comparison
            if "comparison" in ride:
                comp_str = format_comparison(ride["wait_time"], ride["comparison"])
                if comp_str:
                    status = ride["comparison"]["status"]
                    if status in ["much_lower", "lower"]:
                        line += f" 🟢 {comp_str}"
                    elif status in ["much_higher", "higher"]:
                        line += f" 🔴 {comp_str}"
                    else:
                        line += f" ⚪ {comp_str}"
            
            lines.append(line)
        lines.append("")
    
    if closed:
        lines.append("---")
        lines.append(f"*{len(closed)} attractions currently closed*")
    
    if not has_history:
        lines.append("\n---")
        lines.append("*No historical data yet. Run the data collector to build history.*")
    
    return "\n".join(lines)


@mcp.tool()
async def find_shortest_waits(park: str, max_wait: int = 30) -> str:
    """
    Find rides with the shortest wait times.
    
    Args:
        park: Park slug (e.g., 'magic-kingdom')
        max_wait: Maximum wait time in minutes (default: 30)
    
    Returns rides with wait times at or below the specified maximum.
    """
    logger.info(f"find_shortest_waits called for park: {park}, max_wait: {max_wait}")
    
    park_info = get_park_by_slug(park)
    if not park_info:
        available = ", ".join(PARKS.keys())
        return f"Unknown park: {park}\n\nAvailable parks: {available}"
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        rides = parse_wait_times(raw_data)
    except Exception as e:
        logger.error(f"API error: {e}")
        return f"Error fetching wait times: {e}"
    
    # Filter and sort
    short_waits = [
        r for r in rides 
        if r["is_open"] and r.get("wait_time") is not None and r["wait_time"] <= max_wait
    ]
    short_waits.sort(key=lambda x: x["wait_time"])
    
    if not short_waits:
        return f"No rides found with wait times of {max_wait} minutes or less at {park_info['name']}."
    
    lines = [f"# {park_info['name']} - Rides with {max_wait}min or less\n"]
    
    for ride in short_waits:
        wait = format_wait_time(ride["wait_time"])
        land = ride.get("land", "")
        lines.append(f"- **{ride['name']}**: {wait} ({land})")
    
    lines.append(f"\n*{len(short_waits)} rides found*")
    return "\n".join(lines)


@mcp.tool()
async def find_best_value_rides(park: str) -> str:
    """
    Find rides with wait times significantly below their historical average.
    
    These represent the best "value" - popular rides that currently
    have shorter-than-usual waits.
    
    Args:
        park: Park slug (e.g., 'magic-kingdom')
    
    Returns rides sorted by how much below average their current wait is.
    """
    logger.info(f"find_best_value_rides called for park: {park}")
    
    park_info = get_park_by_slug(park)
    if not park_info:
        available = ", ".join(PARKS.keys())
        return f"Unknown park: {park}\n\nAvailable parks: {available}"
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        rides = parse_wait_times(raw_data)
    except Exception as e:
        logger.error(f"API error: {e}")
        return f"Error fetching wait times: {e}"
    
    # Get historical averages
    now = datetime.now()
    try:
        averages = get_ride_averages_for_park(
            park_info["id"],
            day_of_week=now.weekday(),
            hour_of_day=now.hour
        )
    except Exception as e:
        return f"No historical data available yet. Run the data collector to build history."
    
    if not averages:
        return "No historical data available yet. Run the data collector to build history."
    
    # Find rides below average
    best_values = []
    for ride in rides:
        if not ride["is_open"] or not ride.get("wait_time"):
            continue
        
        ride_avg = averages.get(ride["name"])
        if ride_avg and ride_avg["sample_count"] >= 5:  # Need enough samples
            comparison = compare_to_average(ride["wait_time"], ride_avg["average"])
            if comparison["percent_diff"] and comparison["percent_diff"] < -10:
                best_values.append({
                    **ride,
                    "comparison": comparison,
                    "historical_avg": ride_avg["average"]
                })
    
    if not best_values:
        return f"No rides currently below their historical average at {park_info['name']}."
    
    # Sort by percent below average
    best_values.sort(key=lambda x: x["comparison"]["percent_diff"])
    
    lines = [f"# {park_info['name']} - Best Value Rides Right Now\n"]
    lines.append("*Rides with shorter-than-usual waits*\n")
    
    for ride in best_values[:10]:  # Top 10
        wait = format_wait_time(ride["wait_time"])
        avg = format_wait_time(int(ride["historical_avg"]))
        percent = abs(ride["comparison"]["percent_diff"])
        lines.append(
            f"- **{ride['name']}**: {wait} "
            f"(usually {avg}, {percent}% below average)"
        )
    
    return "\n".join(lines)


@mcp.tool()
async def get_ride_status(park: str, ride_name: str) -> str:
    """
    Get detailed status for a specific ride.
    
    Args:
        park: Park slug (e.g., 'magic-kingdom')
        ride_name: Full or partial ride name (e.g., 'Space Mountain', 'space')
    
    Returns current status and wait time for matching rides.
    """
    logger.info(f"get_ride_status called for park: {park}, ride: {ride_name}")
    
    park_info = get_park_by_slug(park)
    if not park_info:
        available = ", ".join(PARKS.keys())
        return f"Unknown park: {park}\n\nAvailable parks: {available}"
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        rides = parse_wait_times(raw_data)
    except Exception as e:
        logger.error(f"API error: {e}")
        return f"Error fetching wait times: {e}"
    
    # Search for matching rides
    search = ride_name.lower()
    matches = [r for r in rides if search in r["name"].lower()]
    
    if not matches:
        return f"No rides matching '{ride_name}' found at {park_info['name']}."
    
    lines = []
    for ride in matches:
        lines.append(f"## {ride['name']}")
        
        if ride["is_open"]:
            wait = format_wait_time(ride.get("wait_time"))
            lines.append(f"- **Status**: Open")
            lines.append(f"- **Wait Time**: {wait}")
        else:
            lines.append(f"- **Status**: Closed")
        
        if ride.get("land"):
            lines.append(f"- **Location**: {ride['land']}")
        lines.append("")
    
    return "\n".join(lines)


@mcp.tool()
async def get_history_stats() -> str:
    """
    Get statistics about the historical data collected.
    
    Shows how much data has been collected and the date range covered.
    Useful for understanding when historical comparisons will be accurate.
    """
    logger.info("get_history_stats called")
    
    try:
        stats = get_database_stats()
    except Exception as e:
        return f"Could not load database stats: {e}"
    
    if stats["total_records"] == 0:
        return (
            "# Historical Data\n\n"
            "No data collected yet.\n\n"
            "Run `python collect_data.py` to start collecting historical wait times.\n"
            "For best results, run the collector every 15-30 minutes over several weeks."
        )
    
    lines = [
        "# Historical Data Statistics\n",
        f"- **Total Records**: {stats['total_records']:,}",
        f"- **Parks Tracked**: {stats['parks_tracked']}",
        f"- **Rides Tracked**: {stats['rides_tracked']}",
    ]
    
    if stats["earliest_record"]:
        lines.append(f"- **Date Range**: {stats['earliest_record'][:10]} to {stats['latest_record'][:10]}")
    
    lines.append("\n---")
    lines.append("*More data = more accurate comparisons. Keep the collector running!*")
    
    return "\n".join(lines)


@mcp.tool()
async def plan_touring_route(
    park: str, 
    rides: str = None,
    max_time: int = None
) -> str:
    """
    Generate an optimized touring route for a theme park.
    
    Creates a plan that minimizes walking by grouping nearby attractions
    and orders rides efficiently based on current wait times.
    
    Args:
        park: Park slug (e.g., 'magic-kingdom', 'epcot')
        rides: Optional comma-separated list of specific rides to include 
               (e.g., "Space Mountain, Pirates, Haunted Mansion").
               If not provided, includes all open rides.
        max_time: Optional maximum total time in minutes for the route.
    
    Returns an optimized touring plan with estimated times.
    """
    logger.info(f"plan_touring_route called for park: {park}, rides: {rides}, max_time: {max_time}")
    
    park_info = get_park_by_slug(park)
    if not park_info:
        available = ", ".join(PARKS.keys())
        return f"Unknown park: {park}\n\nAvailable parks: {available}"
    
    # Check if we have layout data for this park
    if park_info["id"] not in PARK_LAYOUTS:
        return f"Touring optimization not yet available for {park_info['name']}. Coming soon!"
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        all_rides = parse_wait_times(raw_data)
    except Exception as e:
        logger.error(f"API error: {e}")
        return f"Error fetching wait times: {e}"
    
    # Parse must-do rides if provided
    must_do = None
    if rides:
        must_do = [r.strip() for r in rides.split(",")]
    
    # Get historical averages if available
    try:
        averages = get_ride_averages_for_park(park_info["id"])
    except Exception:
        averages = {}
    
    # Generate optimized route
    result = optimize_route(
        park_id=park_info["id"],
        rides=all_rides,
        must_do=must_do,
        historical_averages=averages,
        max_total_time=max_time
    )
    
    return format_route(result, park_info["name"])


@mcp.tool()
async def plan_quick_tour(park: str, time_available: int = 120) -> str:
    """
    Generate a quick touring plan for limited time.
    
    Perfect for when you only have a few hours. Prioritizes the best
    rides with shortest waits that can fit in your time budget.
    
    Args:
        park: Park slug (e.g., 'magic-kingdom')
        time_available: How many minutes you have (default: 120 = 2 hours)
    
    Returns a focused touring plan optimized for your time.
    """
    logger.info(f"plan_quick_tour called for park: {park}, time: {time_available}")
    
    park_info = get_park_by_slug(park)
    if not park_info:
        available = ", ".join(PARKS.keys())
        return f"Unknown park: {park}\n\nAvailable parks: {available}"
    
    if park_info["id"] not in PARK_LAYOUTS:
        return f"Quick tour planning not yet available for {park_info['name']}. Coming soon!"
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        all_rides = parse_wait_times(raw_data)
    except Exception as e:
        logger.error(f"API error: {e}")
        return f"Error fetching wait times: {e}"
    
    # Get historical averages
    try:
        averages = get_ride_averages_for_park(park_info["id"])
    except Exception:
        averages = {}
    
    # Generate route with time limit
    result = optimize_route(
        park_id=park_info["id"],
        rides=all_rides,
        must_do=None,
        historical_averages=averages,
        max_total_time=time_available
    )
    
    output = format_route(result, park_info["name"])
    
    # Add time budget note
    hours = time_available // 60
    mins = time_available % 60
    time_str = f"{hours}h {mins}m" if mins else f"{hours} hours"
    
    output = output.replace(
        "# Optimized Touring Plan",
        f"# Quick Tour ({time_str} Budget)"
    )
    
    return output


# Entry point for running with `python -m theme_park_mcp.server`
if __name__ == "__main__":
    mcp.run()
