"""
Optimal touring route suggestions for theme parks.

Factors considered (in current weight order):
1. Land grouping - minimize walking between areas
2. Walk times - estimated time between lands
3. Current wait times - prioritize shorter waits within a land
4. Historical patterns - (future) prioritize rides that get busier later

The algorithm:
1. Group requested rides by land
2. Determine optimal land order based on park layout
3. Within each land, order rides by wait time (shortest first)
4. Apply historical adjustments (when data available)
"""

from datetime import datetime
from typing import Optional


# Park layouts - which lands are adjacent to each other
# This helps minimize walking by visiting nearby lands consecutively
PARK_LAYOUTS = {
    6: {  # Magic Kingdom
        "name": "Magic Kingdom",
        "lands": {
            "Main Street, U.S.A.": {"position": 0, "adjacent": ["Adventureland", "Tomorrowland"]},
            "Adventureland": {"position": 1, "adjacent": ["Main Street, U.S.A.", "Frontierland"]},
            "Frontierland": {"position": 2, "adjacent": ["Adventureland", "Liberty Square"]},
            "Liberty Square": {"position": 3, "adjacent": ["Frontierland", "Fantasyland"]},
            "Fantasyland": {"position": 4, "adjacent": ["Liberty Square", "Tomorrowland"]},
            "Tomorrowland": {"position": 5, "adjacent": ["Fantasyland", "Main Street, U.S.A."]},
        },
        # Estimated walk times in minutes between lands
        "walk_times": {
            ("Main Street, U.S.A.", "Adventureland"): 3,
            ("Main Street, U.S.A.", "Tomorrowland"): 3,
            ("Adventureland", "Frontierland"): 3,
            ("Frontierland", "Liberty Square"): 2,
            ("Liberty Square", "Fantasyland"): 3,
            ("Fantasyland", "Tomorrowland"): 4,
            # Longer walks (non-adjacent)
            ("Main Street, U.S.A.", "Frontierland"): 6,
            ("Main Street, U.S.A.", "Liberty Square"): 7,
            ("Main Street, U.S.A.", "Fantasyland"): 8,
            ("Adventureland", "Liberty Square"): 5,
            ("Adventureland", "Fantasyland"): 8,
            ("Adventureland", "Tomorrowland"): 10,
            ("Frontierland", "Fantasyland"): 5,
            ("Frontierland", "Tomorrowland"): 9,
            ("Liberty Square", "Tomorrowland"): 7,
        },
        "entry_land": "Main Street, U.S.A.",
    },
    5: {  # EPCOT
        "name": "EPCOT",
        "lands": {
            "World Celebration": {"position": 0, "adjacent": ["World Discovery", "World Nature"]},
            "World Discovery": {"position": 1, "adjacent": ["World Celebration", "World Showcase"]},
            "World Nature": {"position": 2, "adjacent": ["World Celebration", "World Showcase"]},
            "World Showcase": {"position": 3, "adjacent": ["World Discovery", "World Nature"]},
        },
        "walk_times": {
            ("World Celebration", "World Discovery"): 5,
            ("World Celebration", "World Nature"): 5,
            ("World Discovery", "World Showcase"): 8,
            ("World Nature", "World Showcase"): 8,
            ("World Celebration", "World Showcase"): 12,
            ("World Discovery", "World Nature"): 10,
        },
        "entry_land": "World Celebration",
    },
    7: {  # Hollywood Studios
        "name": "Hollywood Studios",
        "lands": {
            "Hollywood Boulevard": {"position": 0, "adjacent": ["Echo Lake", "Sunset Boulevard"]},
            "Echo Lake": {"position": 1, "adjacent": ["Hollywood Boulevard", "Star Wars: Galaxy's Edge"]},
            "Sunset Boulevard": {"position": 2, "adjacent": ["Hollywood Boulevard", "Toy Story Land"]},
            "Star Wars: Galaxy's Edge": {"position": 3, "adjacent": ["Echo Lake", "Toy Story Land", "Grand Avenue"]},
            "Toy Story Land": {"position": 4, "adjacent": ["Sunset Boulevard", "Star Wars: Galaxy's Edge"]},
            "Grand Avenue": {"position": 5, "adjacent": ["Star Wars: Galaxy's Edge", "Hollywood Boulevard"]},
        },
        "walk_times": {
            ("Hollywood Boulevard", "Echo Lake"): 3,
            ("Hollywood Boulevard", "Sunset Boulevard"): 3,
            ("Echo Lake", "Star Wars: Galaxy's Edge"): 5,
            ("Sunset Boulevard", "Toy Story Land"): 6,
            ("Star Wars: Galaxy's Edge", "Toy Story Land"): 5,
            ("Star Wars: Galaxy's Edge", "Grand Avenue"): 3,
            ("Grand Avenue", "Hollywood Boulevard"): 4,
            # Longer walks
            ("Hollywood Boulevard", "Star Wars: Galaxy's Edge"): 8,
            ("Hollywood Boulevard", "Toy Story Land"): 9,
            ("Echo Lake", "Toy Story Land"): 10,
            ("Echo Lake", "Sunset Boulevard"): 6,
            ("Sunset Boulevard", "Star Wars: Galaxy's Edge"): 10,
            ("Sunset Boulevard", "Grand Avenue"): 7,
            ("Echo Lake", "Grand Avenue"): 6,
        },
        "entry_land": "Hollywood Boulevard",
    },
    8: {  # Animal Kingdom
        "name": "Animal Kingdom",
        "lands": {
            "Oasis": {"position": 0, "adjacent": ["Discovery Island"]},
            "Discovery Island": {"position": 1, "adjacent": ["Oasis", "Africa", "Asia", "Pandora", "DinoLand U.S.A."]},
            "Africa": {"position": 2, "adjacent": ["Discovery Island", "Asia"]},
            "Asia": {"position": 3, "adjacent": ["Discovery Island", "Africa", "DinoLand U.S.A."]},
            "Pandora - The World of Avatar": {"position": 4, "adjacent": ["Discovery Island"]},
            "DinoLand U.S.A.": {"position": 5, "adjacent": ["Discovery Island", "Asia"]},
        },
        "walk_times": {
            ("Oasis", "Discovery Island"): 3,
            ("Discovery Island", "Africa"): 4,
            ("Discovery Island", "Asia"): 5,
            ("Discovery Island", "Pandora - The World of Avatar"): 4,
            ("Discovery Island", "DinoLand U.S.A."): 5,
            ("Africa", "Asia"): 6,
            ("Asia", "DinoLand U.S.A."): 4,
            # Longer walks
            ("Oasis", "Africa"): 7,
            ("Oasis", "Asia"): 8,
            ("Oasis", "Pandora - The World of Avatar"): 7,
            ("Oasis", "DinoLand U.S.A."): 8,
            ("Africa", "Pandora - The World of Avatar"): 8,
            ("Africa", "DinoLand U.S.A."): 9,
            ("Asia", "Pandora - The World of Avatar"): 9,
            ("Pandora - The World of Avatar", "DinoLand U.S.A."): 9,
        },
        "entry_land": "Oasis",
    },
    64: {  # Universal Studios Florida
        "name": "Universal Studios Florida",
        "lands": {
            "Production Central": {"position": 0, "adjacent": ["New York", "Hollywood"]},
            "New York": {"position": 1, "adjacent": ["Production Central", "San Francisco"]},
            "San Francisco": {"position": 2, "adjacent": ["New York", "The Wizarding World of Harry Potter - Diagon Alley"]},
            "The Wizarding World of Harry Potter - Diagon Alley": {"position": 3, "adjacent": ["San Francisco", "World Expo"]},
            "World Expo": {"position": 4, "adjacent": ["The Wizarding World of Harry Potter - Diagon Alley", "Springfield"]},
            "Springfield": {"position": 5, "adjacent": ["World Expo", "Woody Woodpecker's KidZone"]},
            "Woody Woodpecker's KidZone": {"position": 6, "adjacent": ["Springfield", "Hollywood"]},
            "Hollywood": {"position": 7, "adjacent": ["Woody Woodpecker's KidZone", "Production Central"]},
        },
        "walk_times": {
            ("Production Central", "New York"): 3,
            ("Production Central", "Hollywood"): 3,
            ("New York", "San Francisco"): 4,
            ("San Francisco", "The Wizarding World of Harry Potter - Diagon Alley"): 3,
            ("The Wizarding World of Harry Potter - Diagon Alley", "World Expo"): 4,
            ("World Expo", "Springfield"): 3,
            ("Springfield", "Woody Woodpecker's KidZone"): 3,
            ("Woody Woodpecker's KidZone", "Hollywood"): 4,
        },
        "entry_land": "Production Central",
    },
    65: {  # Islands of Adventure
        "name": "Islands of Adventure",
        "lands": {
            "Port of Entry": {"position": 0, "adjacent": ["Marvel Super Hero Island", "Seuss Landing"]},
            "Marvel Super Hero Island": {"position": 1, "adjacent": ["Port of Entry", "Toon Lagoon"]},
            "Toon Lagoon": {"position": 2, "adjacent": ["Marvel Super Hero Island", "Skull Island"]},
            "Skull Island": {"position": 3, "adjacent": ["Toon Lagoon", "Jurassic Park"]},
            "Jurassic Park": {"position": 4, "adjacent": ["Skull Island", "The Wizarding World of Harry Potter - Hogsmeade"]},
            "The Wizarding World of Harry Potter - Hogsmeade": {"position": 5, "adjacent": ["Jurassic Park", "The Lost Continent"]},
            "The Lost Continent": {"position": 6, "adjacent": ["The Wizarding World of Harry Potter - Hogsmeade", "Seuss Landing"]},
            "Seuss Landing": {"position": 7, "adjacent": ["The Lost Continent", "Port of Entry"]},
        },
        "walk_times": {
            ("Port of Entry", "Marvel Super Hero Island"): 3,
            ("Port of Entry", "Seuss Landing"): 3,
            ("Marvel Super Hero Island", "Toon Lagoon"): 4,
            ("Toon Lagoon", "Skull Island"): 3,
            ("Skull Island", "Jurassic Park"): 4,
            ("Jurassic Park", "The Wizarding World of Harry Potter - Hogsmeade"): 4,
            ("The Wizarding World of Harry Potter - Hogsmeade", "The Lost Continent"): 3,
            ("The Lost Continent", "Seuss Landing"): 4,
        },
        "entry_land": "Port of Entry",
    },
}


def get_walk_time(park_id: int, from_land: str, to_land: str) -> int:
    """Get estimated walk time between two lands in minutes."""
    if from_land == to_land:
        return 0
    
    layout = PARK_LAYOUTS.get(park_id)
    if not layout:
        return 5  # Default estimate
    
    walk_times = layout.get("walk_times", {})
    
    # Check both directions
    key1 = (from_land, to_land)
    key2 = (to_land, from_land)
    
    if key1 in walk_times:
        return walk_times[key1]
    if key2 in walk_times:
        return walk_times[key2]
    
    return 7  # Default for unknown routes


def get_land_order(park_id: int, lands_to_visit: set[str]) -> list[str]:
    """
    Determine optimal order to visit lands based on park layout.
    Uses a greedy nearest-neighbor approach starting from entry.
    """
    layout = PARK_LAYOUTS.get(park_id)
    if not layout:
        return list(lands_to_visit)
    
    entry = layout.get("entry_land", list(lands_to_visit)[0])
    
    # Start from entry if it's in our list, otherwise find closest to entry
    if entry in lands_to_visit:
        current = entry
    else:
        # Find land closest to entry
        current = min(
            lands_to_visit,
            key=lambda land: get_walk_time(park_id, entry, land)
        )
    
    ordered = [current]
    remaining = lands_to_visit - {current}
    
    # Greedy nearest neighbor
    while remaining:
        next_land = min(
            remaining,
            key=lambda land: get_walk_time(park_id, current, land)
        )
        ordered.append(next_land)
        remaining.remove(next_land)
        current = next_land
    
    return ordered


def calculate_historical_priority(
    ride_name: str,
    current_hour: int,
    historical_averages: dict
) -> float:
    """
    Calculate priority boost based on historical patterns.
    
    Returns a multiplier:
    - > 1.0 means ride should be prioritized (gets busier later)
    - < 1.0 means ride can wait (gets less busy later)
    - 1.0 means no historical adjustment
    
    For now, returns 1.0 (neutral) until we have enough data.
    Future: Compare current hour's average to later hours.
    """
    # TODO: Implement once we have multi-hour historical data
    # For now, return neutral priority
    return 1.0


def optimize_route(
    park_id: int,
    rides: list[dict],
    must_do: list[str] = None,
    historical_averages: dict = None,
    max_total_time: int = None
) -> dict:
    """
    Generate an optimized touring route.
    
    Args:
        park_id: Queue-Times park ID
        rides: List of ride dicts from parse_wait_times()
        must_do: Optional list of ride names that must be included
        historical_averages: Optional dict of historical data
        max_total_time: Optional max time in minutes for the route
        
    Returns:
        Dict with ordered route, estimated times, and stats
    """
    if historical_averages is None:
        historical_averages = {}
    
    current_hour = datetime.now().hour
    
    # Filter to open rides with wait times
    available_rides = [
        r for r in rides 
        if r["is_open"] and r.get("wait_time") is not None
    ]
    
    # If must_do specified, filter to those (with fuzzy matching)
    if must_do:
        must_do_lower = [m.lower() for m in must_do]
        selected_rides = []
        for ride in available_rides:
            ride_name_lower = ride["name"].lower()
            for must in must_do_lower:
                if must in ride_name_lower:
                    selected_rides.append(ride)
                    break
        available_rides = selected_rides if selected_rides else available_rides
    
    if not available_rides:
        return {
            "success": False,
            "error": "No matching rides available",
            "route": [],
            "total_wait_time": 0,
            "total_walk_time": 0,
            "total_time": 0
        }
    
    # Group rides by land
    rides_by_land: dict[str, list] = {}
    for ride in available_rides:
        land = ride.get("land", "Unknown")
        if land not in rides_by_land:
            rides_by_land[land] = []
        
        # Calculate priority score
        wait_time = ride.get("wait_time", 0)
        historical_priority = calculate_historical_priority(
            ride["name"], current_hour, historical_averages
        )
        
        # Score: lower is better
        # Weight: 70% current wait, 30% historical priority
        ride["priority_score"] = wait_time * (0.7 + 0.3 * historical_priority)
        rides_by_land[land].append(ride)
    
    # Sort rides within each land by priority score
    for land in rides_by_land:
        rides_by_land[land].sort(key=lambda x: x["priority_score"])
    
    # Determine optimal land order
    land_order = get_land_order(park_id, set(rides_by_land.keys()))
    
    # Build the route
    route = []
    total_wait_time = 0
    total_walk_time = 0
    current_land = None
    
    for land in land_order:
        land_rides = rides_by_land.get(land, [])
        
        for ride in land_rides:
            # Add walk time if changing lands
            if current_land and current_land != land:
                walk_time = get_walk_time(park_id, current_land, land)
                total_walk_time += walk_time
            
            wait_time = ride.get("wait_time", 0)
            total_wait_time += wait_time
            
            # Check if we've exceeded max time
            current_total = total_wait_time + total_walk_time
            if max_total_time and current_total > max_total_time:
                break
            
            route.append({
                "name": ride["name"],
                "land": land,
                "wait_time": wait_time,
                "walk_from_previous": get_walk_time(park_id, current_land, land) if current_land else 0,
                "cumulative_time": current_total
            })
            
            current_land = land
        
        # Check max time at land level too
        if max_total_time and (total_wait_time + total_walk_time) > max_total_time:
            break
    
    return {
        "success": True,
        "route": route,
        "total_wait_time": total_wait_time,
        "total_walk_time": total_walk_time,
        "total_time": total_wait_time + total_walk_time,
        "ride_count": len(route),
        "lands_visited": len(set(r["land"] for r in route))
    }


def format_route(route_result: dict, park_name: str) -> str:
    """Format the route result as markdown."""
    if not route_result["success"]:
        return f"Could not generate route: {route_result.get('error', 'Unknown error')}"
    
    route = route_result["route"]
    if not route:
        return "No rides available for routing."
    
    lines = [
        f"# Optimized Touring Plan - {park_name}",
        f"*Generated {datetime.now().strftime('%I:%M %p')}*\n",
        f"**Summary:** {route_result['ride_count']} rides across {route_result['lands_visited']} lands",
        f"**Estimated Total Time:** {route_result['total_time']} minutes",
        f"  - Wait time: {route_result['total_wait_time']} min",
        f"  - Walking: {route_result['total_walk_time']} min\n",
        "---\n",
        "## Your Route\n"
    ]
    
    current_land = None
    step = 1
    
    for item in route:
        # Add land header when changing lands
        if item["land"] != current_land:
            if current_land is not None:
                walk = item["walk_from_previous"]
                lines.append(f"\n🚶 *Walk to {item['land']} ({walk} min)*\n")
            lines.append(f"### 📍 {item['land']}\n")
            current_land = item["land"]
        
        wait = item["wait_time"]
        if wait == 0:
            wait_str = "Walk-on"
        else:
            wait_str = f"{wait} min wait"
        
        lines.append(f"{step}. **{item['name']}** - {wait_str}")
        step += 1
    
    lines.append("\n---")
    lines.append("*Tip: Times are estimates. Check wait times as you go and adjust!*")
    
    return "\n".join(lines)
