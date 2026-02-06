"""
Client for the Queue-Times.com API.
Free API that provides live wait times for theme parks.
Attribution required: "Powered by Queue-Times.com"
"""

import httpx
from datetime import datetime, timezone

BASE_URL = "https://queue-times.com"


async def get_park_wait_times(park_id: int) -> dict:
    """
    Fetch current wait times for a specific park.
    
    Args:
        park_id: The Queue-Times park ID
        
    Returns:
        Dict with lands and rides, each ride having:
        - id, name, is_open, wait_time, last_updated
    """
    url = f"{BASE_URL}/parks/{park_id}/queue_times.json"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        return response.json()


async def get_all_parks() -> list[dict]:
    """
    Fetch list of all parks available in Queue-Times.
    
    Returns:
        List of park groups, each containing parks with:
        - id, name, country, continent, latitude, longitude, timezone
    """
    url = f"{BASE_URL}/parks.json"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        return response.json()


def parse_wait_times(raw_data: dict) -> list[dict]:
    """
    Parse raw Queue-Times response into a flat list of rides.
    
    Args:
        raw_data: Response from get_park_wait_times()
        
    Returns:
        List of rides with land info included
    """
    rides = []
    
    # Rides grouped by land
    for land in raw_data.get("lands", []):
        land_name = land.get("name", "Unknown Land")
        for ride in land.get("rides", []):
            rides.append({
                "id": ride.get("id"),
                "name": ride.get("name"),
                "land": land_name,
                "is_open": ride.get("is_open", False),
                "wait_time": ride.get("wait_time", 0),
                "last_updated": ride.get("last_updated"),
            })
    
    # Some parks have rides not in lands
    for ride in raw_data.get("rides", []):
        rides.append({
            "id": ride.get("id"),
            "name": ride.get("name"),
            "land": "General",
            "is_open": ride.get("is_open", False),
            "wait_time": ride.get("wait_time", 0),
            "last_updated": ride.get("last_updated"),
        })
    
    return rides


def format_wait_time(minutes: int) -> str:
    """Format wait time in a human-readable way."""
    if minutes == 0:
        return "Walk-on"
    elif minutes < 60:
        return f"{minutes} min"
    else:
        hours = minutes // 60
        mins = minutes % 60
        if mins == 0:
            return f"{hours}h"
        return f"{hours}h {mins}min"