"""
Web server entry point for Railway deployment.
Simple REST API for theme park wait times.
"""

import sys
import os
import asyncio
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from theme_park_mcp.api.queue_times import get_park_wait_times, parse_wait_times
from theme_park_mcp.data.parks import PARKS, get_park_by_slug, list_all_parks


async def health(request):
    """Health check endpoint."""
    return JSONResponse({
        "status": "healthy",
        "service": "theme-park-mcp",
        "version": "1.0.0"
    })


async def homepage(request):
    """Homepage with API info."""
    return JSONResponse({
        "name": "Theme Park Wait Times API",
        "description": "Live wait times for Disney World and Universal Orlando",
        "endpoints": {
            "health": "/health",
            "parks": "/api/parks",
            "wait_times": "/api/parks/{park_slug}/wait-times"
        },
        "available_parks": list(PARKS.keys()),
        "powered_by": "Queue-Times.com"
    })


async def get_parks(request):
    """List all available parks."""
    parks = list_all_parks()
    return JSONResponse({
        "parks": parks
    })


async def get_wait_times_api(request):
    """Get wait times for a specific park."""
    park_slug = request.path_params["park_slug"]
    
    park_info = get_park_by_slug(park_slug)
    if not park_info:
        return JSONResponse(
            {"error": f"Unknown park: {park_slug}", "available": list(PARKS.keys())},
            status_code=404
        )
    
    try:
        raw_data = await get_park_wait_times(park_info["id"])
        rides = parse_wait_times(raw_data)
        
        return JSONResponse({
            "park": park_info["name"],
            "park_slug": park_slug,
            "ride_count": len(rides),
            "rides": rides
        })
    except Exception as e:
        return JSONResponse(
            {"error": str(e)},
            status_code=500
        )


# Create the app
app = Starlette(
    routes=[
        Route("/", homepage),
        Route("/health", health),
        Route("/api/parks", get_parks),
        Route("/api/parks/{park_slug}/wait-times", get_wait_times_api),
    ]
)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting server on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
