"""
Web server entry point for Railway deployment.
Runs the MCP server over HTTP using SSE transport.
"""

import sys
import os
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, Mount

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from theme_park_mcp.server import mcp


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
        "name": "Theme Park Wait Times MCP Server",
        "description": "Live wait times for Disney World and Universal Orlando",
        "endpoints": {
            "health": "/health",
            "sse": "/sse"
        },
        "powered_by": "Queue-Times.com"
    })


# Create the main app
app = Starlette(
    routes=[
        Route("/", homepage),
        Route("/health", health),
        Mount("/", app=mcp.sse_app()),
    ]
)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
