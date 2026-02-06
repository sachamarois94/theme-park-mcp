"""
Web server entry point for Railway deployment.
Runs the MCP server over HTTP using SSE transport.
"""

import sys
import os
import uvicorn

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from theme_park_mcp.server import mcp

if __name__ == "__main__":
    # Get port from environment (Railway sets this)
    port = int(os.environ.get("PORT", 8000))
    
    # Get the SSE app from FastMCP
    app = mcp.sse_app()
    
    # Run with uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
