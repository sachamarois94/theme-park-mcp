"""
Web server entry point for Railway deployment.
Runs the MCP server over HTTP using SSE transport.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from theme_park_mcp.server import mcp

if __name__ == "__main__":
    # Run with SSE transport for web deployment
    mcp.run(transport="sse")
