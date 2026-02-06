#!/usr/bin/env python3
"""
Entry point for the Theme Park MCP Server.
Run this file to start the server.
"""

import sys
from pathlib import Path

# Add src to path so imports work
sys.path.insert(0, str(Path(__file__).parent / "src"))

from theme_park_mcp.server import mcp

if __name__ == "__main__":
    mcp.run()