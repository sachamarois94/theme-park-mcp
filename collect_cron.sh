#!/bin/bash
cd ~/Documents/theme_park_mcp
source .venv/bin/activate
python collect_data.py >> ~/.theme_park_mcp/cron.log 2>&1
