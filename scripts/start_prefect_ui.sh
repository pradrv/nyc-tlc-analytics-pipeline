#!/bin/bash

# Start Prefect Server UI
# This script starts a persistent Prefect server for monitoring pipeline runs

echo "======================================================================"
echo "Starting Prefect Server UI"
echo "======================================================================"
echo ""
echo "Access the UI at: http://localhost:4200"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""
echo "======================================================================"

# Check for uv
if ! command -v uv &> /dev/null; then
    echo " Error: uv not found. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Start Prefect server
uv run prefect server start

