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

# Activate virtual environment
source .venv/bin/activate

# Start Prefect server
prefect server start

