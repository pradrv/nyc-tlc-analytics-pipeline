#!/bin/bash

# Run sample pipeline (1 month)

set -e

echo "======================================================================"
echo "NYC Taxi Pipeline - Running Sample (3 month)"
echo "======================================================================"

# Check for uv
if ! command -v uv &> /dev/null; then
    echo " Error: uv not found. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Run sample pipeline
uv run python -m src.cli run-pipeline --sample

echo ""
echo "======================================================================"
echo " Sample pipeline complete!"
echo "======================================================================"
echo ""
echo "Database location: data/database/nyc_taxi.duckdb"
echo ""
echo "Next steps:"
echo "  • Query the database: uv run python -m src.cli db-stats"
echo "  • Run analytics queries: uv run python -m src.cli run-analytics sql/analytics/01_top_zones_by_revenue.sql"
echo "  • Explore all queries: ls sql/analytics/"
echo ""

