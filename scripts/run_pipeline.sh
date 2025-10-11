#!/bin/bash

# Run full pipeline (49 months: 2021-01 to 2025-01)

set -e

echo "======================================================================"
echo "NYC Taxi Pipeline - Running Full Pipeline (49 months)"
echo "======================================================================"
echo ""
echo "  WARNING: This will download ~21GB of data and take 60-90 minutes"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# Check for uv
if ! command -v uv &> /dev/null; then
    echo " Error: uv not found. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Run full pipeline
uv run python -m src.cli run-pipeline --full

echo ""
echo "======================================================================"
echo " Full pipeline complete!"
echo "======================================================================"
echo ""
echo "Database location: data/database/nyc_taxi.duckdb"
echo ""
echo "Next steps:"
echo "  • Query the database: uv run python -m src.cli db-stats"
echo "  • Run analytics queries: uv run python -m src.cli run-analytics sql/analytics/01_top_zones_by_revenue.sql"
echo "  • Explore all queries: ls sql/analytics/"
echo ""

