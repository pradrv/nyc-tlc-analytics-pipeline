#!/bin/bash

# NYC Taxi Pipeline - Full E2E Execution
# This script runs the complete end-to-end pipeline including:
# - Database initialization
# - Data ingestion (download + load)
# - Quality checks
# - Transformations (raw → fact → aggregates)
# - Analytics readiness

set -e  # Exit on error

echo "======================================================================"
echo " NYC Taxi Pipeline - Full E2E Execution"
echo "======================================================================"
echo ""

# Check for uv (no need to activate with uv)
if ! command -v uv &> /dev/null; then
    echo " Error: uv not found. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Check for sample or full mode
MODE=${1:-sample}

if [ "$MODE" = "sample" ]; then
    echo " Running in SAMPLE mode (1 month of data)"
    SAMPLE_FLAG="--sample"
else
    echo " Running in FULL mode (49 months of data)"
    SAMPLE_FLAG="--full"
fi

echo ""
echo "======================================================================"
echo "Starting E2E Pipeline..."
echo "======================================================================"
echo ""

# Run the full E2E pipeline
uv run python -m src.cli run-e2e $SAMPLE_FLAG

echo ""
echo "======================================================================"
echo " E2E Pipeline Complete!"
echo "======================================================================"
echo ""
echo "Database location: data/database/nyc_taxi.duckdb"
echo ""
echo "Next steps:"
echo "  • View database stats: uv run python -m src.cli db-stats"
echo "  • Run analytics query: uv run python -m src.cli run-analytics sql/analytics/01_top_zones_by_revenue.sql"
echo "  • Explore all queries: ls sql/analytics/"
echo "  • Open Jupyter: docker compose --profile analysis up jupyter"
echo ""
echo "Available analytics queries:"
echo "  01_top_zones_by_revenue.sql       - Top revenue zones"
echo "  02_hourly_demand_patterns.sql     - Peak hours analysis"
echo "  03_market_share_trends.sql        - Yellow vs Green vs HVFHV trends"
echo "  04_hvfhv_platform_economics.sql   - Platform take rates"
echo "  05_pricing_comparison.sql         - Pricing across services"
echo "  06_airport_trips_analysis.sql     - Airport trip patterns"
echo "  07_weekend_vs_weekday.sql         - Weekend vs weekday comparison"
echo "  08_shared_rides_analysis.sql      - Shared ride adoption"
echo "  09_borough_comparison.sql         - Borough-level analysis"
echo "  10_data_quality_summary.sql       - Data quality report"
echo ""
echo "======================================================================"
