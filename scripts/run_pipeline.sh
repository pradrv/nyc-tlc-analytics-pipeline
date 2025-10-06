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

# Activate virtual environment if not already active
if [[ -z "$VIRTUAL_ENV" ]]; then
    if [ -f .venv/bin/activate ]; then
        source .venv/bin/activate
    else
        echo " Virtual environment not found. Run ./scripts/setup.sh first"
        exit 1
    fi
fi

# Run full pipeline
python -m src.cli run-pipeline --full

echo ""
echo "======================================================================"
echo " Full pipeline complete!"
echo "======================================================================"
echo ""
echo "Database location: data/database/nyc_taxi.duckdb"
echo ""
echo "Next steps:"
echo "  • Query the database: python -m src.cli db-stats"
echo "  • Run analytics queries from sql/analytics/"
echo "  • Or explore in Jupyter: docker-compose --profile analysis up jupyter"
echo ""

