#!/bin/bash

# Run sample pipeline (1 month)

set -e

echo "======================================================================"
echo "NYC Taxi Pipeline - Running Sample (1 month)"
echo "======================================================================"

# Activate virtual environment if not already active
if [[ -z "$VIRTUAL_ENV" ]]; then
    if [ -f .venv/bin/activate ]; then
        source .venv/bin/activate
    else
        echo " Virtual environment not found. Run ./scripts/setup.sh first"
        exit 1
    fi
fi

# Run sample pipeline
python -m src.cli run-pipeline --sample

echo ""
echo "======================================================================"
echo " Sample pipeline complete!"
echo "======================================================================"
echo ""
echo "Database location: data/database/nyc_taxi.duckdb"
echo ""
echo "Next steps:"
echo "  • Query the database: python -m src.cli db-stats"
echo "  • Run analytics queries from sql/analytics/"
echo "  • Or explore in Jupyter: docker-compose --profile analysis up jupyter"
echo ""

