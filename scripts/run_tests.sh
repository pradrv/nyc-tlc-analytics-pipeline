#!/bin/bash

echo "==========================================="
echo "NYC Taxi Pipeline - Unit Tests"
echo "==========================================="
echo ""

# Run tests with coverage
uv run pytest tests/ -v --tb=short --color=yes

echo ""
echo "==========================================="
echo "Test Summary"
echo "==========================================="
echo ""
echo "To run specific tests:"
echo "  uv run pytest tests/test_utils.py -v"
echo "  uv run pytest tests/test_database_connection.py -v"
echo "  uv run pytest tests/test_validators.py -v"
echo "  uv run pytest tests/test_quality_checks.py -v"
echo "  uv run pytest tests/test_transformations.py -v"
echo ""
echo "To run with coverage:"
echo "  uv run pytest tests/ --cov=src --cov-report=html"
echo ""

