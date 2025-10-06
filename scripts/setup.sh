#!/bin/bash

# NYC Taxi Pipeline Setup Script

set -e

echo "======================================================================"
echo "NYC Taxi & HVFHV Data Pipeline - Setup"
echo "======================================================================"

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo " Installing uv package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

echo " uv is installed"

# Create virtual environment and install dependencies
echo " Creating virtual environment and installing dependencies..."
uv venv
source .venv/bin/activate
uv pip install -e .

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo " Creating .env file..."
    cp .env.example .env 2>/dev/null || echo "# Environment variables" > .env
    echo "PIPELINE_ENV=development" >> .env
    echo "LOG_LEVEL=INFO" >> .env
fi

# Create data directories
echo " Creating data directories..."
mkdir -p data/raw data/database data/logs

echo ""
echo "======================================================================"
echo " Setup complete!"
echo "======================================================================"
echo ""
echo "Next steps:"
echo "  1. Activate virtual environment: source .venv/bin/activate"
echo "  2. Run sample pipeline: ./scripts/run_sample.sh"
echo "  3. Or run full pipeline: ./scripts/run_pipeline.sh"
echo ""

