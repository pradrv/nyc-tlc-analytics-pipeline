# Virtual Environment Guide - UV

## The Issue: No `activate` Script

Your `.venv` was created by `uv`, which doesn't include the traditional `activate` script for bash/zsh. This is by design.

## Solution: Use `uv run` (Recommended)

`uv` provides a better way - you don't need to activate the virtual environment at all!

### Run Commands Directly

```bash
# Instead of activating and then running:
# source .venv/bin/activate  # ❌ Won't work with uv
# python -m src.cli db-stats

# Just use uv run:
uv run python -m src.cli db-stats  # ✅ Works!
```

### All Commands with `uv run`

```bash
# Database operations
uv run python -m src.cli init-db
uv run python -m src.cli db-stats

# Run analytics
uv run python -m src.cli run-analytics sql/analytics/01_top_zones_by_revenue.sql

# Run E2E pipeline
uv run python -m src.cli run-e2e --sample

# Run full pipeline
uv run python -m src.cli run-e2e --full
```

## Alternative: Use Python Directly

Since the virtual environment exists, you can call Python directly from the `.venv/bin` directory:

```bash
# Use the venv Python directly
.venv/bin/python -m src.cli db-stats

# Or use python3 with the venv
.venv/bin/python3 -m src.cli db-stats
```

## Alternative: Recreate with Standard venv (If You Need Activation)

If you really need the traditional `activate` script:

```bash
# 1. Remove the uv venv
rm -rf .venv

# 2. Create with standard Python venv
python3 -m venv .venv

# 3. Activate it (this will now work)
source .venv/bin/activate

# 4. Install dependencies with pip
pip install -e .
```

## Why UV is Better

`uv` is much faster than traditional pip/venv:
- 10-100x faster package installation
- No need to activate/deactivate
- Better dependency resolution
- Drop-in replacement for pip

## Updated Scripts

I'll update all the shell scripts to use `uv run`:

### Updated run_e2e.sh
```bash
#!/bin/bash
echo "Running SAMPLE E2E Pipeline (1 month)..."
uv run python -m src.cli run-e2e --sample
```

### Updated test_all_analytics.sh
```bash
#!/bin/bash
for query in $(ls sql/analytics/*.sql | sort); do
    uv run python -m src.cli run-analytics "$query"
done
```

## Quick Reference

| Old Way (won't work) | New Way (uv) |
|---------------------|--------------|
| `source .venv/bin/activate` | Not needed! |
| `python -m src.cli` | `uv run python -m src.cli` |
| `pip install package` | `uv pip install package` |
| `deactivate` | Not needed! |

## Running Analytics Now

To run all your analytics queries:

```bash
# Query 1: Top zones by revenue
uv run python -m src.cli run-analytics sql/analytics/01_top_zones_by_revenue.sql

# Query 11: Uber/Lyft vs Taxi pricing
uv run python -m src.cli run-analytics sql/analytics/11_uber_lyft_vs_taxi_pricing_by_zone_hour.sql

# Query 12: CBD congestion fee impact
uv run python -m src.cli run-analytics sql/analytics/12_cbd_congestion_fee_impact.sql

# Run all queries (using updated script)
./test_all_analytics.sh
```

## Environment Variables

If you need to set environment variables:

```bash
# Set for single command
DATABASE_PATH=/custom/path uv run python -m src.cli db-stats

# Or export first
export DATABASE_PATH=/custom/path
uv run python -m src.cli db-stats
```

---

**Bottom Line:** With `uv`, just use `uv run python ...` instead of activating!

