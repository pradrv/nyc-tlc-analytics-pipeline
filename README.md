# NYC Taxi & HVFHV Data Pipeline

**End-to-end ETL pipeline for analyzing NYC TLC trip data (2021-2025)**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/database-DuckDB-yellow.svg)](https://duckdb.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Overview

This project implements a production-quality data pipeline that:

- **Ingests** NYC Taxi & Limousine Commission (TLC) trip records for:
  - Yellow Taxis
  - Green Taxis  
  - High-Volume For-Hire Vehicles (HVFHV: Uber, Lyft, Via, Juno)

- **Processes** 49 months of data (January 2021 - January 2025)

- **Enables** analytics on:
  - Pricing comparison ($/mile, $/minute) across services
  - Congestion fee impact (before/after Jan 5, 2025)
  - HVFHV platform take-rates and driver economics
  - Market share shifts by zone and time

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES (147 files)                    │
│  Yellow Taxi | Green Taxi | HVFHV | Taxi Zone Lookup           │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              INGESTION (Async Downloads + Validation)            │
│  • httpx async client (10 concurrent downloads)                  │
│  • SHA256 checksums                                              │
│  • Retry logic with exponential backoff                          │
│  • Row count & schema drift detection                            │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                  RAW/STAGING (DuckDB Tables)                     │
│  • raw_yellow, raw_green, raw_hvfhv                             │
│  • Stores data as-is from source                                 │
│  • Idempotent inserts (no duplicates on re-run)                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│           DATA QUALITY & TRANSFORMATION (SQL + Python)           │
│  • Fare validation (non-negative, realistic)                     │
│  • Timestamp ordering checks                                     │
│  • Speed validation (<100 mph)                                   │
│  • Deduplication                                                 │
│  • Schema standardization across services                        │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ANALYTICS LAYER (Star Schema)                   │
│  Fact: fact_trips (unified 370M+ rows)                          │
│  Dims: dim_zones, dim_date, dim_time, dim_service               │
│  Aggs: Pricing, Take Rates, Market Share, Congestion Impact     │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS & INSIGHTS                          │
│  • SQL queries for business questions                            │
│  • Ready for BI tools, APIs, and downstream analytics            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- **uv** package manager (will be installed by setup script)
- **10GB+ disk space** for data

### Installation

```bash
# Clone repository
git clone <repo-url>
cd data_pipeline

# Run setup (installs uv, creates venv, installs dependencies)
./scripts/setup.sh

# Note: With uv, no need to activate! Use 'uv run' for commands
# See VIRTUAL_ENV_GUIDE.md for details
```

### Run Sample Pipeline (1 month)

```bash
# Download, load, and process sample data
uv run python -m src.cli run-e2e --sample

# This will:
# 1. Initialize database schema
# 2. Download 1 sample month (June 2024)
# 3. Load taxi zone lookup
# 4. Load trip data to raw tables
# 5. Run quality checks
# 6. Transform to fact tables
# 7. Build aggregate tables
```

### Run Full Pipeline (49 months)

```bash
# Download and process all data (2021-01 to 2025-01)
uv run python -m src.cli run-e2e --full

# Note: This downloads ~60-80GB and takes several hours
```

---

## 📊 Project Structure

```
data_pipeline/
├── README.md                      # This file  
├── docs/                           # Documentation
│   ├── QUICK_START.md            # Step-by-step setup guide
│   ├── IMPLEMENTATION_GUIDE.md   # Detailed implementation guide
│   ├── BUSINESS_INSIGHTS.md      # Analytics query explanations
│   ├── MONITORING_AND_SCHEDULING.md  # Pipeline monitoring & scheduling
│   ├── VIRTUAL_ENV_GUIDE.md      # UV virtual environment usage
│   └── CONTRIBUTING.md           # Contribution guidelines
├── pyproject.toml                 # uv package configuration
├── .python-version                # Python version (3.11)
│
├── config/
│   └── pipeline_config.yaml       # Pipeline configuration
│
├── src/
│   ├── cli.py                     # Command-line interface
│   ├── config.py                  # Configuration management
│   ├── utils.py                   # Utility functions
│   │
│   ├── ingestion/
│   │   ├── downloader.py         # Async HTTP downloader
│   │   └── validators.py         # File validation
│   │
│   ├── database/
│   │   ├── connection.py         # DuckDB connection manager
│   │   ├── schema.py             # Schema initialization
│   │   └── loader.py             # Data loading
│   │
│   ├── transformations/
│   │   ├── quality_checks.py     # Data quality validation
│   │   ├── standardize.py        # Schema standardization
│   │   └── aggregations.py       # Aggregate table builders
│   │
│   └── orchestration/
│       └── flows.py              # Prefect orchestration flows
│
├── sql/
│   ├── ddl/
│   │   ├── 00_schema_reference.sql   # Source schema documentation
│   │   ├── 01_raw_tables.sql         # Raw/staging tables
│   │   ├── 02_dimension_tables.sql   # Dimension tables
│   │   ├── 03_fact_tables.sql        # Fact tables
│   │   └── 04_aggregate_tables.sql   # Aggregate tables
│   │
│   └── analytics/
│       ├── 01_top_zones_by_revenue.sql
│       ├── 02_hourly_demand_patterns.sql
│       ├── ...
│       └── 14_market_share_shift_vs_pricing.sql
│
├── data/
│   ├── raw/                      # Downloaded parquet files (gitignored)
│   ├── database/                 # DuckDB files (gitignored)
│   └── logs/                     # Log files (gitignored)
│
├── scripts/
│   ├── setup.sh                  # Setup script
│   ├── run_e2e.sh                # Run E2E pipeline
│   └── run_pipeline.sh           # Run full pipeline
│
├── test_all_analytics.sh         # Test all SQL queries
├── start_prefect_ui.sh           # Start Prefect UI
│
├── Dockerfile                    # Docker image
├── docker-compose.yml            # Docker services
│
├── tests/
│   └── test_*.py                 # Test suite (future)
│
```

---

## 🗄️ Database Schema

### Raw Tables
- `raw_yellow` - Yellow taxi trips (as-is from source)
- `raw_green` - Green taxi trips
- `raw_hvfhv` - HVFHV trips (Uber, Lyft, Via, Juno)
- `raw_taxi_zones` - Zone lookup

### Fact Table
- `fact_trips` - Unified, standardized trip records (370M+ rows)
  - Fields: pickup/dropoff timestamps, zones, distance, duration, fares, driver_pay, take_rate

### Dimension Tables
- `dim_zones` - Taxi zones (263 zones across 5 boroughs)
- `dim_date` - Date dimension (2021-2025)
- `dim_time` - Hour dimension (0-23)
- `dim_service` - Service types (yellow, green, hvfhv)
- `dim_hvfhs_company` - HVFHS license lookup (Uber, Lyft, etc.)

### Aggregate Tables (Performance)
- `agg_pricing_by_zone_hour` - Pre-computed pricing metrics
- `agg_hvfhv_take_rates` - Platform commission analysis
- `agg_market_share` - Market share by zone/day
- `agg_congestion_fee_impact` - Before/after Jan 5, 2025 analysis

---

## 📈 Usage Examples

### CLI Commands

```bash
# Initialize database
uv run python -m src.cli init-db

# Show database statistics
uv run python -m src.cli db-stats

# Run E2E pipeline (recommended)
uv run python -m src.cli run-e2e --sample  # 1 month
uv run python -m src.cli run-e2e --full    # 49 months

# Run analytics query
uv run python -m src.cli run-analytics sql/analytics/01_top_zones_by_revenue.sql

# Test all queries
./test_all_analytics.sh

# Individual pipeline steps (advanced)
uv run python -m src.cli download --sample
uv run python -m src.cli load
uv run python -m src.cli quality-check
uv run python -m src.cli transform
uv run python -m src.cli build-aggregates
```

### Python API

```python
from src.database.connection import DatabaseConnection
from src.database.schema import SchemaManager
import pandas as pd

# Connect to database
conn = DatabaseConnection.get_connection()

# Query data
df = conn.execute("""
    SELECT 
        service_type,
        COUNT(*) as trips,
        AVG(price_per_mile) as avg_price_per_mile
    FROM fact_trips
    WHERE pickup_date = '2024-06-01'
    GROUP BY service_type
""").df()

print(df)
```

### SQL Analytics

```sql
-- Compare HVFHV vs Taxi pricing in Manhattan
SELECT 
    CASE 
        WHEN service_type = 'hvfhv' THEN 'Uber/Lyft'
        ELSE 'Traditional Taxi'
    END AS service_category,
    z.zone,
    COUNT(*) as trips,
    AVG(price_per_mile) as avg_price_per_mile,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_mile) as median_price_per_mile
FROM fact_trips t
JOIN dim_zones z ON t.pickup_zone_id = z.location_id
WHERE z.borough = 'Manhattan'
    AND t.pickup_date >= '2024-01-01'
    AND t.is_valid = TRUE
GROUP BY service_category, z.zone
ORDER BY z.zone, service_category;
```

---

## 🔍 Key Features

### Idempotent & Incremental-Ready
- ✅ Can re-run safely without duplicates
- ✅ Supports loading specific month ranges
- ✅ Skips already-downloaded files
- ✅ Uses checksums to verify data integrity

### Data Quality Checks
- ✅ Non-negative fares
- ✅ Ordered timestamps (dropoff ≥ pickup)
- ✅ Realistic speeds (<100 mph)
- ✅ Schema drift detection
- ✅ Deduplication tracking
- ✅ Zero-division safe calculations

### Performance
- ✅ Async downloads (10 concurrent)
- ✅ DuckDB columnar storage (fast analytics)
- ✅ Pre-computed aggregate tables
- ✅ Efficient indexes on common query patterns
- ✅ Native Parquet support (no conversion needed)

### Reproducibility
- ✅ Single DuckDB file (portable)
- ✅ Docker support for consistent environment
- ✅ Comprehensive logging
- ✅ Complete metadata tracking

---

## 📊 Analytics Questions Answered

### 1. **Pricing Comparison**
*Are Uber/Lyft pricing $/mile and $/minute materially higher than Yellow/Green by zone and hour?*

**Query:** `sql/analytics/pricing_analysis.sql`

**Key Insights:**
- Compare median price_per_mile across service types
- By zone (263 zones) and hour (0-23)
- Statistical significance testing

### 2. **Congestion Fee Impact**
*How did rider prices change before vs. after Jan 5, 2025 (CBD congestion fee)? Which operators passed through more?*

**Query:** `sql/analytics/congestion_fee_impact.sql`

**Key Insights:**
- Before/after fare comparison
- Fee adoption rates by service
- Pass-through analysis (fare increase beyond fee)

### 3. **HVFHV Take Rates**
*What are HVFHV take-rates over time (median, p25/p75)? Which factors explain variance?*

**Query:** `sql/analytics/hvfhv_take_rates.sql`

**Key Insights:**
- Platform commission rates (Uber, Lyft, Via, Juno)
- Variance by zone, hour, trip length
- Driver economics analysis

### 4. **Market Share**
*Where is market share shifting (operator share of trips by zone/day), and is share correlated with relative price levels?*

**Query:** `sql/analytics/market_share.sql`

**Key Insights:**
- Trip share by service type
- Correlation with pricing
- Temporal trends

---

## 🛠️ Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Package Manager** | uv | Fast, modern Python package manager |
| **Database** | DuckDB | Embedded, columnar, excellent Parquet support |
| **Orchestration** | Prefect | Python-native, easy local execution |
| **Async I/O** | httpx | Parallel downloads with retry logic |
| **Data Processing** | Pandas + SQL | Familiar, efficient |
| **Containerization** | Docker | Reproducible environment |
| **Logging** | Loguru | Beautiful, structured logging |
| **CLI** | Click | User-friendly command-line interface |

---

## 📝 Database Choice: DuckDB

**Why DuckDB over PostgreSQL/Spark/SQLite?**

✅ **Embedded** - No server, single file, works anywhere  
✅ **Native Parquet** - Reads Parquet files directly without conversion  
✅ **Columnar** - 10-100x faster for analytical queries  
✅ **Zero Config** - No setup, users, passwords  
✅ **Portable** - Copy one file to share entire database  
✅ **Modern SQL** - Full SQL:2016 support including window functions, percentiles  

For production at scale, consider:
- **PostgreSQL** for transactional workloads
- **Snowflake/BigQuery** for cloud-scale, team collaboration
- **ClickHouse** for billions of rows, real-time dashboards

The pipeline architecture remains the same; only the connection string changes.

---

## 🧪 Testing

```bash
# Run all 63 tests (passes in ~0.5s)
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_utils.py -v
```

**Test Coverage:** 63 tests covering utilities, database operations, validators, quality checks, and transformations.

---

## 📦 Docker Usage

```bash
# Build and run pipeline
docker-compose up pipeline

# Run Prefect UI for monitoring
docker-compose --profile monitoring up prefect-server
# Open browser to http://localhost:4200
```

---

## 📋 Data Sources

- **Yellow Taxi:** https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
- **Green Taxi:** https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_YYYY-MM.parquet
- **HVFHV:** https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_YYYY-MM.parquet
- **Taxi Zones:** https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

### Data Dictionaries
- [Yellow Taxi](https://nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Green Taxi](https://nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)
- [HVFHV](https://nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf)

---

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

---

## 📚 Additional Documentation

All detailed guides are in the **[docs/](docs/)** folder:

- **[Quick Start Guide](docs/QUICK_START.md)** - Get started in 5 minutes
- **[Implementation Guide](docs/IMPLEMENTATION_GUIDE.md)** - Complete technical details
- **[Business Insights](docs/BUSINESS_INSIGHTS.md)** - Analytics answers
- **[Monitoring & Scheduling](docs/MONITORING_AND_SCHEDULING.md)** - Production deployment
- **[Virtual Environment Guide](docs/VIRTUAL_ENV_GUIDE.md)** - Understanding `uv`
- **[Contributing](docs/CONTRIBUTING.md)** - How to contribute

See the **[docs/README.md](docs/README.md)** for a complete documentation index.

---

## 📄 License

MIT License - see LICENSE file for details

---

## 👤 Author

**Pradeep Vishwakarma**  
Data Engineering Case Study

---

## 🙏 Acknowledgments

- NYC Taxi & Limousine Commission for open data
- DuckDB team for an amazing database
- Prefect team for modern orchestration

---

**Built with ❤️ for quantamental research**

