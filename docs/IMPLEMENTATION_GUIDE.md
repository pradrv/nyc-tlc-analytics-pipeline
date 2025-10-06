# Implementation Guide

This guide provides detailed implementation information for developers working with the NYC Taxi & HVFHV Data Pipeline. For high-level overview and quick start, see **README.md**.

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Pipeline Architecture Deep Dive](#pipeline-architecture-deep-dive)
3. [Module Implementation Details](#module-implementation-details)
4. [Database Design & Schema](#database-design--schema)
5. [Data Quality Framework](#data-quality-framework)
6. [Extending the Pipeline](#extending-the-pipeline)
7. [Performance Optimization](#performance-optimization)
8. [Troubleshooting](#troubleshooting)

---

## Development Environment Setup

### Using UV Package Manager

This project uses `uv` instead of traditional `pip` for faster dependency management.

**Why UV?**
- 10-100x faster package installation
- Better dependency resolution
- No need to activate virtual environments
- Drop-in replacement for pip

**Installation:**
```bash
# Install uv (automatically done by setup.sh)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify installation
uv --version
```

**Running Commands:**
```bash
# Instead of activating venv:
source .venv/bin/activate  # ❌ Won't work with uv
python -m src.cli db-stats

# Use uv run directly:
uv run python -m src.cli db-stats  # ✅ Correct way
```

### Environment Configuration

Create `.env` file in project root:

```bash
# Environment
PIPELINE_ENV=development  # or production
LOG_LEVEL=INFO           # DEBUG, INFO, WARNING, ERROR

# Database
DUCKDB_PATH=data/database/nyc_taxi.duckdb

# Performance tuning
MAX_WORKERS=10           # Concurrent downloads
BATCH_SIZE=50000        # Transformation batch size

# Memory limits (for 32GB Mac)
DUCKDB_MEMORY_LIMIT=8GB
DUCKDB_THREADS=4
DUCKDB_TEMP_DIR_SIZE=50GB
```

### IDE Setup

**VS Code Extensions (Recommended):**
- Python (Microsoft) - Required
- SQLTools with DuckDB driver - Recommended for SQL editing
- YAML - Helpful for config files
- Docker - Optional (only if using containers)

**Settings:**
```json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "python.linting.enabled": true,
  "python.formatting.provider": "black",
  "[sql]": {
    "editor.defaultFormatter": "mtxr.sqltools"
  }
}
```

**Note:** This pipeline works perfectly without Docker. Docker is provided for optional production deployment and team collaboration scenarios.

---

## Pipeline Architecture Deep Dive

### Data Flow Stages

#### 1. Ingestion Stage

**Components:**
- `src/ingestion/downloader.py` - Async HTTP downloads
- `src/ingestion/validators.py` - File validation

**Process:**
```python
async def download_and_validate(url: str, dest: Path):
    # 1. Check if file exists (skip if valid)
    if dest.exists() and validate_checksum(dest):
        return
    
    # 2. Download with retries (3 attempts, exponential backoff)
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=300.0)
    
    # 3. Write to disk
    async with aiofiles.open(dest, 'wb') as f:
        await f.write(response.content)
    
    # 4. Validate (row count, schema, checksum)
    metadata = validate_parquet(dest)
    
    # 5. Log to ingestion_log table
    log_ingestion(metadata)
```

**Key Features:**
- **Concurrency**: 10 files download simultaneously
- **Resume**: Skips already-downloaded valid files
- **Validation**: Schema drift detection, row count verification
- **Idempotency**: Can re-run safely

#### 2. Loading Stage

**Process:**
```sql
-- DuckDB can read Parquet directly
INSERT INTO raw_yellow 
SELECT *, 
    'yellow_tripdata_2024-06.parquet' as source_file,
    CURRENT_TIMESTAMP as ingestion_timestamp
FROM parquet_scan('data/raw/yellow_tripdata_2024-06.parquet');
```

**Key Features:**
- **Native Parquet**: No conversion needed
- **Batch Insert**: Efficient bulk loading
- **Metadata Tracking**: source_file, ingestion_timestamp

#### 3. Quality Check Stage

**Checks Implemented:**

| Check | SQL Logic | Failure Threshold |
|-------|-----------|-------------------|
| Non-negative fares | `total_fare >= 0 AND fare_amount >= 0` | >5% |
| Ordered timestamps | `dropoff_datetime > pickup_datetime` | >2% |
| Realistic speed | `(trip_distance / trip_duration) * 3600 < 100` | >10% |
| Distance validation | `trip_distance >= 0` | >1% |

**Implementation:**
```python
def check_fares(table_name: str) -> dict:
    sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            SUM(CASE 
                WHEN total_fare < 0 OR fare_amount < 0 
                THEN 1 ELSE 0 
            END) as failed_rows
        FROM {table_name}
    """
    result = conn.execute(sql).fetchone()
    
    return {
        'check_type': 'fare_validation',
        'total_rows': result[0],
        'failed_rows': result[1],
        'failure_rate': result[1] / result[0] * 100 if result[0] > 0 else 0
    }
```

#### 4. Transformation Stage

**Standardization Logic:**

```sql
INSERT INTO fact_trips (
    trip_id,
    service_type,
    pickup_datetime,
    dropoff_datetime,
    pickup_zone_id,
    dropoff_zone_id,
    trip_distance_miles,
    trip_duration_minutes,
    base_fare,
    tips,
    tolls,
    surcharges,
    total_fare,
    price_per_mile,
    price_per_minute,
    avg_speed_mph,
    is_valid
)
SELECT 
    -- Generate unique trip_id
    MD5(CONCAT(
        'yellow',
        CAST(tpep_pickup_datetime AS VARCHAR),
        CAST(tpep_dropoff_datetime AS VARCHAR),
        CAST(trip_distance AS VARCHAR),
        CAST(total_amount AS VARCHAR)
    )) as trip_id,
    
    'yellow' as service_type,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    PULocationID as pickup_zone_id,
    DOLocationID as dropoff_zone_id,
    
    trip_distance as trip_distance_miles,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 as trip_duration_minutes,
    
    fare_amount as base_fare,
    tip_amount as tips,
    tolls_amount as tolls,
    extra + mta_tax + congestion_surcharge + Airport_fee as surcharges,
    total_amount as total_fare,
    
    -- Calculated metrics
    CASE 
        WHEN trip_distance > 0.1 
        THEN total_amount / trip_distance 
        ELSE NULL 
    END as price_per_mile,
    
    CASE 
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 60
        THEN total_amount / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0)
        ELSE NULL
    END as price_per_minute,
    
    CASE 
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0
        THEN trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0)
        ELSE NULL
    END as avg_speed_mph,
    
    -- Validation flag
    CASE 
        WHEN total_amount >= 0 
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        AND trip_distance >= 0
        AND (trip_distance / NULLIF(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0, 0)) < 100
        THEN TRUE
        ELSE FALSE
    END as is_valid

FROM raw_yellow
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL;
```

**Key Features:**
- **Schema Unification**: Different source schemas → single fact table
- **Derived Metrics**: price_per_mile, price_per_minute, take_rate
- **Quality Flag**: `is_valid` for filtering bad data
- **NULL Safety**: NULLIF, CASE statements prevent division by zero

#### 5. Aggregation Stage

Pre-compute common queries for performance:

```sql
CREATE TABLE agg_pricing_by_zone_hour AS
SELECT 
    z.zone,
    z.borough,
    t.hour,
    f.service_type,
    DATE_TRUNC('month', f.pickup_date) as month,
    
    COUNT(*) as trip_count,
    AVG(f.price_per_mile) as avg_price_per_mile,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price_per_mile) as median_price_per_mile,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.price_per_mile) as p25_price_per_mile,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.price_per_mile) as p75_price_per_mile
    
FROM fact_trips f
JOIN dim_zones z ON f.pickup_zone_id = z.location_id
JOIN dim_time t ON f.pickup_hour = t.hour
WHERE f.is_valid = TRUE
GROUP BY z.zone, z.borough, t.hour, f.service_type, DATE_TRUNC('month', f.pickup_date);
```

---

## Module Implementation Details

### 1. Configuration Management (`src/config.py`)

```python
class Config:
    """Centralized configuration"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        with open(config_path) as f:
            self._config = yaml.safe_load(f)
        
        # Override with environment variables
        self._config['database']['path'] = os.getenv(
            'DUCKDB_PATH', 
            self._config['database']['path']
        )
    
    @property
    def date_range(self) -> dict:
        """Get configured date range"""
        return self._config['date_range']
    
    @property
    def database_path(self) -> Path:
        """Get database path"""
        return Path(self._config['database']['path'])
    
    def get_source_url(self, service_type: str, year_month: str) -> str:
        """Generate download URL"""
        template = self._config['data_sources'][service_type]['url_template']
        return template.replace('{year_month}', year_month)
```

### 2. Database Connection (`src/database/connection.py`)

**Singleton Pattern:**
```python
class DatabaseConnection:
    _instance = None
    _connection = None
    
    @classmethod
    def get_connection(cls) -> duckdb.DuckDBPyConnection:
        """Get or create database connection (singleton)"""
        if cls._connection is None:
            db_path = config.database_path
            cls._connection = duckdb.connect(str(db_path))
            
            # Performance tuning
            cls._connection.execute("SET memory_limit='8GB'")
            cls._connection.execute("SET threads=4")
            cls._connection.execute("SET preserve_insertion_order=false")
            
        return cls._connection
```

**Why Singleton?**
- Prevent multiple connections to same file
- Share connection across modules
- Avoid locking issues

### 3. Schema Management (`src/database/schema.py`)

```python
class SchemaManager:
    
    @staticmethod
    def initialize_database():
        """Initialize all tables and dimensions"""
        ddl_files = [
            'sql/ddl/01_raw_tables.sql',
            'sql/ddl/02_dimension_tables.sql',
            'sql/ddl/03_fact_tables.sql',
            'sql/ddl/04_aggregate_tables.sql'
        ]
        
        for ddl_file in ddl_files:
            # Skip documentation files
            if '00_' in ddl_file:
                continue
                
            DatabaseConnection.execute_sql_file(Path(ddl_file))
        
        # Load taxi zones
        SchemaManager.load_taxi_zones()
```

### 4. Data Loader (`src/database/loader.py`)

**Schema Drift Handling:**

```python
class DataLoader:
    EXPECTED_SCHEMAS = {
        'yellow': [
            'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'passenger_count', 'trip_distance', 'RatecodeID',
            'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
            'payment_type', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'improvement_surcharge',
            'total_amount', 'congestion_surcharge', 'Airport_fee'
        ],
        'green': [
            'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
            'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID',
            'passenger_count', 'trip_distance', 'fare_amount', 'extra',
            'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee',
            'improvement_surcharge', 'total_amount', 'payment_type',
            'trip_type', 'congestion_surcharge'
        ],
        'hvfhv': [
            'hvfhs_license_num', 'dispatching_base_num', 'originating_base_num',
            'request_datetime', 'on_scene_datetime', 'pickup_datetime',
            'dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_miles',
            'trip_time', 'base_passenger_fare', 'tolls', 'bcf', 'sales_tax',
            'congestion_surcharge', 'airport_fee', 'tips', 'driver_pay',
            'shared_request_flag', 'shared_match_flag', 'access_a_ride_flag',
            'wav_request_flag', 'wav_match_flag'
        ]
    }
    
    @staticmethod
    def check_schema_drift(file_name: str, actual_cols: list, expected_cols: list):
        """Detect schema changes"""
        missing = set(expected_cols) - set(actual_cols)
        extra = set(actual_cols) - set(expected_cols)
        
        if missing or extra:
            logger.warning(f"Schema drift in {file_name}")
            if missing:
                logger.warning(f"  Missing: {missing}")
            if extra:
                logger.warning(f"  Extra: {extra}")
```

### 5. Quality Checks (`src/transformations/quality_checks.py`)

**Idempotent Checks:**

```python
@staticmethod
def run_all_checks(table_name: str) -> List[Dict]:
    """Run all quality checks (idempotent)"""
    conn = DatabaseConnection.get_connection()
    
    # Delete existing checks for this table
    conn.execute(
        "DELETE FROM data_quality_metrics WHERE service_type = ?", 
        [table_name]
    )
    
    # Run checks
    checks = [
        DataQualityChecker.check_fares(table_name),
        DataQualityChecker.check_timestamps(table_name),
        DataQualityChecker.check_realistic_speed(table_name),
        DataQualityChecker.check_distance(table_name)
    ]
    
    # Insert new checks
    for check in checks:
        conn.execute("""
            INSERT INTO data_quality_metrics (
                check_id, service_type, check_type, total_rows,
                passed_rows, failed_rows, failure_rate, details
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            f"{table_name}_{check['check_type']}_{datetime.now().timestamp()}",
            table_name,
            check['check_type'],
            check['total_rows'],
            check['passed_rows'],
            check['failed_rows'],
            check['failure_rate'],
            check['details']
        ])
    
    return checks
```

### 6. Transformations (`src/transformations/standardize.py`)

**Memory Optimization for Large Tables:**

```python
@staticmethod
def transform_hvfhv_to_fact():
    """Transform HVFHV in batches to avoid OOM"""
    conn = DatabaseConnection.get_connection()
    
    # Memory settings for 32GB Mac
    conn.execute("SET memory_limit='8GB'")
    conn.execute("SET threads=4")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET max_temp_directory_size='50GB'")
    
    # Get total rows
    total_rows = conn.execute("SELECT COUNT(*) FROM raw_hvfhv").fetchone()[0]
    
    # Process in batches
    batch_size = 5_000_000  # 5M rows
    offset = 0
    
    while offset < total_rows:
        logger.info(f"Processing batch: rows {offset:,} to {offset + batch_size:,}")
        
        sql = f"""
            INSERT OR IGNORE INTO fact_trips (...)
            SELECT ...
            FROM raw_hvfhv
            WHERE pickup_datetime IS NOT NULL
            ORDER BY pickup_datetime
            LIMIT {batch_size} OFFSET {offset}
        """
        
        conn.execute(sql)
        offset += batch_size
```

**Why Batching?**
- Prevents out-of-memory errors
- Allows progress tracking
- Enables partial recovery on failure

### 7. Orchestration (`src/orchestration/flows.py`)

**Prefect Flow Example:**

```python
@flow(name="full-pipeline-e2e")
async def full_pipeline_flow(
    service_types: List[str],
    year_months: List[str],
    skip_download: bool = False
) -> dict:
    """Complete E2E pipeline orchestration"""
    
    # Phase 1: Initialize
    init_task()
    
    # Phase 2: Ingestion
    if not skip_download:
        await ingestion_flow(service_types, year_months)
    
    load_count = load_data_task()
    
    # Phase 3: Quality
    quality_results = quality_check_task()
    
    # Phase 4: Transform
    transform_results = transform_task()
    
    # Phase 5: Aggregate
    agg_results = aggregate_task()
    
    return {
        'pipeline_status': 'success',
        'ingestion': {'total_rows_loaded': load_count},
        'quality': quality_results,
        'transformation': transform_results,
        'aggregation': agg_results
    }
```

---

## Database Design & Schema

### Star Schema Design

```
                    ┌──────────────┐
                    │   dim_date   │
                    └──────┬───────┘
                           │
    ┌──────────────┐       │       ┌──────────────┐
    │  dim_zones   │◄──────┼──────►│  dim_time    │
    └──────┬───────┘       │       └──────┬───────┘
           │               │              │
           │       ┌───────▼────────┐     │
           └──────►│  fact_trips    │◄────┘
                   └───────┬────────┘
                           │
                           ▼
            ┌──────────────────────────┐
            │  Aggregate Tables        │
            │  • agg_pricing_*         │
            │  • agg_take_rates        │
            │  • agg_market_share      │
            └──────────────────────────┘
```

### Indexing Strategy

```sql
-- Fact table indexes (query performance)
CREATE INDEX idx_fact_pickup_date ON fact_trips(pickup_date);
CREATE INDEX idx_fact_service_type ON fact_trips(service_type);
CREATE INDEX idx_fact_pickup_zone ON fact_trips(pickup_zone_id);
CREATE INDEX idx_fact_valid ON fact_trips(is_valid);

-- Composite index for common query pattern
CREATE INDEX idx_fact_date_service ON fact_trips(pickup_date, service_type);
```

### Partitioning Considerations

For production at scale:

```sql
-- Monthly partitioning (future enhancement)
CREATE TABLE fact_trips_2024_01 AS
SELECT * FROM fact_trips
WHERE pickup_date >= '2024-01-01' AND pickup_date < '2024-02-01';

-- Query partition pruning
SELECT * FROM fact_trips_2024_01
WHERE pickup_date = '2024-01-15';  -- Only scans one partition
```

---

## Data Quality Framework

### Quality Dimensions

| Dimension | Checks | Threshold |
|-----------|--------|-----------|
| **Completeness** | NULL counts in required fields | <1% |
| **Validity** | Range checks, format validation | <5% |
| **Accuracy** | Cross-field validation, speed checks | <2% |
| **Consistency** | Schema drift, duplicate detection | 0% |
| **Timeliness** | Ingestion lag tracking | <24h |

### Quality Metrics Storage

```sql
CREATE TABLE data_quality_metrics (
    check_id VARCHAR PRIMARY KEY,
    service_type VARCHAR,
    check_type VARCHAR,
    total_rows BIGINT,
    passed_rows BIGINT,
    failed_rows BIGINT,
    failure_rate DOUBLE,
    details VARCHAR,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Quality Report Query

```sql
SELECT 
    service_type,
    check_type,
    total_rows,
    failed_rows,
    ROUND(failure_rate, 2) as failure_pct,
    check_timestamp
FROM data_quality_metrics
WHERE check_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY failure_rate DESC;
```

---

## Extending the Pipeline

### Adding a New Data Source

**1. Update Configuration:**

```yaml
# config/pipeline_config.yaml
data_sources:
  fhv:  # For-Hire Vehicle (non-HVFHV)
    url_template: "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year_month}.parquet"
    enabled: true
```

**2. Create Raw Table:**

```sql
-- sql/ddl/01_raw_tables.sql
CREATE TABLE IF NOT EXISTS raw_fhv (
    dispatching_base_num VARCHAR,
    pickup_datetime TIMESTAMP,
    dropOff_datetime TIMESTAMP,
    PUlocationID INTEGER,
    DOlocationID INTEGER,
    SR_Flag VARCHAR,
    Affiliated_base_number VARCHAR,
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**3. Add Transformation:**

```python
# src/transformations/standardize.py
@staticmethod
def transform_fhv_to_fact():
    sql = """
        INSERT INTO fact_trips (...)
        SELECT 
            MD5(...) as trip_id,
            'fhv' as service_type,
            ...
        FROM raw_fhv
        WHERE pickup_datetime IS NOT NULL
    """
    conn.execute(sql)
```

**4. Update CLI:**

```python
# src/cli.py
@cli.command()
@click.option('--service-type', default='all', 
              type=click.Choice(['yellow', 'green', 'hvfhv', 'fhv', 'all']))
def download(service_type):
    # existing code...
```

### Adding Custom Analytics Query

**1. Create SQL File:**

```sql
-- sql/analytics/15_custom_analysis.sql
-- Purpose: Analyze surge pricing patterns
-- Use case: Identify when and where surge is highest

SELECT 
    z.zone,
    z.borough,
    t.hour,
    DATE_TRUNC('hour', f.pickup_datetime) as hour_bucket,
    
    COUNT(*) as trip_count,
    AVG(f.total_fare) as avg_fare,
    AVG(f.price_per_mile) as avg_price_per_mile,
    
    -- Surge indicator (price > 1.5x daily average)
    CASE 
        WHEN AVG(f.price_per_mile) > 1.5 * (
            SELECT AVG(price_per_mile) 
            FROM fact_trips 
            WHERE pickup_date = CAST(f.pickup_datetime AS DATE)
        )
        THEN 'SURGE'
        ELSE 'NORMAL'
    END as pricing_mode

FROM fact_trips f
JOIN dim_zones z ON f.pickup_zone_id = z.location_id
JOIN dim_time t ON f.pickup_hour = t.hour
WHERE f.service_type = 'hvfhv'
  AND f.is_valid = TRUE
GROUP BY z.zone, z.borough, t.hour, hour_bucket
HAVING COUNT(*) >= 10
ORDER BY avg_price_per_mile DESC
LIMIT 100;
```

**2. Run Query:**

```bash
uv run python -m src.cli run-analytics sql/analytics/15_custom_analysis.sql
```

---

## Performance Optimization

### DuckDB Settings

```python
# Optimal settings for 32GB Mac
conn.execute("SET memory_limit='8GB'")          # Use 25% of RAM
conn.execute("SET threads=4")                   # CPU cores
conn.execute("SET max_temp_directory_size='50GB'")  # Temp space
conn.execute("SET preserve_insertion_order=false")  # Faster inserts
```

### Query Optimization Tips

**1. Use aggregate tables:**
```sql
-- Slow (scan 370M rows)
SELECT AVG(price_per_mile) FROM fact_trips WHERE pickup_date = '2024-06-01';

-- Fast (scan pre-aggregated table)
SELECT avg_price_per_mile FROM agg_daily_summary WHERE trip_date = '2024-06-01';
```

**2. Filter early:**
```sql
-- Bad: Filter after JOIN
SELECT * FROM fact_trips f
JOIN dim_zones z ON f.pickup_zone_id = z.location_id
WHERE f.pickup_date = '2024-06-01';

-- Good: Filter before JOIN
SELECT * FROM (
    SELECT * FROM fact_trips WHERE pickup_date = '2024-06-01'
) f
JOIN dim_zones z ON f.pickup_zone_id = z.location_id;
```

**3. Use column projection:**
```sql
-- Bad: SELECT *
SELECT * FROM fact_trips WHERE pickup_date = '2024-06-01';

-- Good: Only needed columns
SELECT trip_id, pickup_datetime, total_fare 
FROM fact_trips 
WHERE pickup_date = '2024-06-01';
```

### Monitoring Performance

```python
# Enable query profiling
conn.execute("PRAGMA enable_profiling")

# Run query
result = conn.execute("SELECT ...").fetchall()

# View profile
profile = conn.execute("SELECT * FROM pragma_last_profiling_output()").fetchdf()
print(profile)
```

---

## Troubleshooting

### Common Issues

#### 1. Out of Memory Errors

**Symptom:**
```
OutOfMemoryException: failed to allocate block of size 256.0 KiB
```

**Solutions:**
```python
# Reduce memory limit
conn.execute("SET memory_limit='4GB'")

# Use batching
for batch in range(0, total_rows, batch_size):
    sql = f"... LIMIT {batch_size} OFFSET {batch}"
    conn.execute(sql)

# Clear fact_trips before transformation
conn.execute("DELETE FROM fact_trips")
```

#### 2. Schema Drift Warnings

**Symptom:**
```
Schema drift detected: Missing columns: ['Airport_fee']
```

**Solutions:**
```python
# Update expected schema in src/database/loader.py
EXPECTED_SCHEMAS = {
    'yellow': [
        ...,
        'Airport_fee',  # Use capital A
    ]
}

# Or update DDL in sql/ddl/01_raw_tables.sql
CREATE TABLE raw_yellow (
    ...
    Airport_fee DOUBLE,  -- Match source casing
);
```

#### 3. Database Locked

**Symptom:**
```
IO Error: Could not set lock on file: Conflicting lock is held
```

**Solutions:**
```bash
# Close all connections
# Remove lock files
rm -f data/database/nyc_taxi.duckdb.wal
rm -f data/database/nyc_taxi.duckdb.tmp

# Restart pipeline
uv run python -m src.cli init-db
```

#### 4. Virtual Environment Activation

**Symptom:**
```
source: no such file or directory: .venv/bin/activate
```

**Solution:**
```bash
# Don't activate with uv! Just use uv run:
uv run python -m src.cli db-stats
```

See [VIRTUAL_ENV_GUIDE.md](VIRTUAL_ENV_GUIDE.md) for details.

---

## Testing

### Running Tests

The project includes a comprehensive test suite with 63 unit tests covering all core functionality:

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_utils.py -v

# Run with coverage
uv run pytest tests/ --cov=src --cov-report=html
```

### Test Coverage

- **Utility Functions** (11 tests) - Date ranges, checksums, file operations
- **Database Operations** (8 tests) - DuckDB queries, transactions
- **Data Validators** (16 tests) - Schema validation, Parquet files, drift detection
- **Quality Checks** (12 tests) - Fare validation, timestamps, speeds, completeness
- **Transformations** (16 tests) - Trip IDs, price calculations, take rates

All tests run in ~0.5 seconds and use isolated temporary data.

---

## Best Practices

### 1. Always Use is_valid Flag

```sql
-- Filter invalid data
SELECT * FROM fact_trips WHERE is_valid = TRUE;
```

### 2. Handle NULL Values

```sql
-- Safe division
price_per_mile / NULLIF(trip_distance, 0)

-- Safe aggregation
AVG(CASE WHEN take_rate BETWEEN 0 AND 1 THEN take_rate END)
```

### 3. Idempotent Operations

```python
# Always check if work is already done
if not file_exists or not is_valid_checksum:
    download_file()

# Use INSERT OR IGNORE, not INSERT
conn.execute("INSERT OR IGNORE INTO ...")

# Delete before insert for metrics
conn.execute("DELETE FROM data_quality_metrics WHERE service_type = ?")
conn.execute("INSERT INTO data_quality_metrics ...")
```

### 4. Log Everything

```python
from loguru import logger

logger.info(f"Processing {file_name}: {row_count:,} rows")
logger.success(f"Transformation complete: {total:,} trips")
logger.error(f"Download failed for {url}: {error}")
```

### 5. Test with Sample Data First

```bash
# Always test with sample before full run
uv run python -m src.cli run-e2e --sample

# Verify results
uv run python -m src.cli db-stats

# Then run full
uv run python -m src.cli run-e2e --full
```

---

## Current Limitations

### 1. Single Machine Architecture

**Limitation:**
- Pipeline runs on a single machine (local)
- Limited by local disk space and memory
- No horizontal scalability

**Impact:**
- Processing 49 months (~60-80GB) takes several hours
- Memory constraints for very large transformations
- Cannot parallelize across multiple machines

### 2. No Incremental Processing

**Limitation:**
- Full re-transformation required for fact_trips
- Cannot update only new/changed data
- Must reprocess entire dataset

**Impact:**
- Re-running pipeline is expensive (time and compute)
- Difficult to maintain near real-time updates
- Higher cloud costs if deployed

### 3. Basic Error Recovery

**Limitation:**
- Manual intervention required for partial failures
- No automatic retry for transformation failures
- No transaction rollback for multi-stage operations

**Impact:**
- Pipeline may need manual restart
- Partial data states possible
- Requires monitoring

### 4. Limited Monitoring & Alerting

**Limitation:**
- Basic logging only (Loguru to console/file)
- No built-in alerting system
- No dashboards for pipeline health

**Impact:**
- Must manually check logs for issues
- No proactive failure detection
- Difficult to monitor long-running jobs

### 5. Data Freshness

**Limitation:**
- Batch processing only (no streaming)
- Monthly data granularity from TLC
- Manual trigger required for new data

**Impact:**
- Cannot analyze same-day data
- Depends on TLC publishing schedule
- Not suitable for real-time use cases

### 6. No Data Versioning

**Limitation:**
- Single version of data in database
- Cannot compare different pipeline runs
- No audit trail for data changes

**Impact:**
- Difficult to reproduce historical analyses
- Cannot A/B test different transformations
- No rollback capability

### 7. Single User / No Concurrency Control

**Limitation:**
- Designed for single user access
- No user authentication or authorization
- No multi-tenancy support

**Impact:**
- Not suitable for team collaboration
- Risk of conflicting writes
- No query prioritization

### 8. Storage Efficiency

**Limitation:**
- Stores raw + fact + aggregate data (redundancy)
- No data compression optimization
- No archival/cold storage strategy

**Impact:**
- Higher storage costs
- Inefficient for long-term data retention
- Database grows linearly with data

---

## Production Enhancements

### Phase 1: Cloud Infrastructure (Weeks 1-2)

#### 1.1 Migrate to Cloud Data Warehouse

**Current: DuckDB (local file)**
**Production: Snowflake / BigQuery / Redshift**

**Benefits:**
- Scalable compute and storage
- Built-in high availability
- Team collaboration support
- Query performance for large datasets

**Implementation:**
```python
# src/database/connection.py
class DatabaseConnection:
    @classmethod
    def get_connection(cls):
        if config.environment == 'production':
            # Snowflake
            return snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse='COMPUTE_WH',
                database='NYC_TAXI',
                schema='PUBLIC'
            )
        else:
            # DuckDB for local dev
            return duckdb.connect(str(config.database_path))
```

**Migration Steps:**
1. Export DuckDB to Parquet: `COPY fact_trips TO 'fact_trips.parquet'`
2. Upload to S3/GCS/Azure Blob
3. Use COPY INTO for bulk load to warehouse
4. Update SQL dialect (minor differences)
5. Re-create indexes and materialized views

#### 1.2 Object Storage for Raw Data

**Current: Local disk**
**Production: S3 / GCS / Azure Blob**

**Benefits:**
- Unlimited scalable storage
- Pay only for what you use
- Lifecycle policies (auto-archive old data)
- Versioning and immutability

**Implementation:**
```python
# src/ingestion/downloader.py
async def download_to_cloud(url: str, service_type: str, year_month: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    
    # Upload directly to S3
    s3_key = f"raw/{service_type}/{year_month}.parquet"
    s3_client.put_object(
        Bucket='nyc-taxi-data',
        Key=s3_key,
        Body=response.content,
        ServerSideEncryption='AES256'
    )
    
    return f"s3://nyc-taxi-data/{s3_key}"
```

**Configuration:**
```yaml
# config/pipeline_config.yaml
storage:
  provider: s3  # or gcs, azure
  bucket: nyc-taxi-data
  region: us-east-1
  lifecycle:
    archive_after_days: 90  # Move to Glacier
    delete_after_days: 365  # Delete old raw files
```

#### 1.3 Containerization & Orchestration

**Current: Local execution**
**Production: Kubernetes / ECS / Cloud Run**

**Benefits:**
- Consistent environment
- Auto-scaling
- Self-healing
- Resource limits

**Kubernetes Deployment:**
```yaml
# k8s/pipeline-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nyc-taxi-pipeline
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: pipeline
            image: ghcr.io/your-org/nyc-taxi-pipeline:latest
            command: ["uv", "run", "python", "-m", "src.cli", "run-e2e", "--full"]
            resources:
              requests:
                memory: "16Gi"
                cpu: "4"
              limits:
                memory: "32Gi"
                cpu: "8"
            env:
            - name: SNOWFLAKE_ACCOUNT
              valueFrom:
                secretKeyRef:
                  name: snowflake-creds
                  key: account
          restartPolicy: OnFailure
```

---

### Phase 2: Scalability & Performance (Weeks 3-4)

#### 2.1 Distributed Processing with Spark

**Current: Pandas (single machine)**
**Production: Apache Spark / Dask**

**Benefits:**
- Process terabytes of data
- Horizontal scaling (10s-100s of nodes)
- Fault tolerance
- 10-100x faster for large datasets

**Implementation:**
```python
# src/transformations/spark_transform.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat, lit

def transform_yellow_spark(spark: SparkSession):
    """Transform using Spark for scalability"""
    df = spark.read.parquet("s3://nyc-taxi-data/raw/yellow/*.parquet")
    
    fact_df = df.select(
        md5(concat(
            lit('yellow'),
            col('tpep_pickup_datetime').cast('string'),
            col('tpep_dropoff_datetime').cast('string'),
            col('trip_distance').cast('string'),
            col('total_amount').cast('string')
        )).alias('trip_id'),
        
        lit('yellow').alias('service_type'),
        col('tpep_pickup_datetime').alias('pickup_datetime'),
        col('tpep_dropoff_datetime').alias('dropoff_datetime'),
        col('PULocationID').alias('pickup_zone_id'),
        col('DOLocationID').alias('dropoff_zone_id'),
        # ... other transformations
    )
    
    # Write partitioned by date for efficient queries
    fact_df.write.partitionBy('pickup_date') \
        .mode('overwrite') \
        .parquet('s3://nyc-taxi-data/fact/trips/')
```

#### 2.2 Incremental Processing

**Current: Full refresh**
**Production: Delta Lake / Iceberg**

**Benefits:**
- Update only new/changed data
- Time travel (query historical versions)
- ACID transactions
- Merge/upsert operations

**Implementation:**
```python
# src/transformations/delta_transform.py
from delta import DeltaTable

def incremental_load(spark: SparkSession, new_data_path: str):
    """Merge new data into fact table"""
    new_df = spark.read.parquet(new_data_path)
    
    fact_table = DeltaTable.forPath(spark, 's3://nyc-taxi-data/fact/trips/')
    
    fact_table.alias('target').merge(
        new_df.alias('source'),
        'target.trip_id = source.trip_id'
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
```

**Workflow:**
```bash
# Daily incremental update
1. Detect new files: s3://nyc-taxi-data/raw/yellow/2025-03.parquet
2. Transform only new month
3. Merge into fact_trips (upsert)
4. Refresh aggregate tables for affected dates
```

#### 2.3 Query Optimization

**Materialized Views:**
```sql
-- Snowflake
CREATE MATERIALIZED VIEW mv_hourly_pricing AS
SELECT 
    DATE_TRUNC('hour', pickup_datetime) as hour,
    service_type,
    AVG(price_per_mile) as avg_price_per_mile,
    COUNT(*) as trip_count
FROM fact_trips
WHERE is_valid = TRUE
GROUP BY 1, 2;

-- Auto-refresh on data change
ALTER MATERIALIZED VIEW mv_hourly_pricing AUTO REFRESH;
```

**Result Caching:**
```python
# Use Redis for query results
import redis

def cached_query(sql: str, ttl: int = 3600):
    cache_key = f"query:{hashlib.md5(sql.encode()).hexdigest()}"
    
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    
    result = conn.execute(sql).fetchdf()
    redis_client.setex(cache_key, ttl, result.to_json())
    return result
```

---

### Phase 3: Reliability & Monitoring (Weeks 5-6)

#### 3.1 Advanced Orchestration with Airflow

**Current: Prefect (local)**
**Production: Apache Airflow / Dagster**

**Benefits:**
- Rich UI for DAG visualization
- Extensive retry/backfill capabilities
- SLA monitoring and alerting
- Large community and integrations

**Airflow DAG:**
```python
# dags/nyc_taxi_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['alerts@company.com'],
    'sla': timedelta(hours=6)
}

with DAG(
    'nyc_taxi_etl',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # Daily 3 AM
    catchup=False,
    tags=['production', 'data-pipeline']
) as dag:
    
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_new_files,
        execution_timeout=timedelta(hours=2)
    )
    
    transform_task = SnowflakeOperator(
        task_id='transform_to_fact',
        sql='transform_yellow_to_fact.sql',
        snowflake_conn_id='snowflake_prod'
    )
    
    quality_check = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_quality_checks
    )
    
    aggregate_task = SnowflakeOperator(
        task_id='build_aggregates',
        sql='build_all_aggregates.sql',
        snowflake_conn_id='snowflake_prod'
    )
    
    download_task >> transform_task >> quality_check >> aggregate_task
```

#### 3.2 Observability Stack

**Metrics: Prometheus + Grafana**
```python
# src/monitoring/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Pipeline metrics
pipeline_runs = Counter('pipeline_runs_total', 'Total pipeline runs', ['status'])
pipeline_duration = Histogram('pipeline_duration_seconds', 'Pipeline duration')
rows_processed = Counter('rows_processed_total', 'Rows processed', ['service_type'])
data_quality_score = Gauge('data_quality_pct', 'Data quality percentage', ['table'])

# Usage
with pipeline_duration.time():
    result = run_pipeline()
    pipeline_runs.labels(status='success').inc()
    rows_processed.labels(service_type='yellow').inc(result['yellow_count'])
```

**Logging: ELK Stack (Elasticsearch, Logstash, Kibana)**
```python
# src/logging/structured_logging.py
import structlog

logger = structlog.get_logger()

logger.info(
    "transformation_complete",
    service_type="yellow",
    rows_processed=3404479,
    duration_seconds=127.5,
    memory_peak_gb=4.2,
    success=True
)
```

**Alerting: PagerDuty / Opsgenie**
```python
# src/monitoring/alerts.py
def alert_on_failure(context):
    """Send alert on pipeline failure"""
    if context['exception']:
        pagerduty.trigger_incident(
            title=f"NYC Taxi Pipeline Failed: {context['task_id']}",
            severity='high',
            details={
                'task': context['task_id'],
                'error': str(context['exception']),
                'execution_date': context['execution_date']
            }
        )
```

#### 3.3 Data Quality Monitoring

**Great Expectations Integration:**
```python
# tests/expectations/yellow_taxi_suite.py
import great_expectations as gx

context = gx.get_context()

# Define expectations
suite = context.suites.add(gx.ExpectationSuite(name="yellow_taxi_quality"))

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="fare_amount",
        min_value=0,
        max_value=1000
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="pickup_datetime"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="service_type",
        value_set=['yellow', 'green', 'hvfhv']
    )
)

# Run validation
batch = context.sources.add_pandas("yellow_taxi").read_dataframe(df)
results = batch.validate(suite)

if not results.success:
    alert_quality_failure(results)
```

---

### Phase 4: Advanced Features (Weeks 7-8)

#### 4.1 Real-Time Stream Processing

**For Near Real-Time Analytics:**

**Architecture:**
```
TLC API → Kafka → Flink/Spark Streaming → Snowflake
```

**Implementation:**
```python
# src/streaming/kafka_consumer.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYCTaxiStreaming") \
    .getOrCreate()

# Read from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi-trips") \
    .load()

# Transform
processed_df = stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), trip_schema).alias("data")) \
    .select("data.*") \
    .withColumn("price_per_mile", col("fare") / col("distance"))

# Write to warehouse (micro-batches)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("snowflake") \
    .option("dbtable", "fact_trips_streaming") \
    .trigger(processingTime='1 minute') \
    .start()
```

#### 4.2 Machine Learning Integration

**Demand Forecasting:**
```python
# src/ml/demand_forecast.py
from sklearn.ensemble import RandomForestRegressor
import mlflow

def train_demand_model():
    # Feature engineering
    features = spark.sql("""
        SELECT 
            EXTRACT(DOW FROM pickup_datetime) as day_of_week,
            EXTRACT(HOUR FROM pickup_datetime) as hour,
            pickup_zone_id,
            LAG(trip_count, 1) OVER (PARTITION BY pickup_zone_id ORDER BY hour) as prev_hour_count,
            AVG(trip_count) OVER (PARTITION BY pickup_zone_id, hour) as avg_hourly_count,
            trip_count as target
        FROM agg_hourly_trips
    """).toPandas()
    
    # Train model
    X = features.drop('target', axis=1)
    y = features['target']
    
    model = RandomForestRegressor(n_estimators=100)
    model.fit(X, y)
    
    # Log with MLflow
    with mlflow.start_run():
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_metric("rmse", rmse)
        mlflow.sklearn.log_model(model, "demand_forecast_model")
```

**Price Optimization:**
```python
# Surge pricing optimization model
def optimize_pricing(zone_id: int, hour: int, current_demand: int):
    """Recommend optimal price based on demand"""
    model = mlflow.pyfunc.load_model("models:/price_optimizer/production")
    
    features = {
        'zone_id': zone_id,
        'hour': hour,
        'current_demand': current_demand,
        'historical_demand': get_avg_demand(zone_id, hour)
    }
    
    optimal_price = model.predict(pd.DataFrame([features]))
    return optimal_price[0]
```

#### 4.3 Data Governance & Compliance

**Data Catalog (DataHub / Atlan):**
```python
# Automatically catalog all tables
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter('http://datahub:8080')

# Register fact_trips
dataset_urn = make_dataset_urn(platform='snowflake', name='fact_trips')

metadata = {
    'description': 'Unified trip records across all service types',
    'tags': ['taxi', 'hvfhv', 'production', 'pii'],
    'owners': ['data-engineering@company.com'],
    'schema': [...],
    'sla': '6 hours',
    'retention': '7 years'
}

emitter.emit(dataset_urn, metadata)
```

**PII Masking:**
```sql
-- Create dynamic masking policies
CREATE MASKING POLICY mask_driver_id AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ADMIN', 'DATA_ENGINEER') THEN val
    ELSE '***MASKED***'
  END;

-- Apply to sensitive columns
ALTER TABLE fact_trips MODIFY COLUMN driver_id 
  SET MASKING POLICY mask_driver_id;
```

#### 4.4 Cost Optimization

**Auto-Scaling Compute:**
```sql
-- Snowflake: Auto-scale warehouse based on load
CREATE WAREHOUSE COMPUTE_WH WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300          -- Suspend after 5 min idle
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 10      -- Scale to 10 clusters under load
  SCALING_POLICY = 'STANDARD';
```

**Query Optimization:**
```python
# Analyze slow queries
slow_queries = conn.execute("""
    SELECT 
        query_id,
        query_text,
        execution_time_seconds,
        bytes_scanned_gb
    FROM snowflake.account_usage.query_history
    WHERE execution_time_seconds > 60
      AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    ORDER BY execution_time_seconds DESC
    LIMIT 20
""").fetchdf()

# Auto-create materialized views for expensive queries
for query in slow_queries.itertuples():
    if query.bytes_scanned_gb > 100:
        create_materialized_view(query.query_text)
```

**Data Lifecycle:**
```sql
-- Archive old data to cheaper storage
CREATE TABLE fact_trips_archive AS
SELECT * FROM fact_trips
WHERE pickup_date < DATEADD(year, -2, CURRENT_DATE());

DELETE FROM fact_trips
WHERE pickup_date < DATEADD(year, -2, CURRENT_DATE());

-- Set table to use infrequent access tier
ALTER TABLE fact_trips_archive SET DATA_RETENTION_TIME_IN_DAYS = 90;
```

---

## Production Deployment Checklist

### Pre-Deployment

- [ ] Load testing with production data volumes
- [ ] Disaster recovery plan documented
- [ ] Backup strategy implemented
- [ ] Security audit completed
- [ ] Cost estimation and budget approval
- [ ] Runbooks for common issues
- [ ] On-call rotation established

### Infrastructure

- [ ] Cloud accounts and billing set up
- [ ] VPC and network security configured
- [ ] Secrets management (AWS Secrets Manager / Vault)
- [ ] CI/CD pipeline (GitHub Actions / GitLab CI)
- [ ] Container registry (ECR / GCR / DockerHub)
- [ ] Infrastructure as Code (Terraform / Pulumi)

### Monitoring & Alerting

- [ ] Metrics dashboards created
- [ ] Alert thresholds configured
- [ ] On-call escalation policy
- [ ] Log aggregation and search
- [ ] Performance baselines established
- [ ] SLA definitions and tracking

### Security & Compliance

- [ ] IAM roles and policies (least privilege)
- [ ] Data encryption at rest and in transit
- [ ] PII identification and masking
- [ ] Audit logging enabled
- [ ] Vulnerability scanning automated
- [ ] Compliance requirements validated (GDPR, CCPA, etc.)

### Operations

- [ ] Automated backups configured
- [ ] Scaling policies tested
- [ ] Rollback procedures documented
- [ ] Health check endpoints implemented
- [ ] Incident response process
- [ ] Change management workflow

---

## Cost Estimation (Production Scale)

### Monthly Costs (49 months of data, ~60-80GB)

| Component | Local/Dev | Production (Cloud) |
|-----------|-----------|-------------------|
| **Compute** | $0 (local) | $500-1,000 (Snowflake Medium WH, 40h/month) |
| **Storage** | $0 (local disk) | $100-200 (S3 + Snowflake storage) |
| **Orchestration** | $0 (Prefect local) | $200-400 (Managed Airflow / Cloud Composer) |
| **Monitoring** | $0 (local logs) | $100-200 (Datadog / New Relic) |
| **Networking** | $0 | $50-100 (data transfer) |
| **Total** | **$0** | **$950-1,900/month** |

**Optimization opportunities:**
- Use auto-suspend (reduce compute costs by 60-80%)
- Implement data archival (reduce storage costs by 40-50%)
- Use spot instances for batch jobs (reduce compute by 70%)
- Optimize queries to scan less data (reduce costs by 30-50%)

**Estimated with optimizations: $300-600/month**

---

## Additional Resources

### Documentation Files

- **[README.md](../README.md)** - Project overview and quick start
- **[QUICK_START.md](QUICK_START.md)** - Step-by-step setup guide
- **[IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md)** - This file - detailed implementation details, limitations, and production enhancements
- **[BUSINESS_INSIGHTS.md](BUSINESS_INSIGHTS.md)** - Analytics query explanations and business questions answered
- **[VIRTUAL_ENV_GUIDE.md](VIRTUAL_ENV_GUIDE.md)** - UV virtual environment usage (why no activate needed)
- **[MONITORING_AND_SCHEDULING.md](MONITORING_AND_SCHEDULING.md)** - How to monitor and schedule the pipeline
- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Guidelines for contributors

### Key Points to Remember

1. **Docker is Optional** - This pipeline runs perfectly without Docker. Use native Python + `uv` for simplicity.
2. **Use `uv run`** - No need to activate virtual environments with `uv`. Just prefix commands with `uv run`.
3. **DuckDB is Portable** - Single `.duckdb` file contains everything. Easy to backup and share.
4. **Monitoring is Built-In** - Use `db-stats` command and monitoring scripts for observability.
5. **Scheduling Made Easy** - Use cron for simple scheduling, Prefect for advanced workflows.

### Getting Help

For questions or issues, open a GitHub issue with:
- Clear description of the problem
- Steps to reproduce
- Error messages and logs
- Environment details (OS, Python version, `uv` version)

