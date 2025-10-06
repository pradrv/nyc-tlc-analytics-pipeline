# Quick Start Guide

## 🚀 Get Running in 5 Minutes

### Step 1: Setup (1 minute)

```bash
cd /Users/pvishwakar005/GDS/data_pipeline
./scripts/setup.sh
source .venv/bin/activate
```

This will:
- Install `uv` package manager (if not already installed)
- Create Python virtual environment
- Install all dependencies
- Create necessary directories

### Step 2: Run Sample Pipeline (3-5 minutes)

```bash
./scripts/run_sample.sh
```

This will:
1. Initialize DuckDB database with schema (~5 seconds)
2. Download 3 sample months of data (~1-2 minutes)
   - January 2024: Yellow, Green, HVFHV
   - June 2024: Yellow, Green, HVFHV
   - December 2024: Yellow, Green, HVFHV
   - Taxi zone lookup
3. Load taxi zones (~1 second)
4. Load trip data to raw tables (~1-2 minutes)
5. Show database statistics

**Expected output:**
```
📊 Database Schema Summary
====================================================================
Database: data/database/nyc_taxi.duckdb
Size: 1.8 GB
Tables: 15

Table Row Counts:
  raw_yellow................................... 6,234,891
  raw_green....................................   892,445
  raw_hvfhv................................... 42,156,782
  dim_zones........................................     263
  dim_date.........................................   1,826
  dim_time...........................................    24
  ...
====================================================================
```

### Step 3: Explore Data

**Option A: SQL Queries**

```bash
# Open DuckDB CLI
duckdb data/database/nyc_taxi.duckdb

# Run queries
SELECT 
    service_type,
    COUNT(*) as trips,
    AVG(trip_distance) as avg_distance
FROM raw_yellow
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024
    AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 6
GROUP BY service_type;
```

**Option B: Python**

```python
from src.database.connection import DatabaseConnection

conn = DatabaseConnection.get_connection()

# Query raw data
df = conn.execute("""
    SELECT 
        DATE_TRUNC('day', tpep_pickup_datetime) as date,
        COUNT(*) as trips,
        AVG(trip_distance) as avg_distance,
        AVG(total_amount) as avg_fare
    FROM raw_yellow
    WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) = 6
    GROUP BY date
    ORDER BY date
""").df()

print(df)
```

**Option C: Jupyter Notebook**

```bash
# Start Jupyter
docker-compose --profile analysis up jupyter

# Open browser to http://localhost:8888
# Create new notebook and import:
from src.database.connection import DatabaseConnection
import pandas as pd
```

---

## 📊 What You Can Do Now

### Check Database Stats

```bash
python -m src.cli db-stats
```

### Download More Data

```bash
# Download specific months
python -m src.cli download --start-date 2024-01 --end-date 2024-12

# Load new data
python -m src.cli load
```

### View Raw Data

```sql
-- Top 10 most expensive yellow taxi trips in June 2024
SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    total_amount,
    passenger_count
FROM raw_yellow
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024
    AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 6
    AND total_amount > 0
ORDER BY total_amount DESC
LIMIT 10;
```

```sql
-- Compare average fares by service type
SELECT 
    'Yellow' as service_type,
    COUNT(*) as trips,
    AVG(total_amount) as avg_fare,
    AVG(trip_distance) as avg_distance
FROM raw_yellow
WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) = 6

UNION ALL

SELECT 
    'Green' as service_type,
    COUNT(*),
    AVG(total_amount),
    AVG(trip_distance)
FROM raw_green
WHERE EXTRACT(MONTH FROM lpep_pickup_datetime) = 6

UNION ALL

SELECT 
    'HVFHV' as service_type,
    COUNT(*),
    AVG(base_passenger_fare + tolls + tips + congestion_surcharge + airport_fee),
    AVG(trip_miles)
FROM raw_hvfhv
WHERE EXTRACT(MONTH FROM pickup_datetime) = 6;
```

---

## 🎯 Next Steps

### Phase 2: Transformations (Not Yet Implemented)

Once transformations are complete, you'll be able to:

```sql
-- Query unified fact table
SELECT 
    service_type,
    COUNT(*) as trips,
    AVG(price_per_mile) as avg_price_per_mile,
    AVG(price_per_minute) as avg_price_per_minute
FROM fact_trips
WHERE pickup_date = '2024-06-15'
GROUP BY service_type;
```

```sql
-- Analyze HVFHV take rates
SELECT 
    hvfhs_license_num,
    AVG(take_rate) as avg_take_rate,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY take_rate) as median_take_rate
FROM fact_trips
WHERE service_type = 'hvfhv'
    AND take_rate IS NOT NULL
GROUP BY hvfhs_license_num;
```

```sql
-- Congestion fee impact (before/after Jan 5, 2025)
SELECT 
    CASE 
        WHEN pickup_date < '2025-01-05' THEN 'Before'
        ELSE 'After'
    END as period,
    service_type,
    COUNT(*) as trips,
    AVG(total_fare) as avg_fare,
    AVG(cbd_congestion_fee) as avg_cbd_fee
FROM fact_trips
WHERE pickup_date BETWEEN '2024-12-01' AND '2025-02-01'
GROUP BY period, service_type;
```

---

## 🐛 Troubleshooting

### "uv: command not found"

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.cargo/bin:$PATH"
```

### "Virtual environment not found"

```bash
./scripts/setup.sh
```

### "File download failed"

Some months may not be available yet (e.g., Jan 2025). This is normal. The pipeline will log warnings and continue.

### "Database locked"

Close any other connections to the DuckDB file:

```bash
# Kill any running Python processes
pkill -f "python.*cli.py"

# Or delete the database and re-run
rm data/database/nyc_taxi.duckdb
./scripts/run_sample.sh
```

### Check logs

```bash
ls -lh data/logs/
tail -n 50 data/logs/pipeline_*.log
```

---

## 📚 Learn More

- **README.md** - Full documentation
- **IMPLEMENTATION_PLAN.md** - Detailed technical design
- **STATUS.md** - Current implementation status
- **sql/ddl/** - Database schema definitions

---

## 💡 Pro Tips

1. **Start small:** Run sample pipeline first (3 months) before full pipeline (49 months)

2. **Check file sizes:** The full pipeline downloads ~21GB
   ```bash
   du -sh data/raw/
   ```

3. **Monitor progress:** Watch the logs in real-time
   ```bash
   tail -f data/logs/pipeline_*.log
   ```

4. **DuckDB is fast:** Queries on millions of rows return in seconds

5. **Schema is documented:** Check `sql/ddl/00_schema_reference.sql` for field mappings

6. **Idempotent by design:** Re-running is safe and fast (skips already-loaded data)

---

**Ready? Run this:**

```bash
./scripts/setup.sh && ./scripts/run_sample.sh
```

**Then explore the data!** 🎉

