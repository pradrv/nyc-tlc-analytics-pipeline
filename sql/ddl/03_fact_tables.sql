-- ================================================================
-- FACT TABLES
-- ================================================================
-- Unified, standardized trip data across all service types

-- ================================================================
-- FACT_TRIPS (Main Fact Table)
-- ================================================================
CREATE TABLE IF NOT EXISTS fact_trips (
    -- Primary key
    trip_id VARCHAR PRIMARY KEY,
    
    -- Service identification
    service_type VARCHAR NOT NULL,
    hvfhs_license_num VARCHAR,  -- Only for HVFHV
    
    -- Time dimensions
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_date DATE,
    pickup_hour INTEGER,
    pickup_day_of_week INTEGER,
    
    -- Location dimensions
    pickup_zone_id INTEGER,
    dropoff_zone_id INTEGER,
    
    -- Trip metrics
    trip_distance_miles DOUBLE,
    trip_duration_minutes DOUBLE,
    passenger_count INTEGER,
    
    -- Fare breakdown (standardized across services)
    base_fare DOUBLE,
    tips DOUBLE,
    tolls DOUBLE,
    surcharges DOUBLE,
    congestion_fee DOUBLE,
    cbd_congestion_fee DOUBLE,  -- Jan 2025+ congestion relief zone fee
    airport_fee DOUBLE,
    taxes DOUBLE,
    total_fare DOUBLE,
    
    -- HVFHV-specific economics
    driver_pay DOUBLE,  -- NULL for yellow/green
    take_rate DOUBLE,   -- (total_fare - driver_pay) / total_fare, NULL for yellow/green
    
    -- Derived metrics (for efficiency)
    price_per_mile DOUBLE,
    price_per_minute DOUBLE,
    avg_speed_mph DOUBLE,
    
    -- Flags
    is_shared_request BOOLEAN,  -- HVFHV only
    is_shared_match BOOLEAN,    -- HVFHV only
    is_airport_trip BOOLEAN,
    payment_type INTEGER,       -- Yellow/Green only
    
    -- Data quality
    is_valid BOOLEAN DEFAULT TRUE,
    quality_flags VARCHAR,  -- JSON array of quality issues
    
    -- Metadata
    source_file VARCHAR,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_trips_service_datetime 
    ON fact_trips(service_type, pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_zone 
    ON fact_trips(pickup_zone_id);

CREATE INDEX IF NOT EXISTS idx_trips_dropoff_zone 
    ON fact_trips(dropoff_zone_id);

CREATE INDEX IF NOT EXISTS idx_trips_date 
    ON fact_trips(pickup_date);

CREATE INDEX IF NOT EXISTS idx_trips_service_zone_date 
    ON fact_trips(service_type, pickup_zone_id, pickup_date);

CREATE INDEX IF NOT EXISTS idx_trips_valid 
    ON fact_trips(is_valid, service_type);

CREATE INDEX IF NOT EXISTS idx_trips_hvfhs 
    ON fact_trips(hvfhs_license_num);

-- ================================================================
-- DATA_QUALITY_METRICS (Quality Tracking)
-- ================================================================
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    check_id VARCHAR PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    service_type VARCHAR,
    source_file VARCHAR,
    check_date DATE,
    
    -- Counts
    total_rows INTEGER,
    passed_rows INTEGER,
    failed_rows INTEGER,
    duplicate_rows INTEGER,
    
    -- Check details
    check_type VARCHAR,  -- 'fare', 'timestamp', 'speed', 'distance', 'schema'
    failure_rate DOUBLE,
    
    -- Details (JSON)
    details VARCHAR,
    
    -- Summary statistics
    min_value DOUBLE,
    max_value DOUBLE,
    avg_value DOUBLE,
    median_value DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_quality_service_date 
    ON data_quality_metrics(service_type, check_date);

CREATE INDEX IF NOT EXISTS idx_quality_check_type 
    ON data_quality_metrics(check_type);

