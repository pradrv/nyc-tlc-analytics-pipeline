-- ================================================================
-- RAW/STAGING TABLES
-- ================================================================
-- These tables store data as-is from source files
-- No transformations, just direct ingestion with metadata tracking

-- ================================================================
-- RAW YELLOW TAXI
-- ================================================================
CREATE TABLE IF NOT EXISTS raw_yellow (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,  -- Note: Capital A in actual data
    -- Metadata for tracking
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================================
-- RAW GREEN TAXI
-- ================================================================
CREATE TABLE IF NOT EXISTS raw_green (
    VendorID INTEGER,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag VARCHAR,
    RatecodeID DOUBLE,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type INTEGER,
    trip_type DOUBLE,
    congestion_surcharge DOUBLE,
    -- Metadata for tracking
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================================
-- RAW HVFHV (High Volume For-Hire Vehicle)
-- ================================================================
CREATE TABLE IF NOT EXISTS raw_hvfhv (
    hvfhs_license_num VARCHAR,
    dispatching_base_num VARCHAR,
    originating_base_num VARCHAR,
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    trip_miles DOUBLE,
    trip_time BIGINT,
    base_passenger_fare DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    shared_request_flag VARCHAR,
    shared_match_flag VARCHAR,
    access_a_ride_flag VARCHAR,
    wav_request_flag VARCHAR,
    wav_match_flag VARCHAR,
    -- Metadata for tracking
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================================================================
-- RAW TAXI ZONES (Reference Data)
-- ================================================================
CREATE TABLE IF NOT EXISTS raw_taxi_zones (
    LocationID INTEGER PRIMARY KEY,
    Borough VARCHAR,
    Zone VARCHAR,
    service_zone VARCHAR
);

-- ================================================================
-- INGESTION LOG (Track All Downloads)
-- ================================================================
CREATE TABLE IF NOT EXISTS ingestion_log (
    log_id INTEGER PRIMARY KEY,
    service_type VARCHAR,
    year INTEGER,
    month INTEGER,
    source_file VARCHAR,
    file_url VARCHAR,
    file_path VARCHAR,
    file_size_bytes BIGINT,
    file_checksum VARCHAR,
    row_count INTEGER,
    column_count INTEGER,
    column_names VARCHAR,
    download_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP,
    status VARCHAR,
    error_message VARCHAR
);

-- Create sequence for log_id
CREATE SEQUENCE IF NOT EXISTS ingestion_log_seq START 1;

