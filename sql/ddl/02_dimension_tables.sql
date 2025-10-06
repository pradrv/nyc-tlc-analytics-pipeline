-- ================================================================
-- DIMENSION TABLES
-- ================================================================
-- Support dimensional modeling for efficient analytics

-- ================================================================
-- DIM_ZONES (Taxi Zone Lookup)
-- ================================================================
CREATE TABLE IF NOT EXISTS dim_zones (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR,
    -- Derived fields
    is_airport BOOLEAN,
    is_manhattan BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate from raw_taxi_zones
INSERT OR REPLACE INTO dim_zones (location_id, borough, zone, service_zone, is_airport, is_manhattan)
SELECT 
    LocationID,
    Borough,
    Zone,
    service_zone,
    -- Flag airports
    CASE 
        WHEN Zone LIKE '%Airport%' OR service_zone = 'Airports' THEN TRUE
        ELSE FALSE
    END AS is_airport,
    -- Flag Manhattan
    CASE 
        WHEN Borough = 'Manhattan' THEN TRUE
        ELSE FALSE
    END AS is_manhattan
FROM raw_taxi_zones;

-- ================================================================
-- DIM_DATE (Date Dimension for Time Analysis)
-- ================================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR,
    month_name VARCHAR,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    week_of_year INTEGER,
    day_of_year INTEGER,
    -- Special flags for this analysis
    is_before_congestion_fee BOOLEAN,  -- Before Jan 5, 2025
    is_after_congestion_fee BOOLEAN    -- On or after Jan 5, 2025
);

-- Populate date dimension (2021-01-01 through 2025-12-31)
INSERT OR REPLACE INTO dim_date
SELECT
    date_series AS date_id,
    EXTRACT(YEAR FROM date_series) AS year,
    EXTRACT(MONTH FROM date_series) AS month,
    EXTRACT(DAY FROM date_series) AS day,
    EXTRACT(QUARTER FROM date_series) AS quarter,
    EXTRACT(DOW FROM date_series) AS day_of_week,
    DAYNAME(date_series) AS day_name,
    MONTHNAME(date_series) AS month_name,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    -- Simple holiday detection (can be enhanced)
    CASE 
        WHEN EXTRACT(MONTH FROM date_series) = 1 AND EXTRACT(DAY FROM date_series) = 1 THEN TRUE  -- New Year
        WHEN EXTRACT(MONTH FROM date_series) = 7 AND EXTRACT(DAY FROM date_series) = 4 THEN TRUE  -- July 4th
        WHEN EXTRACT(MONTH FROM date_series) = 12 AND EXTRACT(DAY FROM date_series) = 25 THEN TRUE -- Christmas
        ELSE FALSE 
    END AS is_holiday,
    EXTRACT(WEEK FROM date_series) AS week_of_year,
    EXTRACT(DOY FROM date_series) AS day_of_year,
    -- Congestion fee analysis flags
    CASE WHEN date_series < '2025-01-05' THEN TRUE ELSE FALSE END AS is_before_congestion_fee,
    CASE WHEN date_series >= '2025-01-05' THEN TRUE ELSE FALSE END AS is_after_congestion_fee
FROM generate_series(
    DATE '2021-01-01',
    DATE '2025-12-31',
    INTERVAL '1 day'
) AS t(date_series);

-- ================================================================
-- DIM_TIME (Hour of Day Dimension)
-- ================================================================
CREATE TABLE IF NOT EXISTS dim_time (
    hour INTEGER PRIMARY KEY,
    hour_12 VARCHAR,
    period VARCHAR,
    is_rush_hour BOOLEAN,
    time_bucket VARCHAR
);

-- Populate time dimension (0-23 hours)
INSERT OR REPLACE INTO dim_time
SELECT
    hour_val AS hour,
    -- 12-hour format
    CASE 
        WHEN hour_val = 0 THEN '12 AM'
        WHEN hour_val < 12 THEN hour_val || ' AM'
        WHEN hour_val = 12 THEN '12 PM'
        ELSE (hour_val - 12) || ' PM'
    END AS hour_12,
    -- Period of day
    CASE 
        WHEN hour_val BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour_val BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour_val BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS period,
    -- Rush hour definition (7-9 AM, 4-7 PM weekdays)
    CASE 
        WHEN hour_val BETWEEN 7 AND 9 THEN TRUE
        WHEN hour_val BETWEEN 16 AND 19 THEN TRUE
        ELSE FALSE
    END AS is_rush_hour,
    -- Time buckets for congestion fee
    CASE 
        WHEN hour_val BETWEEN 6 AND 20 THEN 'Day (6am-9pm)'
        ELSE 'Night (9pm-6am)'
    END AS time_bucket
FROM generate_series(0, 23, 1) AS t(hour_val);

-- ================================================================
-- DIM_SERVICE (Service Type Reference)
-- ================================================================
CREATE TABLE IF NOT EXISTS dim_service (
    service_type VARCHAR PRIMARY KEY,
    service_name VARCHAR,
    service_category VARCHAR,
    description VARCHAR
);

-- Populate service types
INSERT OR REPLACE INTO dim_service VALUES
('yellow', 'Yellow Taxi', 'Traditional Taxi', 'Yellow medallion taxis operating citywide'),
('green', 'Green Taxi', 'Traditional Taxi', 'Green cabs operating outside Manhattan core'),
('hvfhv', 'HVFHV', 'Ride-Hailing', 'High-volume for-hire vehicles (Uber, Lyft, Via, Juno)');

-- ================================================================
-- DIM_HVFHS_COMPANY (HVFHS License Lookup)
-- ================================================================
CREATE TABLE IF NOT EXISTS dim_hvfhs_company (
    hvfhs_license_num VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    is_active BOOLEAN
);

-- Populate known HVFHS licenses
INSERT OR REPLACE INTO dim_hvfhs_company VALUES
('HV0002', 'Juno', TRUE),
('HV0003', 'Uber', TRUE),
('HV0004', 'Via', TRUE),
('HV0005', 'Lyft', TRUE);

