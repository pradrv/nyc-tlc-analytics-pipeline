-- ================================================================
-- AGGREGATE TABLES
-- ================================================================
-- Pre-computed aggregations for fast analytics

-- ================================================================
-- AGG_PRICING_BY_ZONE_HOUR
-- ================================================================
-- Pricing metrics by service, zone, and hour
CREATE TABLE IF NOT EXISTS agg_pricing_by_zone_hour (
    service_type VARCHAR,
    pickup_zone_id INTEGER,
    pickup_hour INTEGER,
    trip_date DATE,
    
    -- Counts
    trip_count INTEGER,
    valid_trip_count INTEGER,
    
    -- Distance metrics
    avg_trip_distance DOUBLE,
    median_trip_distance DOUBLE,
    total_trip_miles DOUBLE,
    
    -- Duration metrics
    avg_trip_duration DOUBLE,
    median_trip_duration DOUBLE,
    
    -- Price per mile
    avg_price_per_mile DOUBLE,
    median_price_per_mile DOUBLE,
    p25_price_per_mile DOUBLE,
    p75_price_per_mile DOUBLE,
    
    -- Price per minute
    avg_price_per_minute DOUBLE,
    median_price_per_minute DOUBLE,
    p25_price_per_minute DOUBLE,
    p75_price_per_minute DOUBLE,
    
    -- Total fare
    avg_total_fare DOUBLE,
    median_total_fare DOUBLE,
    total_revenue DOUBLE,
    
    -- Congestion fee (for Jan 2025+ analysis)
    trips_with_cbd_fee INTEGER,
    avg_cbd_fee DOUBLE,
    total_cbd_fee DOUBLE,
    
    PRIMARY KEY (service_type, pickup_zone_id, pickup_hour, trip_date)
);

CREATE INDEX IF NOT EXISTS idx_agg_pricing_date 
    ON agg_pricing_by_zone_hour(trip_date);

CREATE INDEX IF NOT EXISTS idx_agg_pricing_zone 
    ON agg_pricing_by_zone_hour(pickup_zone_id);

-- ================================================================
-- AGG_HVFHV_TAKE_RATES
-- ================================================================
-- HVFHV take rate analysis (platform commission)
CREATE TABLE IF NOT EXISTS agg_hvfhv_take_rates (
    trip_date DATE,
    pickup_zone_id INTEGER,
    pickup_hour INTEGER,
    hvfhs_license_num VARCHAR,
    
    -- Trip characteristics
    trip_count INTEGER,
    avg_trip_distance DOUBLE,
    avg_trip_duration DOUBLE,
    
    -- Take rate distribution
    median_take_rate DOUBLE,
    p25_take_rate DOUBLE,
    p75_take_rate DOUBLE,
    avg_take_rate DOUBLE,
    stddev_take_rate DOUBLE,
    
    -- Driver economics
    avg_driver_pay DOUBLE,
    median_driver_pay DOUBLE,
    total_driver_pay DOUBLE,
    
    -- Platform economics
    avg_platform_commission DOUBLE,
    total_platform_commission DOUBLE,
    
    -- Fare breakdown
    avg_total_fare DOUBLE,
    total_revenue DOUBLE,
    
    PRIMARY KEY (trip_date, pickup_zone_id, pickup_hour, hvfhs_license_num)
);

CREATE INDEX IF NOT EXISTS idx_agg_take_rate_date 
    ON agg_hvfhv_take_rates(trip_date);

CREATE INDEX IF NOT EXISTS idx_agg_take_rate_company 
    ON agg_hvfhv_take_rates(hvfhs_license_num);

-- ================================================================
-- AGG_MARKET_SHARE
-- ================================================================
-- Market share by service type (zone and day)
CREATE TABLE IF NOT EXISTS agg_market_share (
    trip_date DATE,
    pickup_zone_id INTEGER,
    
    -- Trip counts by service
    yellow_trips INTEGER,
    green_trips INTEGER,
    hvfhv_trips INTEGER,
    total_trips INTEGER,
    
    -- Market share percentages
    yellow_share DOUBLE,
    green_share DOUBLE,
    hvfhv_share DOUBLE,
    
    -- Average pricing by service
    yellow_avg_price_per_mile DOUBLE,
    green_avg_price_per_mile DOUBLE,
    hvfhv_avg_price_per_mile DOUBLE,
    
    -- Revenue by service
    yellow_total_revenue DOUBLE,
    green_total_revenue DOUBLE,
    hvfhv_total_revenue DOUBLE,
    total_revenue DOUBLE,
    
    -- Revenue share
    yellow_revenue_share DOUBLE,
    green_revenue_share DOUBLE,
    hvfhv_revenue_share DOUBLE,
    
    PRIMARY KEY (trip_date, pickup_zone_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_market_date 
    ON agg_market_share(trip_date);

CREATE INDEX IF NOT EXISTS idx_agg_market_zone 
    ON agg_market_share(pickup_zone_id);

-- ================================================================
-- AGG_CONGESTION_FEE_IMPACT
-- ================================================================
-- Before/after congestion fee analysis (Jan 5, 2025)
CREATE TABLE IF NOT EXISTS agg_congestion_fee_impact (
    service_type VARCHAR,
    pickup_zone_id INTEGER,
    time_period VARCHAR,  -- 'before' or 'after'
    
    -- Date range
    start_date DATE,
    end_date DATE,
    
    -- Trip metrics
    trip_count INTEGER,
    avg_trips_per_day DOUBLE,
    
    -- Pricing metrics
    avg_total_fare DOUBLE,
    median_total_fare DOUBLE,
    avg_price_per_mile DOUBLE,
    median_price_per_mile DOUBLE,
    
    -- Congestion fee specific
    trips_with_cbd_fee INTEGER,
    cbd_fee_adoption_rate DOUBLE,
    avg_cbd_fee DOUBLE,
    total_cbd_fee_collected DOUBLE,
    
    -- Pass-through analysis
    avg_fare_excluding_cbd_fee DOUBLE,
    fare_increase_excl_cbd DOUBLE,  -- Change beyond just the fee
    
    PRIMARY KEY (service_type, pickup_zone_id, time_period)
);

CREATE INDEX IF NOT EXISTS idx_agg_congestion_service 
    ON agg_congestion_fee_impact(service_type);

-- ================================================================
-- AGG_DAILY_SUMMARY
-- ================================================================
-- High-level daily summary for monitoring
CREATE TABLE IF NOT EXISTS agg_daily_summary (
    trip_date DATE PRIMARY KEY,
    
    -- Overall metrics
    total_trips INTEGER,
    total_revenue DOUBLE,
    avg_trip_distance DOUBLE,
    avg_trip_duration DOUBLE,
    
    -- By service type
    yellow_trips INTEGER,
    green_trips INTEGER,
    hvfhv_trips INTEGER,
    
    yellow_revenue DOUBLE,
    green_revenue DOUBLE,
    hvfhv_revenue DOUBLE,
    
    -- Quality metrics
    total_valid_trips INTEGER,
    data_quality_score DOUBLE,
    
    -- Congestion fee
    total_cbd_fees DOUBLE,
    trips_with_cbd_fee INTEGER
);

