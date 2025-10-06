-- Data quality summary report
-- Use case: Monitor overall data quality and identify issues

SELECT 
    f.service_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN f.is_valid THEN 1 ELSE 0 END) as valid_records,
    SUM(CASE WHEN NOT f.is_valid THEN 1 ELSE 0 END) as invalid_records,
    ROUND(SUM(CASE WHEN f.is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as data_quality_pct,
    
    -- Specific quality issues
    SUM(CASE WHEN f.total_fare < 0 THEN 1 ELSE 0 END) as negative_fares,
    SUM(CASE WHEN f.trip_distance_miles < 0 THEN 1 ELSE 0 END) as negative_distances,
    SUM(CASE WHEN f.trip_duration_minutes < 0 THEN 1 ELSE 0 END) as negative_durations,
    SUM(CASE WHEN f.dropoff_datetime <= f.pickup_datetime THEN 1 ELSE 0 END) as invalid_timestamps,
    SUM(CASE WHEN f.avg_speed_mph > 100 THEN 1 ELSE 0 END) as excessive_speeds,
    
    -- Null checks
    SUM(CASE WHEN f.pickup_zone_id IS NULL THEN 1 ELSE 0 END) as null_pickup_zones,
    SUM(CASE WHEN f.dropoff_zone_id IS NULL THEN 1 ELSE 0 END) as null_dropoff_zones,
    
    MIN(f.pickup_date) as earliest_trip,
    MAX(f.pickup_date) as latest_trip
    
FROM fact_trips f
GROUP BY f.service_type
ORDER BY total_records DESC;
