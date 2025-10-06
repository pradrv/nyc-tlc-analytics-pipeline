-- Pricing comparison: Yellow vs Green vs HVFHV
-- Use case: Compare pricing strategies across service types

SELECT 
    f.service_type,
    COUNT(*) as trip_count,
    AVG(f.price_per_mile) as avg_price_per_mile,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price_per_mile) as median_price_per_mile,
    AVG(f.price_per_minute) as avg_price_per_minute,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price_per_minute) as median_price_per_minute,
    AVG(f.total_fare) as avg_total_fare,
    AVG(f.trip_distance_miles) as avg_trip_distance,
    AVG(f.trip_duration_minutes) as avg_trip_duration,
    AVG(f.tips) as avg_tips,
    AVG(f.tips) / NULLIF(AVG(f.total_fare), 0) * 100 as avg_tip_percentage
FROM fact_trips f
WHERE f.is_valid = TRUE
    AND f.price_per_mile BETWEEN 0.5 AND 50
    AND f.price_per_minute BETWEEN 0.1 AND 10
GROUP BY f.service_type
ORDER BY trip_count DESC;
