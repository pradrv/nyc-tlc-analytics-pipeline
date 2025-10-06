-- Hourly demand patterns by service type
-- Use case: Understand peak hours for operational planning

SELECT 
    t.hour,
    t.hour_12 as hour_label,
    t.is_rush_hour as is_peak_hour,
    SUM(CASE WHEN f.service_type = 'yellow' THEN 1 ELSE 0 END) as yellow_trips,
    SUM(CASE WHEN f.service_type = 'green' THEN 1 ELSE 0 END) as green_trips,
    SUM(CASE WHEN f.service_type = 'hvfhv' THEN 1 ELSE 0 END) as hvfhv_trips,
    COUNT(*) as total_trips,
    AVG(f.total_fare) as avg_fare,
    AVG(f.trip_duration_minutes) as avg_duration_minutes
FROM fact_trips f
JOIN dim_time t ON f.pickup_hour = t.hour
WHERE f.is_valid = TRUE
GROUP BY t.hour, t.hour_12, t.is_rush_hour
ORDER BY t.hour;
