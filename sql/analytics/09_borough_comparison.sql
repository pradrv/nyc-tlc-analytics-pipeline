-- Borough-level comparison
-- Use case: Compare trip patterns and economics across NYC boroughs

SELECT 
    z.borough,
    COUNT(*) as total_trips,
    SUM(CASE WHEN f.service_type = 'yellow' THEN 1 ELSE 0 END) as yellow_trips,
    SUM(CASE WHEN f.service_type = 'green' THEN 1 ELSE 0 END) as green_trips,
    SUM(CASE WHEN f.service_type = 'hvfhv' THEN 1 ELSE 0 END) as hvfhv_trips,
    AVG(f.total_fare) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.price_per_mile) as avg_price_per_mile,
    SUM(f.total_fare) as total_revenue,
    ROUND(SUM(CASE WHEN f.service_type = 'hvfhv' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as hvfhv_market_share_pct
FROM fact_trips f
JOIN dim_zones z ON f.pickup_zone_id = z.location_id
WHERE f.is_valid = TRUE
    AND z.borough != 'Unknown'
GROUP BY z.borough
ORDER BY total_trips DESC;
