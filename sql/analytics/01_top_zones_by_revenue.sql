-- Top 20 zones by total revenue across all services
-- Use case: Identify highest revenue generating pickup locations

SELECT 
    z.zone as pickup_zone,
    z.borough,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_fare) as total_revenue,
    AVG(f.total_fare) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    SUM(CASE WHEN f.service_type = 'yellow' THEN 1 ELSE 0 END) as yellow_trips,
    SUM(CASE WHEN f.service_type = 'green' THEN 1 ELSE 0 END) as green_trips,
    SUM(CASE WHEN f.service_type = 'hvfhv' THEN 1 ELSE 0 END) as hvfhv_trips
FROM fact_trips f
JOIN dim_zones z ON f.pickup_zone_id = z.location_id
WHERE f.is_valid = TRUE
GROUP BY z.zone, z.borough
ORDER BY total_revenue DESC
LIMIT 20;
