-- Airport trips analysis
-- Use case: Understand airport-related trip patterns and economics

SELECT 
    CASE 
        WHEN z_pickup.service_zone = 'Airports' THEN 'From Airport'
        WHEN z_dropoff.service_zone = 'Airports' THEN 'To Airport'
        ELSE 'Other'
    END as trip_type,
    f.service_type,
    COUNT(*) as trip_count,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.total_fare) as avg_fare,
    AVG(f.airport_fee) as avg_airport_fee,
    SUM(f.airport_fee) as total_airport_fees,
    AVG(f.price_per_mile) as avg_price_per_mile
FROM fact_trips f
JOIN dim_zones z_pickup ON f.pickup_zone_id = z_pickup.location_id
JOIN dim_zones z_dropoff ON f.dropoff_zone_id = z_dropoff.location_id
WHERE (z_pickup.service_zone = 'Airports' OR z_dropoff.service_zone = 'Airports')
    AND f.is_valid = TRUE
GROUP BY 
    CASE 
        WHEN z_pickup.service_zone = 'Airports' THEN 'From Airport'
        WHEN z_dropoff.service_zone = 'Airports' THEN 'To Airport'
        ELSE 'Other'
    END,
    f.service_type
ORDER BY trip_count DESC;
