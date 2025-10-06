-- Weekend vs Weekday patterns
-- Use case: Compare demand and pricing between weekdays and weekends

SELECT 
    d.is_weekend,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    f.service_type,
    COUNT(*) as trip_count,
    AVG(f.total_fare) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.price_per_mile) as avg_price_per_mile,
    AVG(f.tips) / NULLIF(AVG(f.total_fare), 0) * 100 as avg_tip_percentage,
    SUM(f.total_fare) as total_revenue
FROM fact_trips f
JOIN dim_date d ON f.pickup_date = d.date_id
WHERE f.is_valid = TRUE
GROUP BY d.is_weekend, f.service_type
ORDER BY d.is_weekend, trip_count DESC;
