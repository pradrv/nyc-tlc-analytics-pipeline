-- Shared rides analysis for HVFHV
-- Use case: Understand shared ride adoption and economics

SELECT 
    CASE 
        WHEN f.is_shared_request AND f.is_shared_match THEN 'Requested & Matched'
        WHEN f.is_shared_request AND NOT f.is_shared_match THEN 'Requested but Not Matched'
        ELSE 'Not Shared'
    END as shared_status,
    COUNT(*) as trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    AVG(f.total_fare) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.price_per_mile) as avg_price_per_mile,
    AVG(f.take_rate) * 100 as avg_take_rate_pct,
    AVG(f.driver_pay) as avg_driver_pay
FROM fact_trips f
WHERE f.service_type = 'hvfhv'
    AND f.is_valid = TRUE
GROUP BY 
    CASE 
        WHEN f.is_shared_request AND f.is_shared_match THEN 'Requested & Matched'
        WHEN f.is_shared_request AND NOT f.is_shared_match THEN 'Requested but Not Matched'
        ELSE 'Not Shared'
    END
ORDER BY trip_count DESC;
