-- Question 1: Are Uber/Lyft pricing $/mile and $/minute materially higher than Yellow/Green by zone and hour?
-- Use case: Compare HVFHV (Uber/Lyft) vs traditional taxi pricing by location and time

WITH pricing_comparison AS (
    SELECT 
        z.zone as pickup_zone,
        z.borough,
        t.hour,
        t.hour_12 as hour_label,
        t.is_rush_hour as is_peak_hour,
        
        -- Yellow/Green Taxi Metrics
        AVG(CASE WHEN f.service_type IN ('yellow', 'green') THEN f.price_per_mile END) as taxi_avg_price_per_mile,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN f.service_type IN ('yellow', 'green') THEN f.price_per_mile END) as taxi_median_price_per_mile,
        AVG(CASE WHEN f.service_type IN ('yellow', 'green') THEN f.price_per_minute END) as taxi_avg_price_per_minute,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN f.service_type IN ('yellow', 'green') THEN f.price_per_minute END) as taxi_median_price_per_minute,
        COUNT(CASE WHEN f.service_type IN ('yellow', 'green') THEN 1 END) as taxi_trip_count,
        
        -- HVFHV (Uber/Lyft) Metrics
        AVG(CASE WHEN f.service_type = 'hvfhv' THEN f.price_per_mile END) as hvfhv_avg_price_per_mile,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN f.service_type = 'hvfhv' THEN f.price_per_mile END) as hvfhv_median_price_per_mile,
        AVG(CASE WHEN f.service_type = 'hvfhv' THEN f.price_per_minute END) as hvfhv_avg_price_per_minute,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN f.service_type = 'hvfhv' THEN f.price_per_minute END) as hvfhv_median_price_per_minute,
        COUNT(CASE WHEN f.service_type = 'hvfhv' THEN 1 END) as hvfhv_trip_count
        
    FROM fact_trips f
    JOIN dim_zones z ON f.pickup_zone_id = z.location_id
    JOIN dim_time t ON f.pickup_hour = t.hour
    WHERE f.is_valid = TRUE
        AND f.price_per_mile BETWEEN 0.5 AND 50
        AND f.price_per_minute BETWEEN 0.1 AND 10
        AND z.borough != 'Unknown'
    GROUP BY z.zone, z.borough, t.hour, t.hour_12, t.is_rush_hour
    HAVING COUNT(CASE WHEN f.service_type IN ('yellow', 'green') THEN 1 END) >= 10
        AND COUNT(CASE WHEN f.service_type = 'hvfhv' THEN 1 END) >= 10
)
SELECT 
    pickup_zone,
    borough,
    hour,
    hour_label,
    is_peak_hour,
    
    -- Taxi pricing
    ROUND(taxi_median_price_per_mile, 2) as taxi_price_per_mile,
    ROUND(taxi_median_price_per_minute, 2) as taxi_price_per_minute,
    taxi_trip_count,
    
    -- HVFHV pricing
    ROUND(hvfhv_median_price_per_mile, 2) as hvfhv_price_per_mile,
    ROUND(hvfhv_median_price_per_minute, 2) as hvfhv_price_per_minute,
    hvfhv_trip_count,
    
    -- Price differences (absolute)
    ROUND(hvfhv_median_price_per_mile - taxi_median_price_per_mile, 2) as price_diff_per_mile,
    ROUND(hvfhv_median_price_per_minute - taxi_median_price_per_minute, 2) as price_diff_per_minute,
    
    -- Price differences (percentage)
    ROUND((hvfhv_median_price_per_mile - taxi_median_price_per_mile) / NULLIF(taxi_median_price_per_mile, 0) * 100, 1) as price_diff_pct_per_mile,
    ROUND((hvfhv_median_price_per_minute - taxi_median_price_per_minute) / NULLIF(taxi_median_price_per_minute, 0) * 100, 1) as price_diff_pct_per_minute,
    
    -- Is HVFHV materially higher? (>10% premium)
    CASE 
        WHEN (hvfhv_median_price_per_mile - taxi_median_price_per_mile) / NULLIF(taxi_median_price_per_mile, 0) > 0.10 
        THEN 'YES - HVFHV Higher'
        WHEN (hvfhv_median_price_per_mile - taxi_median_price_per_mile) / NULLIF(taxi_median_price_per_mile, 0) < -0.10 
        THEN 'NO - Taxi Higher'
        ELSE 'Similar'
    END as pricing_verdict
    
FROM pricing_comparison
ORDER BY 
    CASE 
        WHEN (hvfhv_median_price_per_mile - taxi_median_price_per_mile) / NULLIF(taxi_median_price_per_mile, 0) > 0.10 
        THEN 1 ELSE 2 
    END,
    price_diff_pct_per_mile DESC
LIMIT 50;
