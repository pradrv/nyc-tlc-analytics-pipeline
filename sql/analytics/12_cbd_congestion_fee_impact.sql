-- Question 2: How did rider prices change before vs. after Jan 5, 2025 (CBD congestion fee)?
-- Which operators passed through more?
-- Use case: Analyze the impact of CBD congestion pricing on fares

WITH cbd_zones AS (
    -- Manhattan south of 60th Street (CBD zones)
    SELECT location_id 
    FROM dim_zones 
    WHERE borough = 'Manhattan' 
        AND service_zone IN ('Yellow Zone', 'Boro Zone')
),
before_after_pricing AS (
    SELECT 
        CASE 
            WHEN f.pickup_date < '2025-01-05' THEN 'Before Jan 5, 2025'
            WHEN f.pickup_date >= '2025-01-05' THEN 'After Jan 5, 2025'
        END as period,
        f.service_type,
        
        -- Trip counts
        COUNT(*) as trip_count,
        
        -- Average fares
        AVG(f.total_fare) as avg_total_fare,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_fare) as median_total_fare,
        
        -- Pricing metrics
        AVG(f.price_per_mile) as avg_price_per_mile,
        AVG(f.price_per_minute) as avg_price_per_minute,
        
        -- Surcharges and fees
        AVG(f.surcharges) as avg_surcharges,
        AVG(COALESCE(f.airport_fee, 0)) as avg_airport_fee,
        
        -- Base fare (excluding surcharges)
        AVG(f.base_fare) as avg_base_fare,
        
        -- For HVFHV: driver pay and take rate
        AVG(CASE WHEN f.service_type = 'hvfhv' THEN f.driver_pay END) as avg_driver_pay,
        AVG(CASE WHEN f.service_type = 'hvfhv' THEN f.take_rate END) * 100 as avg_take_rate_pct
        
    FROM fact_trips f
    WHERE f.is_valid = TRUE
        AND f.pickup_zone_id IN (SELECT location_id FROM cbd_zones)
        AND f.pickup_date BETWEEN '2024-12-01' AND '2025-01-31'  -- 1 month before and after
    GROUP BY 
        CASE 
            WHEN f.pickup_date < '2025-01-05' THEN 'Before Jan 5, 2025'
            WHEN f.pickup_date >= '2025-01-05' THEN 'After Jan 5, 2025'
        END,
        f.service_type
)
SELECT 
    b.service_type,
    
    -- Before period
    b.trip_count as trips_before,
    ROUND(b.median_total_fare, 2) as median_fare_before,
    ROUND(b.avg_price_per_mile, 2) as price_per_mile_before,
    ROUND(b.avg_surcharges, 2) as avg_surcharges_before,
    ROUND(b.avg_driver_pay, 2) as driver_pay_before,
    ROUND(b.avg_take_rate_pct, 1) as take_rate_pct_before,
    
    -- After period
    a.trip_count as trips_after,
    ROUND(a.median_total_fare, 2) as median_fare_after,
    ROUND(a.avg_price_per_mile, 2) as price_per_mile_after,
    ROUND(a.avg_surcharges, 2) as avg_surcharges_after,
    ROUND(a.avg_driver_pay, 2) as driver_pay_after,
    ROUND(a.avg_take_rate_pct, 1) as take_rate_pct_after,
    
    -- Changes (absolute)
    ROUND(a.median_total_fare - b.median_total_fare, 2) as fare_change_dollars,
    ROUND(a.avg_price_per_mile - b.avg_price_per_mile, 2) as price_per_mile_change,
    ROUND(a.avg_surcharges - b.avg_surcharges, 2) as surcharge_change,
    ROUND(a.avg_driver_pay - b.avg_driver_pay, 2) as driver_pay_change,
    ROUND(a.avg_take_rate_pct - b.avg_take_rate_pct, 1) as take_rate_change_pct,
    
    -- Changes (percentage)
    ROUND((a.median_total_fare - b.median_total_fare) / NULLIF(b.median_total_fare, 0) * 100, 1) as fare_change_pct,
    ROUND((a.avg_surcharges - b.avg_surcharges) / NULLIF(b.avg_surcharges, 0) * 100, 1) as surcharge_change_pct,
    
    -- Pass-through analysis (who absorbed more of the fee?)
    CASE 
        WHEN b.service_type = 'hvfhv' THEN
            CASE 
                WHEN (a.avg_take_rate_pct - b.avg_take_rate_pct) > 1 THEN 'Platform kept more (driver absorbed fee)'
                WHEN (a.avg_take_rate_pct - b.avg_take_rate_pct) < -1 THEN 'Driver got more (platform absorbed fee)'
                ELSE 'Passed to rider'
            END
        ELSE 'N/A - Traditional Taxi'
    END as fee_absorption_pattern

FROM before_after_pricing b
LEFT JOIN before_after_pricing a 
    ON b.service_type = a.service_type 
    AND b.period = 'Before Jan 5, 2025' 
    AND a.period = 'After Jan 5, 2025'
WHERE b.period = 'Before Jan 5, 2025'
ORDER BY fare_change_pct DESC;
