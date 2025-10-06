-- Question 3: What are HVFHV take-rates over time (median, p25/p75)?
-- Which factors (zone, hour, trip length) explain the variance?
-- Use case: Deep dive into platform economics and commission structure drivers

-- Part A: Take rates over time (monthly trends)
WITH monthly_take_rates AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        c.company_name,
        
        COUNT(*) as trip_count,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.take_rate) * 100 as p25_take_rate,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.take_rate) * 100 as median_take_rate,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.take_rate) * 100 as p75_take_rate,
        AVG(f.take_rate) * 100 as avg_take_rate,
        STDDEV(f.take_rate) * 100 as stddev_take_rate,
        
        -- IQR (interquartile range - measure of variance)
        (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.take_rate) - 
         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.take_rate)) * 100 as iqr_take_rate
        
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date = d.date_id
    JOIN dim_hvfhs_company c ON f.hvfhs_license_num = c.hvfhs_license_num
    WHERE f.service_type = 'hvfhv'
        AND f.is_valid = TRUE
        AND f.take_rate BETWEEN 0 AND 1
    GROUP BY d.year, d.month, d.month_name, c.company_name
),

-- Part B: Take rate variance by zone (geographic factors)
zone_take_rates AS (
    SELECT 
        z.zone,
        z.borough,
        z.service_zone,
        c.company_name,
        
        COUNT(*) as trip_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.take_rate) * 100 as median_take_rate,
        STDDEV(f.take_rate) * 100 as stddev_take_rate,
        AVG(f.trip_distance_miles) as avg_trip_distance,
        AVG(f.total_fare) as avg_total_fare
        
    FROM fact_trips f
    JOIN dim_zones z ON f.pickup_zone_id = z.location_id
    JOIN dim_hvfhs_company c ON f.hvfhs_license_num = c.hvfhs_license_num
    WHERE f.service_type = 'hvfhv'
        AND f.is_valid = TRUE
        AND f.take_rate BETWEEN 0 AND 1
        AND z.borough != 'Unknown'
    GROUP BY z.zone, z.borough, z.service_zone, c.company_name
    HAVING COUNT(*) >= 100
),

-- Part C: Take rate variance by hour (temporal factors)
hourly_take_rates AS (
    SELECT 
        t.hour,
        t.hour_12 as hour_label,
        t.is_rush_hour as is_peak_hour,
        c.company_name,
        
        COUNT(*) as trip_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.take_rate) * 100 as median_take_rate,
        STDDEV(f.take_rate) * 100 as stddev_take_rate
        
    FROM fact_trips f
    JOIN dim_time t ON f.pickup_hour = t.hour
    JOIN dim_hvfhs_company c ON f.hvfhs_license_num = c.hvfhs_license_num
    WHERE f.service_type = 'hvfhv'
        AND f.is_valid = TRUE
        AND f.take_rate BETWEEN 0 AND 1
    GROUP BY t.hour, t.hour_12, t.is_rush_hour, c.company_name
),

-- Part D: Take rate variance by trip length (trip characteristics)
trip_length_take_rates AS (
    SELECT 
        CASE 
            WHEN f.trip_distance_miles < 2 THEN '1. Short (<2 mi)'
            WHEN f.trip_distance_miles < 5 THEN '2. Medium (2-5 mi)'
            WHEN f.trip_distance_miles < 10 THEN '3. Long (5-10 mi)'
            ELSE '4. Very Long (>10 mi)'
        END as trip_length_category,
        c.company_name,
        
        COUNT(*) as trip_count,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.take_rate) * 100 as p25_take_rate,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.take_rate) * 100 as median_take_rate,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.take_rate) * 100 as p75_take_rate,
        AVG(f.take_rate) * 100 as avg_take_rate,
        AVG(f.trip_distance_miles) as avg_distance,
        AVG(f.total_fare) as avg_fare
        
    FROM fact_trips f
    JOIN dim_hvfhs_company c ON f.hvfhs_license_num = c.hvfhs_license_num
    WHERE f.service_type = 'hvfhv'
        AND f.is_valid = TRUE
        AND f.take_rate BETWEEN 0 AND 1
    GROUP BY 
        CASE 
            WHEN f.trip_distance_miles < 2 THEN '1. Short (<2 mi)'
            WHEN f.trip_distance_miles < 5 THEN '2. Medium (2-5 mi)'
            WHEN f.trip_distance_miles < 10 THEN '3. Long (5-10 mi)'
            ELSE '4. Very Long (>10 mi)'
        END,
        c.company_name
)

-- Return monthly trends (uncomment the section you want to analyze)
SELECT * FROM monthly_take_rates ORDER BY year, month, company_name;

-- To analyze by zone, replace above with:
-- SELECT * FROM zone_take_rates ORDER BY stddev_take_rate DESC LIMIT 30;

-- To analyze by hour, replace above with:
-- SELECT * FROM hourly_take_rates ORDER BY hour, company_name;

-- To analyze by trip length, replace above with:
-- SELECT * FROM trip_length_take_rates ORDER BY trip_length_category, company_name;
