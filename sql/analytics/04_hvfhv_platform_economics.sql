-- HVFHV platform economics: take rates by company
-- Use case: Analyze platform commission structures

SELECT 
    c.company_name,
    COUNT(*) as total_trips,
    AVG(f.take_rate) * 100 as avg_take_rate_pct,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.take_rate) * 100 as median_take_rate_pct,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.take_rate) * 100 as p25_take_rate_pct,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.take_rate) * 100 as p75_take_rate_pct,
    AVG(f.driver_pay) as avg_driver_pay,
    AVG(f.total_fare - f.driver_pay) as avg_platform_commission,
    SUM(f.total_fare - f.driver_pay) as total_platform_revenue,
    SUM(f.driver_pay) as total_driver_pay,
    SUM(f.total_fare) as total_gross_revenue
FROM fact_trips f
JOIN dim_hvfhs_company c ON f.hvfhs_license_num = c.hvfhs_license_num
WHERE f.service_type = 'hvfhv'
    AND f.is_valid = TRUE
    AND f.take_rate BETWEEN 0 AND 1
GROUP BY c.company_name
ORDER BY total_trips DESC;
