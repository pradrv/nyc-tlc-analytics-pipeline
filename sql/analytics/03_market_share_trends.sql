-- Market share trends over time
-- Use case: Track competitive dynamics between yellow/green taxis and HVFHV

SELECT 
    d.date_id as date,
    d.month_name,
    d.year,
    m.total_trips,
    m.yellow_trips,
    m.green_trips,
    m.hvfhv_trips,
    ROUND(m.yellow_share * 100, 2) as yellow_share_pct,
    ROUND(m.green_share * 100, 2) as green_share_pct,
    ROUND(m.hvfhv_share * 100, 2) as hvfhv_share_pct,
    m.total_revenue,
    ROUND(m.yellow_revenue_share * 100, 2) as yellow_revenue_share_pct,
    ROUND(m.green_revenue_share * 100, 2) as green_revenue_share_pct,
    ROUND(m.hvfhv_revenue_share * 100, 2) as hvfhv_revenue_share_pct
FROM (
    SELECT 
        trip_date,
        SUM(total_trips) as total_trips,
        SUM(yellow_trips) as yellow_trips,
        SUM(green_trips) as green_trips,
        SUM(hvfhv_trips) as hvfhv_trips,
        AVG(yellow_share) as yellow_share,
        AVG(green_share) as green_share,
        AVG(hvfhv_share) as hvfhv_share,
        SUM(total_revenue) as total_revenue,
        SUM(yellow_total_revenue) / NULLIF(SUM(total_revenue), 0) as yellow_revenue_share,
        SUM(green_total_revenue) / NULLIF(SUM(total_revenue), 0) as green_revenue_share,
        SUM(hvfhv_total_revenue) / NULLIF(SUM(total_revenue), 0) as hvfhv_revenue_share
    FROM agg_market_share
    GROUP BY trip_date
) m
JOIN dim_date d ON m.trip_date = d.date_id
ORDER BY d.date_id;
