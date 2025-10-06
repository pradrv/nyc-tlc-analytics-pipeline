-- Question 4: Where is market share shifting (operator share of trips by zone/day)?
-- Is share correlated with relative price levels?
-- Use case: Analyze competitive dynamics and price elasticity

WITH daily_zone_metrics AS (
    SELECT 
        f.pickup_date,
        z.zone,
        z.borough,
        f.service_type,
        
        COUNT(*) as trip_count,
        AVG(f.price_per_mile) as avg_price_per_mile,
        AVG(f.total_fare) as avg_total_fare,
        SUM(f.total_fare) as total_revenue
        
    FROM fact_trips f
    JOIN dim_zones z ON f.pickup_zone_id = z.location_id
    WHERE f.is_valid = TRUE
        AND f.price_per_mile BETWEEN 0.5 AND 50
        AND z.borough != 'Unknown'
    GROUP BY f.pickup_date, z.zone, z.borough, f.service_type
),

zone_day_totals AS (
    SELECT 
        pickup_date,
        zone,
        borough,
        SUM(trip_count) as total_trips,
        SUM(total_revenue) as total_revenue
    FROM daily_zone_metrics
    GROUP BY pickup_date, zone, borough
),

market_share_with_pricing AS (
    SELECT 
        m.pickup_date,
        m.zone,
        m.borough,
        m.service_type,
        
        -- Market share metrics
        m.trip_count,
        t.total_trips,
        ROUND(m.trip_count * 100.0 / NULLIF(t.total_trips, 0), 2) as market_share_pct,
        
        -- Pricing metrics
        ROUND(m.avg_price_per_mile, 2) as price_per_mile,
        ROUND(m.avg_total_fare, 2) as avg_fare,
        
        -- Revenue share
        ROUND(m.total_revenue * 100.0 / NULLIF(t.total_revenue, 0), 2) as revenue_share_pct
        
    FROM daily_zone_metrics m
    JOIN zone_day_totals t 
        ON m.pickup_date = t.pickup_date 
        AND m.zone = t.zone
    WHERE t.total_trips >= 50  -- Minimum volume threshold
),

-- Calculate market share changes over time
market_share_changes AS (
    SELECT 
        zone,
        borough,
        service_type,
        
        -- Early period (first 25% of dates)
        AVG(CASE WHEN pickup_date <= (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pickup_date) FROM market_share_with_pricing) 
            THEN market_share_pct END) as early_market_share,
        AVG(CASE WHEN pickup_date <= (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pickup_date) FROM market_share_with_pricing) 
            THEN price_per_mile END) as early_price,
        
        -- Late period (last 25% of dates)
        AVG(CASE WHEN pickup_date >= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pickup_date) FROM market_share_with_pricing) 
            THEN market_share_pct END) as late_market_share,
        AVG(CASE WHEN pickup_date >= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pickup_date) FROM market_share_with_pricing) 
            THEN price_per_mile END) as late_price,
        
        -- Overall averages
        AVG(market_share_pct) as avg_market_share,
        AVG(price_per_mile) as avg_price,
        COUNT(DISTINCT pickup_date) as days_active,
        SUM(trip_count) as total_trips
        
    FROM market_share_with_pricing
    GROUP BY zone, borough, service_type
    HAVING COUNT(DISTINCT pickup_date) >= 10  -- At least 10 days of data
),

-- Calculate relative pricing (vs. market average)
relative_pricing AS (
    SELECT 
        m.*,
        
        -- Market share change
        ROUND(m.late_market_share - m.early_market_share, 2) as market_share_change_pct,
        
        -- Price change
        ROUND(m.late_price - m.early_price, 2) as price_change,
        ROUND((m.late_price - m.early_price) / NULLIF(m.early_price, 0) * 100, 1) as price_change_pct,
        
        -- Relative pricing (vs zone average across all services)
        ROUND(m.avg_price - AVG(m.avg_price) OVER (PARTITION BY m.zone), 2) as price_vs_zone_avg,
        
        -- Classification
        CASE 
            WHEN m.late_market_share - m.early_market_share > 5 THEN 'Gaining Share'
            WHEN m.late_market_share - m.early_market_share < -5 THEN 'Losing Share'
            ELSE 'Stable'
        END as share_trend,
        
        CASE 
            WHEN m.avg_price > AVG(m.avg_price) OVER (PARTITION BY m.zone) * 1.1 THEN 'Premium Priced'
            WHEN m.avg_price < AVG(m.avg_price) OVER (PARTITION BY m.zone) * 0.9 THEN 'Discount Priced'
            ELSE 'Market Priced'
        END as pricing_position
        
    FROM market_share_changes m
)

-- Final output: Market share shifts correlated with pricing
SELECT 
    zone,
    borough,
    service_type,
    
    -- Market share metrics
    ROUND(early_market_share, 1) as early_share_pct,
    ROUND(late_market_share, 1) as late_share_pct,
    market_share_change_pct as share_change_pct,
    share_trend,
    
    -- Pricing metrics
    ROUND(early_price, 2) as early_price_per_mile,
    ROUND(late_price, 2) as late_price_per_mile,
    price_change as price_change_dollars,
    price_change_pct,
    pricing_position,
    
    -- Relative pricing
    price_vs_zone_avg as price_premium_vs_zone,
    
    -- Volume
    total_trips,
    days_active,
    
    -- Correlation insight
    CASE 
        WHEN share_trend = 'Gaining Share' AND pricing_position = 'Discount Priced' 
            THEN '✓ Gaining via Lower Price'
        WHEN share_trend = 'Gaining Share' AND pricing_position = 'Premium Priced' 
            THEN '✓ Gaining Despite Higher Price (Quality/Service)'
        WHEN share_trend = 'Losing Share' AND pricing_position = 'Premium Priced' 
            THEN '✗ Losing due to High Price'
        WHEN share_trend = 'Losing Share' AND pricing_position = 'Discount Priced' 
            THEN '✗ Losing Despite Low Price (Quality Issues?)'
        ELSE 'Stable Market'
    END as competitive_insight
    
FROM relative_pricing
WHERE total_trips >= 500  -- Meaningful volume
ORDER BY ABS(market_share_change_pct) DESC
LIMIT 100;
