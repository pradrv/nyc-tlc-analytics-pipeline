"""Build aggregate tables for fast analytics"""

from loguru import logger

from src.database.connection import DatabaseConnection


class AggregationBuilder:
    """Build pre-computed aggregate tables"""

    @staticmethod
    def build_pricing_by_zone_hour():
        """Build agg_pricing_by_zone_hour table"""
        logger.info(" Building agg_pricing_by_zone_hour...")

        conn = DatabaseConnection.get_connection()

        # Clear existing data
        conn.execute("DELETE FROM agg_pricing_by_zone_hour")

        sql = """
        INSERT INTO agg_pricing_by_zone_hour
        SELECT 
            service_type,
            pickup_zone_id,
            pickup_hour,
            pickup_date as trip_date,
            
            COUNT(*) as trip_count,
            SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_trip_count,
            
            AVG(trip_distance_miles) as avg_trip_distance,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance_miles) as median_trip_distance,
            SUM(trip_distance_miles) as total_trip_miles,
            
            AVG(trip_duration_minutes) as avg_trip_duration,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_duration_minutes) as median_trip_duration,
            
            AVG(price_per_mile) as avg_price_per_mile,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_mile) as median_price_per_mile,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_mile) as p25_price_per_mile,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_mile) as p75_price_per_mile,
            
            AVG(price_per_minute) as avg_price_per_minute,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_minute) as median_price_per_minute,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_minute) as p25_price_per_minute,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_minute) as p75_price_per_minute,
            
            AVG(total_fare) as avg_total_fare,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare) as median_total_fare,
            SUM(total_fare) as total_revenue,
            
            0 as trips_with_cbd_fee,
            0 as avg_cbd_fee,
            0 as total_cbd_fee
            
        FROM fact_trips
        WHERE is_valid = TRUE
            AND price_per_mile IS NOT NULL
            AND price_per_mile BETWEEN 0.5 AND 50
        GROUP BY service_type, pickup_zone_id, pickup_hour, pickup_date
        """

        conn.execute(sql)
        row_count = DatabaseConnection.get_table_row_count("agg_pricing_by_zone_hour")

        logger.success(f" Built agg_pricing_by_zone_hour: {row_count:,} rows")
        return row_count

    @staticmethod
    def build_hvfhv_take_rates():
        """Build agg_hvfhv_take_rates table"""
        logger.info(" Building agg_hvfhv_take_rates...")

        conn = DatabaseConnection.get_connection()

        conn.execute("DELETE FROM agg_hvfhv_take_rates")

        sql = """
        INSERT INTO agg_hvfhv_take_rates
        SELECT 
            pickup_date as trip_date,
            pickup_zone_id,
            pickup_hour,
            hvfhs_license_num,
            
            COUNT(*) as trip_count,
            AVG(trip_distance_miles) as avg_trip_distance,
            AVG(trip_duration_minutes) as avg_trip_duration,
            
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY take_rate) as median_take_rate,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY take_rate) as p25_take_rate,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY take_rate) as p75_take_rate,
            AVG(take_rate) as avg_take_rate,
            STDDEV(take_rate) as stddev_take_rate,
            
            AVG(driver_pay) as avg_driver_pay,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY driver_pay) as median_driver_pay,
            SUM(driver_pay) as total_driver_pay,
            
            AVG(total_fare - driver_pay) as avg_platform_commission,
            SUM(total_fare - driver_pay) as total_platform_commission,
            
            AVG(total_fare) as avg_total_fare,
            SUM(total_fare) as total_revenue
            
        FROM fact_trips
        WHERE service_type = 'hvfhv'
            AND is_valid = TRUE
            AND take_rate IS NOT NULL
            AND take_rate BETWEEN 0 AND 1
        GROUP BY pickup_date, pickup_zone_id, pickup_hour, hvfhs_license_num
        HAVING COUNT(*) >= 5
        """

        conn.execute(sql)
        row_count = DatabaseConnection.get_table_row_count("agg_hvfhv_take_rates")

        logger.success(f" Built agg_hvfhv_take_rates: {row_count:,} rows")
        return row_count

    @staticmethod
    def build_market_share():
        """Build agg_market_share table"""
        logger.info(" Building agg_market_share...")

        conn = DatabaseConnection.get_connection()

        conn.execute("DELETE FROM agg_market_share")

        sql = """
        INSERT INTO agg_market_share
        SELECT 
            pickup_date as trip_date,
            pickup_zone_id,
            
            SUM(CASE WHEN service_type = 'yellow' THEN 1 ELSE 0 END) as yellow_trips,
            SUM(CASE WHEN service_type = 'green' THEN 1 ELSE 0 END) as green_trips,
            SUM(CASE WHEN service_type = 'hvfhv' THEN 1 ELSE 0 END) as hvfhv_trips,
            COUNT(*) as total_trips,
            
            SUM(CASE WHEN service_type = 'yellow' THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) as yellow_share,
            SUM(CASE WHEN service_type = 'green' THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) as green_share,
            SUM(CASE WHEN service_type = 'hvfhv' THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) as hvfhv_share,
            
            AVG(CASE WHEN service_type = 'yellow' THEN price_per_mile END) as yellow_avg_price_per_mile,
            AVG(CASE WHEN service_type = 'green' THEN price_per_mile END) as green_avg_price_per_mile,
            AVG(CASE WHEN service_type = 'hvfhv' THEN price_per_mile END) as hvfhv_avg_price_per_mile,
            
            SUM(CASE WHEN service_type = 'yellow' THEN total_fare ELSE 0 END) as yellow_total_revenue,
            SUM(CASE WHEN service_type = 'green' THEN total_fare ELSE 0 END) as green_total_revenue,
            SUM(CASE WHEN service_type = 'hvfhv' THEN total_fare ELSE 0 END) as hvfhv_total_revenue,
            SUM(total_fare) as total_revenue,
            
            SUM(CASE WHEN service_type = 'yellow' THEN total_fare ELSE 0 END) / NULLIF(SUM(total_fare), 0) as yellow_revenue_share,
            SUM(CASE WHEN service_type = 'green' THEN total_fare ELSE 0 END) / NULLIF(SUM(total_fare), 0) as green_revenue_share,
            SUM(CASE WHEN service_type = 'hvfhv' THEN total_fare ELSE 0 END) / NULLIF(SUM(total_fare), 0) as hvfhv_revenue_share
            
        FROM fact_trips
        WHERE is_valid = TRUE
        GROUP BY pickup_date, pickup_zone_id
        HAVING COUNT(*) >= 10
        """

        conn.execute(sql)
        row_count = DatabaseConnection.get_table_row_count("agg_market_share")

        logger.success(f" Built agg_market_share: {row_count:,} rows")
        return row_count

    @staticmethod
    def build_daily_summary():
        """Build agg_daily_summary table"""
        logger.info(" Building agg_daily_summary...")

        conn = DatabaseConnection.get_connection()

        conn.execute("DELETE FROM agg_daily_summary")

        sql = """
        INSERT INTO agg_daily_summary
        SELECT 
            pickup_date as trip_date,
            
            COUNT(*) as total_trips,
            SUM(total_fare) as total_revenue,
            AVG(trip_distance_miles) as avg_trip_distance,
            AVG(trip_duration_minutes) as avg_trip_duration,
            
            SUM(CASE WHEN service_type = 'yellow' THEN 1 ELSE 0 END) as yellow_trips,
            SUM(CASE WHEN service_type = 'green' THEN 1 ELSE 0 END) as green_trips,
            SUM(CASE WHEN service_type = 'hvfhv' THEN 1 ELSE 0 END) as hvfhv_trips,
            
            SUM(CASE WHEN service_type = 'yellow' THEN total_fare ELSE 0 END) as yellow_revenue,
            SUM(CASE WHEN service_type = 'green' THEN total_fare ELSE 0 END) as green_revenue,
            SUM(CASE WHEN service_type = 'hvfhv' THEN total_fare ELSE 0 END) as hvfhv_revenue,
            
            SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as total_valid_trips,
            SUM(CASE WHEN is_valid THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) as data_quality_score,
            
            0 as total_cbd_fees,
            0 as trips_with_cbd_fee
            
        FROM fact_trips
        GROUP BY pickup_date
        """

        conn.execute(sql)
        row_count = DatabaseConnection.get_table_row_count("agg_daily_summary")

        logger.success(f" Built agg_daily_summary: {row_count:,} rows")
        return row_count

    @staticmethod
    def build_all():
        """Build all aggregate tables"""
        logger.info(" Building all aggregate tables...")

        pricing_rows = AggregationBuilder.build_pricing_by_zone_hour()
        take_rate_rows = AggregationBuilder.build_hvfhv_take_rates()
        market_share_rows = AggregationBuilder.build_market_share()
        daily_rows = AggregationBuilder.build_daily_summary()

        total = pricing_rows + take_rate_rows + market_share_rows + daily_rows

        logger.success(f" Built all aggregates: {total:,} total rows")

        return {
            "pricing": pricing_rows,
            "take_rates": take_rate_rows,
            "market_share": market_share_rows,
            "daily_summary": daily_rows,
            "total": total,
        }
