"""Transform raw data to standardized fact table"""

from loguru import logger

from src.database.connection import DatabaseConnection


class DataTransformer:
    """Transform raw trip data to standardized fact_trips table"""

    @staticmethod
    def transform_yellow_to_fact():
        """Transform yellow taxi data to fact_trips"""
        logger.info(" Transforming yellow taxi data to fact_trips...")

        conn = DatabaseConnection.get_connection()

        # Optimize DuckDB settings - memory-efficient
        conn.execute("SET memory_limit='3GB'")
        conn.execute("SET threads=2")
        conn.execute("SET preserve_insertion_order=false")

        sql = """
        INSERT OR IGNORE INTO fact_trips (
            trip_id, service_type, pickup_datetime,
            pickup_date, pickup_hour, pickup_day_of_week,
            pickup_zone_id,
            trip_distance_miles, trip_duration_minutes,
            base_fare, tips, tolls, surcharges, airport_fee, taxes, total_fare,
            price_per_mile, price_per_minute, avg_speed_mph,
            is_valid, source_file
        )
        SELECT 
            -- Fast ID generation using hash
            MD5(CONCAT('y', CAST(tpep_pickup_datetime AS VARCHAR), CAST(trip_distance AS VARCHAR))) as trip_id,
            
            'yellow' as service_type,
            tpep_pickup_datetime as pickup_datetime,
            CAST(tpep_pickup_datetime AS DATE) as pickup_date,
            EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour,
            EXTRACT(DOW FROM tpep_pickup_datetime) as pickup_day_of_week,
            
            PULocationID as pickup_zone_id,
            
            trip_distance as trip_distance_miles,
            EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 as trip_duration_minutes,
            
            fare_amount as base_fare,
            tip_amount as tips,
            tolls_amount as tolls,
            extra + improvement_surcharge + congestion_surcharge as surcharges,
            Airport_fee as airport_fee,
            mta_tax as taxes,
            total_amount as total_fare,
            
            -- Derived metrics (simplified, zero-division safe)
            total_amount / NULLIF(trip_distance, 0) as price_per_mile,
            total_amount / NULLIF(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0, 0) as price_per_minute,
            trip_distance / NULLIF(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0, 0) as avg_speed_mph,
            
            -- Simplified quality check
            (total_amount >= 0 AND tpep_dropoff_datetime > tpep_pickup_datetime AND trip_distance >= 0) as is_valid,
            
            source_file
            
        FROM raw_yellow
        WHERE tpep_pickup_datetime IS NOT NULL
          AND tpep_dropoff_datetime IS NOT NULL
        """

        result = conn.execute(sql)
        row_count = result.fetchone()[0] if result else 0

        logger.success(f" Transformed {row_count:,} yellow taxi trips to fact_trips")
        return row_count

    @staticmethod
    def transform_green_to_fact():
        """Transform green taxi data to fact_trips"""
        logger.info(" Transforming green taxi data to fact_trips...")

        conn = DatabaseConnection.get_connection()

        # Optimize DuckDB settings - memory-efficient
        conn.execute("SET memory_limit='3GB'")
        conn.execute("SET threads=2")
        conn.execute("SET preserve_insertion_order=false")

        sql = """
        INSERT OR IGNORE INTO fact_trips (
            trip_id, service_type, pickup_datetime,
            pickup_date, pickup_hour, pickup_day_of_week,
            pickup_zone_id,
            trip_distance_miles, trip_duration_minutes,
            base_fare, tips, tolls, surcharges, airport_fee, taxes, total_fare,
            price_per_mile, price_per_minute, avg_speed_mph,
            is_valid, source_file
        )
        SELECT 
            MD5(CONCAT('g', CAST(lpep_pickup_datetime AS VARCHAR), CAST(trip_distance AS VARCHAR))) as trip_id,
            
            'green' as service_type,
            lpep_pickup_datetime as pickup_datetime,
            CAST(lpep_pickup_datetime AS DATE) as pickup_date,
            EXTRACT(HOUR FROM lpep_pickup_datetime) as pickup_hour,
            EXTRACT(DOW FROM lpep_pickup_datetime) as pickup_day_of_week,
            
            PULocationID as pickup_zone_id,
            
            trip_distance as trip_distance_miles,
            EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 60.0 as trip_duration_minutes,
            
            fare_amount as base_fare,
            tip_amount as tips,
            tolls_amount as tolls,
            extra + improvement_surcharge + congestion_surcharge as surcharges,
            NULL as airport_fee,  -- Green taxi doesn't have airport fee
            mta_tax as taxes,
            total_amount as total_fare,
            
            total_amount / NULLIF(trip_distance, 0) as price_per_mile,
            total_amount / NULLIF(EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 60.0, 0) as price_per_minute,
            trip_distance / NULLIF(EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0, 0) as avg_speed_mph,
            
            (total_amount >= 0 AND lpep_dropoff_datetime > lpep_pickup_datetime AND trip_distance >= 0) as is_valid,
            
            source_file
            
        FROM raw_green
        WHERE lpep_pickup_datetime IS NOT NULL
          AND lpep_dropoff_datetime IS NOT NULL
        """

        result = conn.execute(sql)
        row_count = result.fetchone()[0] if result else 0

        logger.success(f" Transformed {row_count:,} green taxi trips to fact_trips")
        return row_count

    @staticmethod
    def transform_hvfhv_to_fact():
        """Transform HVFHV data to fact_trips in small batches to avoid memory issues"""
        logger.info(" Transforming HVFHV data to fact_trips...")

        conn = DatabaseConnection.get_connection()

        # Optimize DuckDB settings - reduce memory to avoid OOM
        conn.execute("SET memory_limit='4GB'")  # Conservative limit
        conn.execute("SET threads=4")  # Fewer threads = less memory
        conn.execute("SET preserve_insertion_order=false")

        # Get total row count
        total_rows = conn.execute("SELECT COUNT(*) FROM raw_hvfhv").fetchone()[0]
        logger.info(f" Processing {total_rows:,} HVFHV rows in batches...")

        # Process in larger batches for better performance
        batch_size = 5_000_000  # 5M rows per batch
        total_inserted = 0
        offset = 0
        batch_num = 1

        while offset < total_rows:
            logger.info(
                f" Processing batch {batch_num}: rows {offset:,} to {min(offset + batch_size, total_rows):,}"
            )

            sql = f"""
            INSERT INTO fact_trips (
                trip_id, service_type, hvfhs_license_num,
                pickup_datetime,
                pickup_date, pickup_hour, pickup_day_of_week,
                pickup_zone_id,
                trip_distance_miles, trip_duration_minutes,
                base_fare, tips, tolls, surcharges, airport_fee, taxes, total_fare,
                driver_pay, take_rate,
                price_per_mile, price_per_minute, avg_speed_mph,
                is_shared_request,
                is_valid, source_file
            )
            SELECT 
                -- Simple unique ID (no MD5 overhead)
                'h_' || CAST({offset} AS VARCHAR) || '_' || CAST(ROW_NUMBER() OVER () AS VARCHAR) as trip_id,
                
                'hvfhv' as service_type,
                hvfhs_license_num,
                pickup_datetime,
                CAST(pickup_datetime AS DATE) as pickup_date,
                EXTRACT(HOUR FROM pickup_datetime) as pickup_hour,
                EXTRACT(DOW FROM pickup_datetime) as pickup_day_of_week,
                
                CAST(PULocationID AS INTEGER) as pickup_zone_id,
                
                trip_miles as trip_distance_miles,
                trip_time / 60.0 as trip_duration_minutes,
                
                base_passenger_fare as base_fare,
                tips,
                tolls,
                bcf + congestion_surcharge as surcharges,
                airport_fee,
                sales_tax as taxes,
                -- Pre-compute total_fare once
                (base_passenger_fare + tips + tolls + bcf + sales_tax + congestion_surcharge + airport_fee) as total_fare,
                
                driver_pay,
                -- Use computed total_fare alias in CTE would be better, but inline for now
                (base_passenger_fare + tips + tolls + bcf + sales_tax + congestion_surcharge + airport_fee - driver_pay) / 
                    NULLIF(base_passenger_fare + tips + tolls + bcf + sales_tax + congestion_surcharge + airport_fee, 0) as take_rate,
                
                (base_passenger_fare + tips + tolls + bcf + sales_tax + congestion_surcharge + airport_fee) / NULLIF(trip_miles, 0) as price_per_mile,
                (base_passenger_fare + tips + tolls + bcf + sales_tax + congestion_surcharge + airport_fee) / NULLIF(trip_time / 60.0, 0) as price_per_minute,
                trip_miles / NULLIF(trip_time / 3600.0, 0) as avg_speed_mph,
                
                (shared_request_flag = 'Y') as is_shared_request,
                
                (base_passenger_fare >= 0 AND dropoff_datetime > pickup_datetime AND trip_miles >= 0 AND driver_pay >= 0) as is_valid,
                
                source_file
                
            FROM raw_hvfhv
            WHERE pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
            LIMIT {batch_size} OFFSET {offset}
            """

            try:
                result = conn.execute(sql)
                batch_count = result.fetchone()[0] if result else 0
                total_inserted += batch_count
                logger.info(
                    f"  Batch {batch_num} complete: {batch_count:,} rows inserted"
                )
            except Exception as e:
                logger.error(f"  Batch {batch_num} failed: {e}")
                # Continue with next batch

            offset += batch_size
            batch_num += 1

        logger.success(f" Transformed {total_inserted:,} HVFHV trips to fact_trips")
        return total_inserted

    @staticmethod
    def transform_all():
        """Transform all raw data to fact_trips"""
        logger.info(" Starting full transformation to fact_trips...")

        yellow_count = DataTransformer.transform_yellow_to_fact()
        green_count = DataTransformer.transform_green_to_fact()
        hvfhv_count = DataTransformer.transform_hvfhv_to_fact()

        total = yellow_count + green_count + hvfhv_count

        logger.success(f" Total transformed: {total:,} trips")

        return {
            "yellow": yellow_count,
            "green": green_count,
            "hvfhv": hvfhv_count,
            "total": total,
        }
