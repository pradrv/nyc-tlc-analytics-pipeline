"""
Tests for data transformations
"""

import hashlib


class TestTripIDGeneration:
    """Tests for trip ID generation"""

    def test_trip_id_generation(self):
        """Test generating MD5 trip ID"""
        components = [
            "yellow",
            "2024-06-01 10:00:00",
            "2024-06-01 10:15:00",
            "2.5",
            "16.3",
        ]

        trip_id = hashlib.md5("".join(components).encode()).hexdigest()

        assert len(trip_id) == 32  # MD5 hash length
        assert isinstance(trip_id, str)

    def test_trip_id_uniqueness(self):
        """Test that different trips generate different IDs"""
        trip1 = hashlib.md5("yellow2024-06-0110:00:002.516.3".encode()).hexdigest()
        trip2 = hashlib.md5("yellow2024-06-0111:00:003.020.0".encode()).hexdigest()

        assert trip1 != trip2

    def test_trip_id_consistency(self):
        """Test that same trip data generates same ID"""
        data = "yellow2024-06-0110:00:002.516.3"
        trip_id1 = hashlib.md5(data.encode()).hexdigest()
        trip_id2 = hashlib.md5(data.encode()).hexdigest()

        assert trip_id1 == trip_id2


class TestPriceCalculations:
    """Tests for price per mile and per minute calculations"""

    def test_price_per_mile(self, test_db_connection):
        """Test price per mile calculation"""
        test_db_connection.execute("""
            CREATE TABLE test_pricing (
                total_fare DOUBLE,
                trip_distance DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_pricing VALUES (20.0, 4.0), (30.0, 5.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                total_fare / NULLIF(trip_distance, 0) as price_per_mile
            FROM test_pricing
        """).fetchall()

        assert abs(result[0][0] - 5.0) < 0.01
        assert abs(result[1][0] - 6.0) < 0.01

    def test_price_per_minute(self, test_db_connection):
        """Test price per minute calculation"""
        test_db_connection.execute("""
            CREATE TABLE test_timing (
                total_fare DOUBLE,
                trip_duration_minutes DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_timing VALUES (20.0, 10.0), (30.0, 15.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                total_fare / NULLIF(trip_duration_minutes, 0) as price_per_minute
            FROM test_timing
        """).fetchall()

        assert abs(result[0][0] - 2.0) < 0.01
        assert abs(result[1][0] - 2.0) < 0.01

    def test_zero_distance_handling(self, test_db_connection):
        """Test handling of zero distance (should return NULL)"""
        test_db_connection.execute("""
            CREATE TABLE test_zero_distance (
                total_fare DOUBLE,
                trip_distance DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_zero_distance VALUES (20.0, 0.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                CASE 
                    WHEN trip_distance > 0.1 
                    THEN total_fare / trip_distance 
                    ELSE NULL 
                END as price_per_mile
            FROM test_zero_distance
        """).fetchone()

        assert result[0] is None


class TestSpeedCalculations:
    """Tests for speed calculations"""

    def test_average_speed_calculation(self, test_db_connection):
        """Test average speed (mph) calculation"""
        test_db_connection.execute("""
            CREATE TABLE test_speed_calc (
                trip_distance DOUBLE,
                trip_duration_hours DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_speed_calc VALUES (30.0, 1.0), (50.0, 2.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                trip_distance / NULLIF(trip_duration_hours, 0) as avg_speed_mph
            FROM test_speed_calc
        """).fetchall()

        assert abs(result[0][0] - 30.0) < 0.01
        assert abs(result[1][0] - 25.0) < 0.01

    def test_speed_from_seconds(self, test_db_connection):
        """Test converting duration from seconds to hours for speed"""
        test_db_connection.execute("""
            CREATE TABLE test_speed_seconds (
                trip_distance DOUBLE,
                trip_duration_seconds INTEGER
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_speed_seconds VALUES (30.0, 3600)
        """)

        result = test_db_connection.execute("""
            SELECT 
                trip_distance / NULLIF(trip_duration_seconds / 3600.0, 0) as avg_speed_mph
            FROM test_speed_seconds
        """).fetchone()

        assert abs(result[0] - 30.0) < 0.01


class TestTakeRateCalculations:
    """Tests for HVFHV take rate calculations"""

    def test_take_rate_calculation(self, test_db_connection):
        """Test take rate percentage calculation"""
        test_db_connection.execute("""
            CREATE TABLE test_take_rate (
                total_fare DOUBLE,
                driver_pay DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_take_rate VALUES (100.0, 75.0), (50.0, 40.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                (total_fare - driver_pay) / NULLIF(total_fare, 0) as take_rate
            FROM test_take_rate
        """).fetchall()

        assert abs(result[0][0] - 0.25) < 0.01  # 25% take rate
        assert abs(result[1][0] - 0.20) < 0.01  # 20% take rate

    def test_take_rate_edge_cases(self, test_db_connection):
        """Test take rate with edge cases"""
        test_db_connection.execute("""
            CREATE TABLE test_take_rate_edges (
                total_fare DOUBLE,
                driver_pay DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_take_rate_edges VALUES 
            (100.0, 100.0),  -- 0% take rate
            (100.0, 0.0),    -- 100% take rate
            (0.0, 0.0)       -- undefined
        """)

        result = test_db_connection.execute("""
            SELECT 
                CASE 
                    WHEN total_fare > 0 
                    THEN (total_fare - driver_pay) / total_fare 
                    ELSE NULL 
                END as take_rate
            FROM test_take_rate_edges
        """).fetchall()

        assert abs(result[0][0] - 0.0) < 0.01  # 0%
        assert abs(result[1][0] - 1.0) < 0.01  # 100%
        assert result[2][0] is None  # NULL


class TestValidationFlags:
    """Tests for is_valid flag calculation"""

    def test_valid_trip(self, test_db_connection):
        """Test that valid trips are flagged correctly"""
        test_db_connection.execute("""
            CREATE TABLE test_validation (
                fare_amount DOUBLE,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                trip_distance DOUBLE,
                avg_speed_mph DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_validation VALUES 
            (20.0, '2024-06-01 10:00:00', '2024-06-01 10:15:00', 5.0, 30.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                CASE 
                    WHEN fare_amount >= 0 
                    AND dropoff_datetime > pickup_datetime
                    AND trip_distance >= 0
                    AND avg_speed_mph < 100
                    THEN TRUE
                    ELSE FALSE
                END as is_valid
            FROM test_validation
        """).fetchone()

        assert result[0]

    def test_invalid_negative_fare(self, test_db_connection):
        """Test that negative fare is flagged as invalid"""
        test_db_connection.execute("""
            CREATE TABLE test_invalid_fare (
                fare_amount DOUBLE,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                trip_distance DOUBLE,
                avg_speed_mph DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_invalid_fare VALUES 
            (-5.0, '2024-06-01 10:00:00', '2024-06-01 10:15:00', 5.0, 30.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                CASE 
                    WHEN fare_amount >= 0 
                    AND dropoff_datetime > pickup_datetime
                    AND trip_distance >= 0
                    AND avg_speed_mph < 100
                    THEN TRUE
                    ELSE FALSE
                END as is_valid
            FROM test_invalid_fare
        """).fetchone()

        assert not result[0]

    def test_invalid_timestamp_order(self, test_db_connection):
        """Test that invalid timestamp order is flagged"""
        test_db_connection.execute("""
            CREATE TABLE test_invalid_time (
                fare_amount DOUBLE,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                trip_distance DOUBLE,
                avg_speed_mph DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_invalid_time VALUES 
            (20.0, '2024-06-01 10:15:00', '2024-06-01 10:00:00', 5.0, 30.0)
        """)

        result = test_db_connection.execute("""
            SELECT 
                CASE 
                    WHEN fare_amount >= 0 
                    AND dropoff_datetime > pickup_datetime
                    AND trip_distance >= 0
                    AND avg_speed_mph < 100
                    THEN TRUE
                    ELSE FALSE
                END as is_valid
            FROM test_invalid_time
        """).fetchone()

        assert not result[0]


class TestDateExtractions:
    """Tests for date and time extractions"""

    def test_extract_date(self, test_db_connection):
        """Test extracting date from timestamp"""
        result = test_db_connection.execute("""
            SELECT CAST('2024-06-01 10:15:30' AS DATE) as pickup_date
        """).fetchone()

        assert str(result[0]) == "2024-06-01"

    def test_extract_hour(self, test_db_connection):
        """Test extracting hour from timestamp"""
        result = test_db_connection.execute("""
            SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-01 14:15:30') as pickup_hour
        """).fetchone()

        assert result[0] == 14

    def test_extract_day_of_week(self, test_db_connection):
        """Test extracting day of week from timestamp"""
        result = test_db_connection.execute("""
            SELECT EXTRACT(DOW FROM DATE '2024-06-01') as day_of_week
        """).fetchone()

        # 2024-06-01 is a Saturday (DOW = 6)
        assert result[0] == 6
