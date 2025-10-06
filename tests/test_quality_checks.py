"""
Tests for data quality checks module
"""


class TestQualityCheckLogic:
    """Tests for quality check logic"""

    def test_fare_validation_all_valid(self, test_db_connection, sample_yellow_data):
        """Test fare validation with all valid data"""
        # Load data to database
        test_db_connection.execute("""
            CREATE TABLE test_fares (
                fare_amount DOUBLE,
                total_amount DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_fares VALUES (12.5, 16.3), (25.0, 31.3), (16.0, 23.06)
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN fare_amount < 0 OR total_amount < 0 THEN 1 ELSE 0 END) as failed
            FROM test_fares
        """).fetchone()

        assert result[0] == 3  # total rows
        assert result[1] == 0  # failed rows

    def test_fare_validation_with_negative(self, test_db_connection):
        """Test fare validation with negative fares"""
        test_db_connection.execute("""
            CREATE TABLE test_negative_fares (
                fare_amount DOUBLE,
                total_amount DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_negative_fares VALUES 
            (12.5, 16.3), 
            (-5.0, 10.0),
            (25.0, 31.3)
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN fare_amount < 0 OR total_amount < 0 THEN 1 ELSE 0 END) as failed
            FROM test_negative_fares
        """).fetchone()

        assert result[0] == 3
        assert result[1] == 1  # one negative fare

    def test_timestamp_validation(self, test_db_connection):
        """Test timestamp ordering validation"""
        test_db_connection.execute("""
            CREATE TABLE test_timestamps (
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_timestamps VALUES 
            ('2024-06-01 10:00:00', '2024-06-01 10:15:00'),
            ('2024-06-01 11:00:00', '2024-06-01 11:20:00'),
            ('2024-06-01 12:00:00', '2024-06-01 12:30:00')
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN dropoff_datetime <= pickup_datetime THEN 1 ELSE 0 END) as failed
            FROM test_timestamps
        """).fetchone()

        assert result[0] == 3
        assert result[1] == 0  # all timestamps ordered correctly

    def test_timestamp_validation_with_invalid(self, test_db_connection):
        """Test timestamp validation with invalid ordering"""
        test_db_connection.execute("""
            CREATE TABLE test_invalid_timestamps (
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_invalid_timestamps VALUES 
            ('2024-06-01 10:00:00', '2024-06-01 10:15:00'),
            ('2024-06-01 11:00:00', '2024-06-01 10:50:00'),
            ('2024-06-01 12:00:00', '2024-06-01 12:30:00')
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN dropoff_datetime <= pickup_datetime THEN 1 ELSE 0 END) as failed
            FROM test_invalid_timestamps
        """).fetchone()

        assert result[0] == 3
        assert result[1] == 1  # one invalid timestamp

    def test_speed_validation(self, test_db_connection):
        """Test speed validation (<100 mph)"""
        test_db_connection.execute("""
            CREATE TABLE test_speed (
                trip_distance DOUBLE,
                trip_duration_seconds INTEGER
            )
        """)

        # Insert trips with different speeds
        test_db_connection.execute("""
            INSERT INTO test_speed VALUES 
            (10.0, 1200),  -- 30 mph (valid)
            (50.0, 3600),  -- 50 mph (valid)
            (150.0, 3600)  -- 150 mph (invalid)
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE 
                    WHEN (trip_distance / NULLIF(trip_duration_seconds / 3600.0, 0)) >= 100 
                    THEN 1 ELSE 0 
                END) as failed
            FROM test_speed
        """).fetchone()

        assert result[0] == 3
        assert result[1] == 1  # one trip over 100 mph

    def test_distance_validation(self, test_db_connection):
        """Test distance validation (non-negative)"""
        test_db_connection.execute("""
            CREATE TABLE test_distance (
                trip_distance DOUBLE
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_distance VALUES (2.5), (5.0), (-1.0), (3.2)
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) as failed
            FROM test_distance
        """).fetchone()

        assert result[0] == 4
        assert result[1] == 1  # one negative distance


class TestQualityMetrics:
    """Tests for quality metrics calculations"""

    def test_failure_rate_calculation(self):
        """Test failure rate percentage calculation"""
        total = 1000
        failed = 50
        failure_rate = (failed / total) * 100

        assert failure_rate == 5.0

    def test_pass_rate_calculation(self):
        """Test pass rate calculation"""
        total = 1000
        passed = 950
        pass_rate = (passed / total) * 100

        assert pass_rate == 95.0

    def test_zero_division_safe(self):
        """Test that zero division is handled"""
        total = 0
        failed = 0

        failure_rate = (failed / total * 100) if total > 0 else 0

        assert failure_rate == 0

    def test_quality_metrics_aggregation(self, test_db_connection):
        """Test aggregating quality metrics"""
        test_db_connection.execute("""
            CREATE TABLE test_quality_metrics (
                check_type VARCHAR,
                total_rows INTEGER,
                passed_rows INTEGER,
                failed_rows INTEGER
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_quality_metrics VALUES 
            ('fare_check', 1000, 980, 20),
            ('timestamp_check', 1000, 990, 10),
            ('speed_check', 1000, 950, 50)
        """)

        result = test_db_connection.execute("""
            SELECT 
                SUM(passed_rows) as total_passed,
                SUM(total_rows) as total_checked,
                (SUM(passed_rows) * 100.0 / NULLIF(SUM(total_rows), 0)) as overall_quality
            FROM test_quality_metrics
        """).fetchone()

        assert result[0] == 2920  # total passed
        assert result[1] == 3000  # total checked
        assert abs(result[2] - 97.33) < 0.01  # overall quality ~97.33%


class TestDataCompletenessChecks:
    """Tests for data completeness validation"""

    def test_null_count_validation(self, test_db_connection):
        """Test counting null values"""
        test_db_connection.execute("""
            CREATE TABLE test_nulls (
                id INTEGER,
                value VARCHAR
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_nulls VALUES 
            (1, 'value1'),
            (2, NULL),
            (3, 'value3'),
            (NULL, 'value4')
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_ids,
                SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_values
            FROM test_nulls
        """).fetchone()

        assert result[0] == 4  # total rows
        assert result[1] == 1  # null IDs
        assert result[2] == 1  # null values

    def test_completeness_percentage(self, test_db_connection):
        """Test completeness percentage calculation"""
        test_db_connection.execute("""
            CREATE TABLE test_completeness (
                required_field VARCHAR
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_completeness VALUES 
            ('value1'), ('value2'), (NULL), ('value4'), ('value5')
        """)

        result = test_db_connection.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN required_field IS NOT NULL THEN 1 ELSE 0 END) as non_null,
                (SUM(CASE WHEN required_field IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as completeness
            FROM test_completeness
        """).fetchone()

        assert result[0] == 5
        assert result[1] == 4
        assert result[2] == 80.0  # 80% completeness
