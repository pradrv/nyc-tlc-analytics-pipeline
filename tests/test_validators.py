"""
Tests for data validation module
"""

import pandas as pd


class TestSchemaValidation:
    """Tests for schema validation"""

    def test_validate_yellow_schema(self, test_data_dir, sample_yellow_data):
        """Test validation of yellow taxi schema"""
        # Write sample data to parquet
        parquet_file = test_data_dir / "yellow_test.parquet"
        sample_yellow_data.to_parquet(parquet_file, index=False)

        # Read and validate schema
        df = pd.read_parquet(parquet_file)

        expected_columns = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]

        for col in expected_columns:
            assert col in df.columns

    def test_validate_green_schema(self, test_data_dir, sample_green_data):
        """Test validation of green taxi schema"""
        parquet_file = test_data_dir / "green_test.parquet"
        sample_green_data.to_parquet(parquet_file, index=False)

        df = pd.read_parquet(parquet_file)

        expected_columns = [
            "VendorID",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]

        for col in expected_columns:
            assert col in df.columns

    def test_validate_hvfhv_schema(self, test_data_dir, sample_hvfhv_data):
        """Test validation of HVFHV schema"""
        parquet_file = test_data_dir / "hvfhv_test.parquet"
        sample_hvfhv_data.to_parquet(parquet_file, index=False)

        df = pd.read_parquet(parquet_file)

        expected_columns = [
            "hvfhs_license_num",
            "pickup_datetime",
            "dropoff_datetime",
            "trip_miles",
            "base_passenger_fare",
            "driver_pay",
        ]

        for col in expected_columns:
            assert col in df.columns


class TestDataQualityValidation:
    """Tests for data quality validation"""

    def test_non_negative_fares(self, sample_yellow_data):
        """Test that fares are non-negative"""
        assert (sample_yellow_data["fare_amount"] >= 0).all()
        assert (sample_yellow_data["total_amount"] >= 0).all()

    def test_ordered_timestamps(self, sample_yellow_data):
        """Test that dropoff is after pickup"""
        assert (
            sample_yellow_data["tpep_dropoff_datetime"]
            > sample_yellow_data["tpep_pickup_datetime"]
        ).all()

    def test_positive_trip_distance(self, sample_yellow_data):
        """Test that trip distances are positive"""
        assert (sample_yellow_data["trip_distance"] >= 0).all()

    def test_realistic_speed(self, sample_yellow_data):
        """Test that average speeds are realistic (<100 mph)"""
        duration_hours = (
            sample_yellow_data["tpep_dropoff_datetime"]
            - sample_yellow_data["tpep_pickup_datetime"]
        ).dt.total_seconds() / 3600

        speeds = sample_yellow_data["trip_distance"] / duration_hours

        assert (speeds < 100).all()

    def test_valid_location_ids(self, sample_yellow_data):
        """Test that location IDs are positive integers"""
        assert (sample_yellow_data["PULocationID"] > 0).all()
        assert (sample_yellow_data["DOLocationID"] > 0).all()

    def test_no_null_required_fields(self, sample_yellow_data):
        """Test that required fields have no nulls"""
        required_fields = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]

        for field in required_fields:
            assert sample_yellow_data[field].notna().all()


class TestParquetFileValidation:
    """Tests for parquet file validation"""

    def test_parquet_row_count(self, test_data_dir, sample_yellow_data):
        """Test counting rows in parquet file"""
        parquet_file = test_data_dir / "count_test.parquet"
        sample_yellow_data.to_parquet(parquet_file, index=False)

        df = pd.read_parquet(parquet_file)

        assert len(df) == len(sample_yellow_data)

    def test_parquet_column_count(self, test_data_dir, sample_yellow_data):
        """Test counting columns in parquet file"""
        parquet_file = test_data_dir / "columns_test.parquet"
        sample_yellow_data.to_parquet(parquet_file, index=False)

        df = pd.read_parquet(parquet_file)

        assert len(df.columns) == len(sample_yellow_data.columns)

    def test_parquet_file_exists(self, test_data_dir, sample_yellow_data):
        """Test that parquet file is created"""
        parquet_file = test_data_dir / "exists_test.parquet"
        sample_yellow_data.to_parquet(parquet_file, index=False)

        assert parquet_file.exists()
        assert parquet_file.stat().st_size > 0

    def test_parquet_round_trip(self, test_data_dir, sample_yellow_data):
        """Test writing and reading parquet maintains data"""
        parquet_file = test_data_dir / "roundtrip_test.parquet"
        sample_yellow_data.to_parquet(parquet_file, index=False)

        df_read = pd.read_parquet(parquet_file)

        # Check key numeric columns
        pd.testing.assert_series_equal(
            sample_yellow_data["trip_distance"],
            df_read["trip_distance"],
            check_names=True,
        )

        pd.testing.assert_series_equal(
            sample_yellow_data["fare_amount"], df_read["fare_amount"], check_names=True
        )


class TestSchemaDriftDetection:
    """Tests for schema drift detection"""

    def test_detect_missing_columns(self, test_data_dir):
        """Test detection of missing columns"""
        # Create parquet with fewer columns
        df = pd.DataFrame({"VendorID": ["1", "2"], "trip_distance": [2.5, 3.0]})
        parquet_file = test_data_dir / "missing_cols.parquet"
        df.to_parquet(parquet_file, index=False)

        df_read = pd.read_parquet(parquet_file)

        expected_columns = ["VendorID", "trip_distance", "fare_amount"]
        missing = set(expected_columns) - set(df_read.columns)

        assert "fare_amount" in missing

    def test_detect_extra_columns(self, test_data_dir):
        """Test detection of extra columns"""
        df = pd.DataFrame(
            {
                "VendorID": ["1", "2"],
                "trip_distance": [2.5, 3.0],
                "extra_column": ["a", "b"],
            }
        )
        parquet_file = test_data_dir / "extra_cols.parquet"
        df.to_parquet(parquet_file, index=False)

        df_read = pd.read_parquet(parquet_file)

        expected_columns = ["VendorID", "trip_distance"]
        extra = set(df_read.columns) - set(expected_columns)

        assert "extra_column" in extra

    def test_matching_schema(self, test_data_dir):
        """Test that matching schema is detected correctly"""
        df = pd.DataFrame({"VendorID": ["1", "2"], "trip_distance": [2.5, 3.0]})
        parquet_file = test_data_dir / "matching_schema.parquet"
        df.to_parquet(parquet_file, index=False)

        df_read = pd.read_parquet(parquet_file)

        expected_columns = ["VendorID", "trip_distance"]

        assert set(df_read.columns) == set(expected_columns)
