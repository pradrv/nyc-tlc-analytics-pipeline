"""
Pytest configuration and shared fixtures for all tests
"""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest


@pytest.fixture(scope="session")
def test_data_dir():
    """Create a temporary directory for test data"""
    temp_dir = tempfile.mkdtemp(prefix="test_nyc_taxi_")
    yield Path(temp_dir)
    # Cleanup after all tests
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_db_path(test_data_dir):
    """Create a temporary test database"""
    db_path = test_data_dir / "test.duckdb"
    yield db_path
    # Cleanup after each test
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def test_db_connection(test_db_path):
    """Create a test database connection"""
    conn = duckdb.connect(str(test_db_path))
    yield conn
    conn.close()


@pytest.fixture
def sample_yellow_data():
    """Generate sample yellow taxi data"""
    return pd.DataFrame(
        {
            "VendorID": ["1", "2", "1", "2"],
            "tpep_pickup_datetime": [
                datetime(2024, 6, 1, 10, 0, 0),
                datetime(2024, 6, 1, 11, 0, 0),
                datetime(2024, 6, 1, 12, 0, 0),
                datetime(2024, 6, 1, 13, 0, 0),
            ],
            "tpep_dropoff_datetime": [
                datetime(2024, 6, 1, 10, 15, 0),
                datetime(2024, 6, 1, 11, 20, 0),
                datetime(2024, 6, 1, 12, 30, 0),
                datetime(2024, 6, 1, 13, 10, 0),
            ],
            "passenger_count": [1.0, 2.0, 1.0, 3.0],
            "trip_distance": [2.5, 5.0, 3.2, 1.8],
            "RatecodeID": [1.0, 1.0, 1.0, 1.0],
            "store_and_fwd_flag": ["N", "N", "N", "N"],
            "PULocationID": [161, 237, 142, 236],
            "DOLocationID": [237, 142, 236, 161],
            "payment_type": [1.0, 1.0, 2.0, 1.0],
            "fare_amount": [12.5, 25.0, 16.0, 9.5],
            "extra": [0.5, 0.5, 0.5, 0.5],
            "mta_tax": [0.5, 0.5, 0.5, 0.5],
            "tip_amount": [2.5, 5.0, 0.0, 2.0],
            "tolls_amount": [0.0, 0.0, 5.76, 0.0],
            "improvement_surcharge": [0.3, 0.3, 0.3, 0.3],
            "total_amount": [16.3, 31.3, 23.06, 12.8],
            "congestion_surcharge": [2.5, 2.5, 2.5, 2.5],
            "Airport_fee": [0.0, 0.0, 0.0, 0.0],
        }
    )


@pytest.fixture
def sample_green_data():
    """Generate sample green taxi data"""
    return pd.DataFrame(
        {
            "VendorID": ["1", "2", "1"],
            "lpep_pickup_datetime": [
                datetime(2024, 6, 1, 10, 0, 0),
                datetime(2024, 6, 1, 11, 0, 0),
                datetime(2024, 6, 1, 12, 0, 0),
            ],
            "lpep_dropoff_datetime": [
                datetime(2024, 6, 1, 10, 20, 0),
                datetime(2024, 6, 1, 11, 25, 0),
                datetime(2024, 6, 1, 12, 35, 0),
            ],
            "store_and_fwd_flag": ["N", "N", "N"],
            "RatecodeID": [1.0, 1.0, 1.0],
            "PULocationID": [74, 75, 41],
            "DOLocationID": [75, 41, 74],
            "passenger_count": [1.0, 2.0, 1.0],
            "trip_distance": [3.0, 4.5, 2.8],
            "fare_amount": [15.0, 22.5, 14.0],
            "extra": [0.5, 0.5, 0.5],
            "mta_tax": [0.5, 0.5, 0.5],
            "tip_amount": [3.0, 4.5, 0.0],
            "tolls_amount": [0.0, 0.0, 5.76],
            "ehail_fee": [None, None, None],
            "improvement_surcharge": [0.3, 0.3, 0.3],
            "total_amount": [19.3, 28.3, 21.06],
            "payment_type": [1.0, 1.0, 2.0],
            "trip_type": [1.0, 1.0, 1.0],
            "congestion_surcharge": [2.5, 2.5, 2.5],
        }
    )


@pytest.fixture
def sample_hvfhv_data():
    """Generate sample HVFHV data"""
    return pd.DataFrame(
        {
            "hvfhs_license_num": ["HV0003", "HV0005", "HV0003"],
            "dispatching_base_num": ["B02764", "B02510", "B02764"],
            "originating_base_num": ["B02764", "B02510", "B02764"],
            "request_datetime": [
                datetime(2024, 6, 1, 10, 0, 0),
                datetime(2024, 6, 1, 11, 0, 0),
                datetime(2024, 6, 1, 12, 0, 0),
            ],
            "on_scene_datetime": [
                datetime(2024, 6, 1, 10, 5, 0),
                datetime(2024, 6, 1, 11, 3, 0),
                datetime(2024, 6, 1, 12, 4, 0),
            ],
            "pickup_datetime": [
                datetime(2024, 6, 1, 10, 10, 0),
                datetime(2024, 6, 1, 11, 8, 0),
                datetime(2024, 6, 1, 12, 8, 0),
            ],
            "dropoff_datetime": [
                datetime(2024, 6, 1, 10, 25, 0),
                datetime(2024, 6, 1, 11, 30, 0),
                datetime(2024, 6, 1, 12, 40, 0),
            ],
            "PULocationID": [161, 237, 142],
            "DOLocationID": [237, 142, 236],
            "trip_miles": [2.8, 5.2, 3.5],
            "trip_time": [900, 1320, 1920],  # seconds
            "base_passenger_fare": [15.5, 28.0, 20.0],
            "tolls": [0.0, 0.0, 5.76],
            "bcf": [0.47, 0.84, 0.60],
            "sales_tax": [1.37, 2.48, 1.77],
            "congestion_surcharge": [2.75, 2.75, 2.75],
            "airport_fee": [0.0, 0.0, 0.0],
            "tips": [3.0, 5.5, 0.0],
            "driver_pay": [12.5, 22.0, 16.0],
            "shared_request_flag": ["N", "N", "Y"],
            "shared_match_flag": ["N", "N", "N"],
            "access_a_ride_flag": [" ", " ", " "],
            "wav_request_flag": ["N", "N", "N"],
            "wav_match_flag": ["N", "N", "N"],
        }
    )


@pytest.fixture
def sample_taxi_zones():
    """Generate sample taxi zone data"""
    return pd.DataFrame(
        {
            "LocationID": [1, 4, 13, 41, 74, 75, 142, 161, 236, 237, 264, 265],
            "Borough": [
                "EWR",
                "Manhattan",
                "Manhattan",
                "Queens",
                "Queens",
                "Queens",
                "Manhattan",
                "Manhattan",
                "Manhattan",
                "Manhattan",
                "Unknown",
                "Unknown",
            ],
            "Zone": [
                "Newark Airport",
                "Alphabet City",
                "Battery Park",
                "Flushing",
                "Corona",
                "East Elmhurst",
                "Lincoln Square East",
                "Midtown Center",
                "Upper East Side South",
                "Upper West Side South",
                "NV",
                "NV",
            ],
            "service_zone": [
                "EWR",
                "Yellow Zone",
                "Yellow Zone",
                "Boro Zone",
                "Boro Zone",
                "Boro Zone",
                "Yellow Zone",
                "Yellow Zone",
                "Yellow Zone",
                "Yellow Zone",
                "N/A",
                "N/A",
            ],
        }
    )
