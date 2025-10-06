"""Data quality checks for NYC Taxi trip data"""

from typing import Dict, List

from loguru import logger

from src.config import config
from src.database.connection import DatabaseConnection


class DataQualityChecker:
    """Validate trip data quality"""

    def __init__(self):
        self.quality_config = config.quality_checks

    def check_fares(self, table_name: str) -> Dict:
        """Check for valid fare amounts"""
        conn = DatabaseConnection.get_connection()

        # Get fare column name based on table
        if "yellow" in table_name or "green" in table_name:
            fare_col = "total_amount"
        else:  # hvfhv
            fare_col = "base_passenger_fare"

        result = conn.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN {fare_col} < 0 THEN 1 ELSE 0 END) as negative_fares,
                SUM(CASE WHEN {fare_col} > {self.quality_config["max_fare"]} THEN 1 ELSE 0 END) as excessive_fares,
                MIN({fare_col}) as min_fare,
                MAX({fare_col}) as max_fare,
                AVG({fare_col}) as avg_fare
            FROM {table_name}
        """).fetchone()

        total, negative, excessive, min_val, max_val, avg_val = result
        passed = total - negative - excessive

        logger.info(
            f"Fare check ({table_name}): {passed:,}/{total:,} passed ({passed / total * 100:.2f}%)"
        )

        return {
            "check_type": "fare_validation",
            "table": table_name,
            "total_rows": total,
            "passed_rows": passed,
            "failed_rows": negative + excessive,
            "failure_rate": (negative + excessive) / total if total > 0 else 0,
            "details": {
                "negative_fares": negative,
                "excessive_fares": excessive,
                "min_fare": min_val,
                "max_fare": max_val,
                "avg_fare": avg_val,
            },
        }

    def check_timestamps(self, table_name: str) -> Dict:
        """Check for valid timestamp ordering"""
        conn = DatabaseConnection.get_connection()

        # Get timestamp column names based on table
        if "yellow" in table_name:
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
        elif "green" in table_name:
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
        else:  # hvfhv
            pickup_col = "pickup_datetime"
            dropoff_col = "dropoff_datetime"

        result = conn.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN {dropoff_col} < {pickup_col} THEN 1 ELSE 0 END) as invalid_order,
                SUM(CASE WHEN {pickup_col} IS NULL OR {dropoff_col} IS NULL THEN 1 ELSE 0 END) as null_timestamps
            FROM {table_name}
        """).fetchone()

        total, invalid, nulls = result
        passed = total - invalid - nulls

        logger.info(
            f"Timestamp check ({table_name}): {passed:,}/{total:,} passed ({passed / total * 100:.2f}%)"
        )

        return {
            "check_type": "timestamp_validation",
            "table": table_name,
            "total_rows": total,
            "passed_rows": passed,
            "failed_rows": invalid + nulls,
            "failure_rate": (invalid + nulls) / total if total > 0 else 0,
            "details": {"invalid_order": invalid, "null_timestamps": nulls},
        }

    def check_realistic_speed(self, table_name: str) -> Dict:
        """Check for realistic trip speeds"""
        conn = DatabaseConnection.get_connection()

        # Get column names based on table
        if "yellow" in table_name:
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
            distance_col = "trip_distance"
        elif "green" in table_name:
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
            distance_col = "trip_distance"
        else:  # hvfhv
            pickup_col = "pickup_datetime"
            dropoff_col = "dropoff_datetime"
            distance_col = "trip_miles"

        max_speed = self.quality_config["max_speed_mph"]

        result = conn.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE 
                    WHEN {distance_col} > 0 
                    AND EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 0
                    AND ({distance_col} / (EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) / 3600.0)) > {max_speed}
                    THEN 1 ELSE 0 
                END) as excessive_speed
            FROM {table_name}
            WHERE {distance_col} > 0
        """).fetchone()

        total, excessive = result
        passed = total - excessive

        logger.info(
            f"Speed check ({table_name}): {passed:,}/{total:,} passed ({passed / total * 100:.2f}%)"
        )

        return {
            "check_type": "speed_validation",
            "table": table_name,
            "total_rows": total,
            "passed_rows": passed,
            "failed_rows": excessive,
            "failure_rate": excessive / total if total > 0 else 0,
            "details": {"excessive_speed": excessive, "max_speed_mph": max_speed},
        }

    def check_distance(self, table_name: str) -> Dict:
        """Check for valid trip distances"""
        conn = DatabaseConnection.get_connection()

        distance_col = "trip_distance" if "hvfhv" not in table_name else "trip_miles"
        max_dist = self.quality_config["max_trip_distance"]

        result = conn.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN {distance_col} < 0 THEN 1 ELSE 0 END) as negative_distance,
                SUM(CASE WHEN {distance_col} > {max_dist} THEN 1 ELSE 0 END) as excessive_distance,
                AVG({distance_col}) as avg_distance
            FROM {table_name}
        """).fetchone()

        total, negative, excessive, avg_dist = result
        passed = total - negative - excessive

        logger.info(
            f"Distance check ({table_name}): {passed:,}/{total:,} passed ({passed / total * 100:.2f}%)"
        )

        return {
            "check_type": "distance_validation",
            "table": table_name,
            "total_rows": total,
            "passed_rows": passed,
            "failed_rows": negative + excessive,
            "failure_rate": (negative + excessive) / total if total > 0 else 0,
            "details": {
                "negative_distance": negative,
                "excessive_distance": excessive,
                "avg_distance": avg_dist,
            },
        }

    def run_all_checks(self, table_name: str) -> List[Dict]:
        """Run all quality checks on a table"""
        logger.info(f" Running quality checks on {table_name}...")

        checks = [
            self.check_fares(table_name),
            self.check_timestamps(table_name),
            self.check_realistic_speed(table_name),
            self.check_distance(table_name),
        ]

        # Log to data_quality_metrics table (idempotent - delete existing records first)
        conn = DatabaseConnection.get_connection()

        # Delete existing quality check records for this table to make it idempotent
        service_type = table_name.replace("raw_", "")
        conn.execute(
            """
            DELETE FROM data_quality_metrics 
            WHERE service_type = ?
        """,
            [service_type],
        )

        # Insert new quality check records
        for check in checks:
            conn.execute(
                """
                INSERT INTO data_quality_metrics (
                    check_id, service_type, check_type, total_rows, 
                    passed_rows, failed_rows, failure_rate, details
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    f"{table_name}_{check['check_type']}_{check['total_rows']}",
                    service_type,
                    check["check_type"],
                    check["total_rows"],
                    check["passed_rows"],
                    check["failed_rows"],
                    check["failure_rate"],
                    str(check["details"]),
                ],
            )

        total_passed = sum(c["passed_rows"] for c in checks)
        total_rows = sum(c["total_rows"] for c in checks)

        logger.success(
            f" Quality checks complete: {total_passed:,}/{total_rows:,} passed ({total_passed / total_rows * 100:.2f}%)"
        )

        return checks
