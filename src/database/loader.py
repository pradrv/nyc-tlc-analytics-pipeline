"""Load data from Parquet files to DuckDB"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List

from loguru import logger

from src.config import config
from src.database.connection import DatabaseConnection
from src.database.schema_drift import SchemaDriftHandler
from src.ingestion.validators import FileValidator


class DataLoader:
    """Load parquet files into DuckDB raw tables"""

    EXPECTED_SCHEMAS = {
        "yellow": [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "Airport_fee",
        ],
        "green": [
            "VendorID",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "store_and_fwd_flag",
            "RatecodeID",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "ehail_fee",
            "improvement_surcharge",
            "total_amount",
            "payment_type",
            "trip_type",
            "congestion_surcharge",
        ],
        "hvfhv": [
            "hvfhs_license_num",
            "dispatching_base_num",
            "originating_base_num",
            "request_datetime",
            "on_scene_datetime",
            "pickup_datetime",
            "dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_miles",
            "trip_time",
            "base_passenger_fare",
            "tolls",
            "bcf",
            "sales_tax",
            "congestion_surcharge",
            "airport_fee",
            "tips",
            "driver_pay",
            "shared_request_flag",
            "shared_match_flag",
            "access_a_ride_flag",
            "wav_request_flag",
            "wav_match_flag",
        ],
    }

    @staticmethod
    def load_parquet_to_raw(
        file_path: Path, service_type: str, validate: bool = True
    ) -> Dict:
        """
        Load parquet file into raw table

        Args:
            file_path: Path to parquet file
            service_type: Service type (yellow, green, hvfhv)
            validate: Run validation before loading

        Returns:
            Dict with load results
        """
        start_time = datetime.now()

        if not file_path.exists():
            logger.error(f" File not found: {file_path}")
            return {
                "file_path": file_path,
                "service_type": service_type,
                "status": "failed",
                "error": "File not found",
            }

        # Validate file
        if validate:
            validation_result = FileValidator.validate_parquet(file_path)
            if not validation_result["is_valid"]:
                logger.error(f" Validation failed: {file_path.name}")
                return {
                    "file_path": file_path,
                    "service_type": service_type,
                    "status": "failed",
                    "error": validation_result["error"],
                }

            # Check schema drift
            expected_columns = DataLoader.EXPECTED_SCHEMAS.get(service_type, [])
            if expected_columns:
                drift_check = FileValidator.check_schema_drift(
                    validation_result["column_names"], expected_columns
                )
                if drift_check["has_drift"]:
                    logger.warning(f"  Schema drift detected in {file_path.name}")

        # Get table name
        service_config = config.get_service_config(service_type)
        raw_table = service_config.get("raw_table", f"raw_{service_type}")

        try:
            logger.info(f" Loading {file_path.name} → {raw_table}")

            conn = DatabaseConnection.get_connection()

            # Check if data already loaded (idempotent check)
            existing_count = conn.execute(f"""
                SELECT COUNT(*) 
                FROM {raw_table}
                WHERE source_file = '{file_path.name}'
            """).fetchone()[0]

            if existing_count > 0:
                logger.info(
                    f"  Data already loaded from {file_path.name} ({existing_count:,} rows)"
                )
                return {
                    "file_path": file_path,
                    "service_type": service_type,
                    "table": raw_table,
                    "rows_inserted": 0,
                    "rows_existing": existing_count,
                    "status": "skipped",
                    "load_time": 0,
                }

            # Load data with automatic schema drift handling
            rows_inserted = SchemaDriftHandler.load_with_schema_handling(
                raw_table, file_path, file_path.name
            )

            load_time = (datetime.now() - start_time).total_seconds()

            logger.success(
                f" Loaded {file_path.name}: {rows_inserted:,} rows in {load_time:.2f}s "
                f"({rows_inserted / max(load_time, 0.001):.0f} rows/sec)"
            )

            return {
                "file_path": file_path,
                "service_type": service_type,
                "table": raw_table,
                "rows_inserted": rows_inserted,
                "rows_existing": 0,
                "status": "success",
                "load_time": load_time,
            }

        except Exception as e:
            logger.error(f" Failed to load {file_path.name}: {e}")
            return {
                "file_path": file_path,
                "service_type": service_type,
                "table": raw_table,
                "status": "failed",
                "error": str(e),
            }

    @staticmethod
    def load_month(service_type: str, year: int, month: int) -> Dict:
        """
        Load specific month of data

        Args:
            service_type: Service type
            year: Year
            month: Month

        Returns:
            Load result dict
        """
        file_path = config.get_file_path(service_type, year, month)
        return DataLoader.load_parquet_to_raw(file_path, service_type)

    @staticmethod
    def load_all_downloaded_files(service_types: List[str] = None) -> List[Dict]:
        """
        Load all downloaded parquet files

        Args:
            service_types: List of service types to load (default: all)

        Returns:
            List of load results
        """
        if service_types is None:
            service_types = config.services

        results = []

        for service_type in service_types:
            service_config = config.get_service_config(service_type)
            filename_pattern = service_config.get("filename_pattern", "")

            # Find all parquet files for this service
            pattern = filename_pattern.replace("{year:04d}", "*").replace(
                "{month:02d}", "*"
            )
            files = list(config.raw_data_dir.glob(pattern))

            logger.info(f" Found {len(files)} files for {service_type}")

            for file_path in sorted(files):
                result = DataLoader.load_parquet_to_raw(file_path, service_type)
                results.append(result)

        # Summary
        successful = sum(1 for r in results if r["status"] == "success")
        skipped = sum(1 for r in results if r["status"] == "skipped")
        failed = sum(1 for r in results if r["status"] == "failed")
        total_rows = sum(r.get("rows_inserted", 0) for r in results)

        logger.info("=" * 70)
        logger.info(" Load Summary:")
        logger.info(f"   Total files: {len(results)}")
        logger.info(f"   Loaded: {successful}")
        logger.info(f"   Skipped: {skipped}")
        logger.info(f"   Failed: {failed}")
        logger.info(f"   Total rows: {total_rows:,}")
        logger.info("=" * 70)

        return results

    @staticmethod
    def log_ingestion_metadata(download_result: Dict, validation_result: Dict = None):
        """
        Log ingestion metadata to ingestion_log table

        Args:
            download_result: Result from downloader
            validation_result: Result from validator (optional)
        """
        conn = DatabaseConnection.get_connection()

        # Extract values
        service_type = download_result.get("service_type")
        year = download_result.get("year")
        month = download_result.get("month")
        source_file = (
            Path(download_result["file_path"]).name
            if download_result.get("file_path")
            else None
        )
        file_url = download_result.get("url")
        file_path = str(download_result.get("file_path"))
        file_size = download_result.get("file_size", 0)
        checksum = download_result.get("checksum")
        status = download_result.get("status")
        error_message = download_result.get("error")
        download_timestamp = datetime.now()

        # From validation
        row_count = validation_result.get("row_count", 0) if validation_result else 0
        column_count = (
            validation_result.get("column_count", 0) if validation_result else 0
        )
        column_names = (
            str(validation_result.get("column_names", []))
            if validation_result
            else None
        )

        # Insert log entry
        conn.execute(
            """
            INSERT INTO ingestion_log (
                log_id, service_type, year, month, source_file, file_url, file_path,
                file_size_bytes, file_checksum, row_count, column_count, column_names,
                download_timestamp, load_timestamp, status, error_message
            ) VALUES (
                nextval('ingestion_log_seq'), ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?
            )
        """,
            [
                service_type,
                year,
                month,
                source_file,
                file_url,
                file_path,
                file_size,
                checksum,
                row_count,
                column_count,
                column_names,
                download_timestamp,
                download_timestamp,
                status,
                error_message,
            ],
        )
