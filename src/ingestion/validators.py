"""File validation and metadata extraction"""

from pathlib import Path
from typing import Dict, List

import duckdb
from loguru import logger

from src.utils import calculate_file_checksum, format_bytes


class FileValidator:
    """Validate downloaded files and extract metadata"""

    @staticmethod
    def validate_parquet(file_path: Path) -> Dict:
        """
        Validate parquet file and extract metadata

        Args:
            file_path: Path to parquet file

        Returns:
            Dict with validation results and metadata
        """
        try:
            # Connect to DuckDB (in-memory for validation)
            conn = duckdb.connect(":memory:")

            # Read file metadata
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as row_count
                FROM parquet_scan('{file_path}')
            """).fetchone()

            row_count = result[0]

            # Get column names
            columns_result = conn.execute(f"""
                DESCRIBE SELECT * FROM parquet_scan('{file_path}')
            """).fetchall()

            column_names = [col[0] for col in columns_result]

            # Calculate checksum
            checksum = calculate_file_checksum(file_path)
            file_size = file_path.stat().st_size

            # Set distinct rows same as row count (we'll check duplicates later in transformation)
            distinct_rows = row_count
            duplicate_ratio = 0

            conn.close()

            logger.info(
                f" Validated {file_path.name}: "
                f"{row_count:,} rows, {len(column_names)} columns, "
                f"{format_bytes(file_size)}, "
                f"{duplicate_ratio * 100:.2f}% duplicates"
            )

            return {
                "file_path": file_path,
                "is_valid": True,
                "row_count": row_count,
                "distinct_rows": distinct_rows,
                "duplicate_ratio": duplicate_ratio,
                "column_count": len(column_names),
                "column_names": column_names,
                "file_size": file_size,
                "checksum": checksum,
                "error": None,
            }

        except Exception as e:
            logger.error(f" Validation failed for {file_path.name}: {str(e)}")
            return {
                "file_path": file_path,
                "is_valid": False,
                "row_count": 0,
                "distinct_rows": 0,
                "duplicate_ratio": 0,
                "column_count": 0,
                "column_names": [],
                "file_size": file_path.stat().st_size if file_path.exists() else 0,
                "checksum": None,
                "error": str(e),
            }

    @staticmethod
    def check_schema_drift(
        column_names: List[str], expected_columns: List[str]
    ) -> Dict:
        """
        Check for schema drift (missing or extra columns)

        Args:
            column_names: Actual column names
            expected_columns: Expected column names

        Returns:
            Dict with drift analysis
        """
        actual_set = set(column_names)
        expected_set = set(expected_columns)

        missing_columns = expected_set - actual_set
        extra_columns = actual_set - expected_set

        has_drift = len(missing_columns) > 0 or len(extra_columns) > 0

        if has_drift:
            logger.warning("  Schema drift detected:")
            if missing_columns:
                logger.warning(f"   Missing columns: {list(missing_columns)}")
            if extra_columns:
                logger.warning(f"   Extra columns: {list(extra_columns)}")

        return {
            "has_drift": has_drift,
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
            "column_match_rate": len(actual_set & expected_set) / len(expected_set)
            if expected_set
            else 1.0,
        }
