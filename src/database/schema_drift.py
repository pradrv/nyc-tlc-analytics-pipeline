"""Schema drift detection and handling"""

from pathlib import Path
from typing import Dict, Set, Tuple

import pyarrow.parquet as pq
from loguru import logger

from src.database.connection import DatabaseConnection


class SchemaDriftHandler:
    """Handle schema evolution and drift"""

    # Map Parquet types to DuckDB types
    PARQUET_TO_DUCKDB_TYPE_MAP = {
        "int32": "INTEGER",
        "int64": "INTEGER",
        "double": "DOUBLE",
        "float": "DOUBLE",
        "timestamp[us]": "TIMESTAMP",
        "timestamp[ms]": "TIMESTAMP",
        "timestamp[ns]": "TIMESTAMP",
        "large_string": "VARCHAR",
        "string": "VARCHAR",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
    }

    @staticmethod
    def get_parquet_schema(file_path: Path) -> Dict[str, str]:
        """
        Get schema from Parquet file

        Returns:
            Dict mapping column name to DuckDB type
        """
        table = pq.read_schema(file_path)
        schema = {}

        for field in table:
            parquet_type = str(field.type)
            duckdb_type = SchemaDriftHandler.PARQUET_TO_DUCKDB_TYPE_MAP.get(
                parquet_type, "VARCHAR"
            )
            schema[field.name] = duckdb_type

        return schema

    @staticmethod
    def get_table_schema(table_name: str) -> Dict[str, str]:
        """
        Get current schema from DuckDB table

        Returns:
            Dict mapping column name to type (excluding metadata columns)
        """
        conn = DatabaseConnection.get_connection()

        result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()

        # Exclude metadata columns
        metadata_cols = {"source_file", "ingestion_timestamp", "load_timestamp"}

        schema = {}
        for row in result:
            col_name = row[1]
            col_type = row[2]
            if col_name not in metadata_cols:
                schema[col_name] = col_type

        return schema

    @staticmethod
    def detect_schema_drift(
        table_name: str, parquet_file: Path
    ) -> Tuple[Set[str], Set[str], Dict[str, Tuple[str, str]]]:
        """
        Detect schema differences between table and Parquet file

        Returns:
            (new_columns, removed_columns, type_changes)
            - new_columns: Columns in Parquet but not in table
            - removed_columns: Columns in table but not in Parquet
            - type_changes: {col: (old_type, new_type)}
        """
        table_schema = SchemaDriftHandler.get_table_schema(table_name)
        parquet_schema = SchemaDriftHandler.get_parquet_schema(parquet_file)

        table_cols = set(table_schema.keys())
        parquet_cols = set(parquet_schema.keys())

        new_columns = parquet_cols - table_cols
        removed_columns = table_cols - parquet_cols

        # Check for type changes in common columns
        type_changes = {}
        common_cols = table_cols & parquet_cols
        for col in common_cols:
            table_type = table_schema[col].upper()
            parquet_type = parquet_schema[col].upper()
            # Normalize types for comparison
            if table_type != parquet_type and not (
                table_type in ["INTEGER", "DOUBLE"]
                and parquet_type in ["INTEGER", "DOUBLE"]
            ):
                type_changes[col] = (table_type, parquet_type)

        return new_columns, removed_columns, type_changes

    @staticmethod
    def handle_schema_drift(
        table_name: str, parquet_file: Path, auto_fix: bool = True
    ) -> bool:
        """
        Detect and optionally fix schema drift

        Args:
            table_name: Name of the table
            parquet_file: Path to Parquet file
            auto_fix: If True, automatically add missing columns

        Returns:
            True if schema is compatible (after fixes if auto_fix=True)
        """
        new_cols, removed_cols, type_changes = SchemaDriftHandler.detect_schema_drift(
            table_name, parquet_file
        )

        if not new_cols and not removed_cols and not type_changes:
            return True  # No drift

        # Log drift detection
        if new_cols:
            logger.warning(
                f"Schema drift in {table_name}: {len(new_cols)} new columns detected: {new_cols}"
            )
        if removed_cols:
            logger.warning(
                f"Schema drift in {table_name}: {len(removed_cols)} columns removed: {removed_cols}"
            )
        if type_changes:
            logger.warning(
                f"Schema drift in {table_name}: {len(type_changes)} type changes: {type_changes}"
            )

        # Handle new columns
        if new_cols and auto_fix:
            parquet_schema = SchemaDriftHandler.get_parquet_schema(parquet_file)
            conn = DatabaseConnection.get_connection()

            for col in new_cols:
                col_type = parquet_schema[col]
                try:
                    sql = f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}"
                    conn.execute(sql)
                    logger.success(f"✓ Added column {col} ({col_type}) to {table_name}")
                except Exception as e:
                    logger.error(f"Failed to add column {col}: {e}")
                    return False

        # Removed columns are OK - we just won't load them
        # Type changes need manual intervention
        if type_changes and not auto_fix:
            logger.error(
                f"Type changes detected in {table_name} - manual intervention required"
            )
            return False

        return True

    @staticmethod
    def load_with_schema_handling(
        table_name: str, parquet_file: Path, source_file_name: str
    ) -> int:
        """
        Load Parquet file with automatic schema drift handling

        Returns:
            Number of rows loaded
        """
        # First, handle any schema drift
        compatible = SchemaDriftHandler.handle_schema_drift(
            table_name, parquet_file, auto_fix=True
        )

        if not compatible:
            raise ValueError(
                f"Schema incompatible between {table_name} and {parquet_file}"
            )

        # Get table schema (after any fixes)
        table_schema = SchemaDriftHandler.get_table_schema(table_name)
        table_cols = list(table_schema.keys())

        # Get Parquet columns
        parquet_schema = SchemaDriftHandler.get_parquet_schema(parquet_file)
        parquet_cols = list(parquet_schema.keys())

        # Build column mapping (only use columns that exist in both)
        common_cols = [col for col in parquet_cols if col in table_cols]

        conn = DatabaseConnection.get_connection()

        # Build INSERT statement with explicit column selection
        columns_str = ", ".join(common_cols + ["source_file"])
        select_cols = ", ".join([f'"{col}"' for col in common_cols])

        sql = f"""
        INSERT INTO {table_name} ({columns_str})
        SELECT {select_cols}, '{source_file_name}' as source_file
        FROM read_parquet('{parquet_file}')
        WHERE '{source_file_name}' NOT IN (
            SELECT DISTINCT source_file FROM {table_name}
        )
        """

        result = conn.execute(sql)
        row_count = result.fetchone()[0] if result else 0

        return row_count
