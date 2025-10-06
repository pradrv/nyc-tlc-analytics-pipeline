"""Database schema initialization"""

from pathlib import Path

from loguru import logger

from src.config import config
from src.database.connection import DatabaseConnection


class SchemaManager:
    """Manage database schema creation and updates"""

    @staticmethod
    def initialize_database():
        """Initialize database with all DDL files"""
        logger.info("  Initializing database schema...")

        sql_ddl_dir = config.sql_dir / "ddl"

        if not sql_ddl_dir.exists():
            raise FileNotFoundError(f"DDL directory not found: {sql_ddl_dir}")

        # Execute DDL files in order (skip reference documentation)
        ddl_files = sorted(sql_ddl_dir.glob("*.sql"))
        executed_count = 0

        for ddl_file in ddl_files:
            # Skip schema reference file (documentation only)
            if ddl_file.name == "00_schema_reference.sql":
                logger.info(f"  Skipping documentation file: {ddl_file.name}")
                continue

            try:
                DatabaseConnection.execute_sql_file(ddl_file)
                executed_count += 1
            except Exception as e:
                logger.error(f"Failed to execute {ddl_file.name}: {e}")
                raise

        logger.success(f" Database schema initialized ({executed_count} DDL files)")

    @staticmethod
    def load_taxi_zones(csv_path: Path = None):
        """
        Load taxi zone lookup data

        Args:
            csv_path: Path to taxi zone CSV (default: data/raw/taxi_zone_lookup.csv)
        """
        if csv_path is None:
            csv_path = config.raw_data_dir / "taxi_zone_lookup.csv"

        if not csv_path.exists():
            logger.warning(f"  Taxi zone CSV not found: {csv_path}")
            return

        logger.info(f"📍 Loading taxi zones from {csv_path.name}")

        conn = DatabaseConnection.get_connection()

        # Load into raw_taxi_zones
        conn.execute(f"""
            INSERT OR REPLACE INTO raw_taxi_zones
            SELECT * FROM read_csv_auto('{csv_path}')
        """)

        # Update dim_zones
        conn.execute("""
            INSERT OR REPLACE INTO dim_zones (location_id, borough, zone, service_zone, is_airport, is_manhattan)
            SELECT 
                LocationID,
                Borough,
                Zone,
                service_zone,
                CASE 
                    WHEN Zone LIKE '%Airport%' OR service_zone = 'Airports' THEN TRUE
                    ELSE FALSE
                END AS is_airport,
                CASE 
                    WHEN Borough = 'Manhattan' THEN TRUE
                    ELSE FALSE
                END AS is_manhattan
            FROM raw_taxi_zones
        """)

        zone_count = DatabaseConnection.get_table_row_count("dim_zones")
        logger.success(f" Loaded {zone_count} taxi zones")

    @staticmethod
    def verify_schema() -> bool:
        """
        Verify that all required tables exist

        Returns:
            True if schema is valid
        """
        required_tables = [
            "raw_yellow",
            "raw_green",
            "raw_hvfhv",
            "raw_taxi_zones",
            "fact_trips",
            "dim_zones",
            "dim_date",
            "dim_time",
            "dim_service",
            "agg_pricing_by_zone_hour",
            "agg_hvfhv_take_rates",
            "agg_market_share",
            "ingestion_log",
            "data_quality_metrics",
        ]

        missing_tables = []
        for table in required_tables:
            if not DatabaseConnection.table_exists(table):
                missing_tables.append(table)

        if missing_tables:
            logger.error(f" Missing tables: {missing_tables}")
            return False

        logger.success(f" Schema verification passed ({len(required_tables)} tables)")
        return True

    @staticmethod
    def get_schema_summary():
        """Print schema summary"""
        stats = DatabaseConnection.get_database_stats()

        logger.info("=" * 70)
        logger.info(" Database Schema Summary")
        logger.info("=" * 70)
        logger.info(f"Database: {stats['database_path']}")
        logger.info(f"Size: {stats.get('database_size', 'N/A')}")
        logger.info(f"Tables: {stats['table_count']}")
        logger.info("")
        logger.info("Table Row Counts:")

        for table, info in sorted(stats["tables"].items()):
            row_count = info["row_count"]
            if isinstance(row_count, int):
                logger.info(f"  {table:.<40} {row_count:>15,}")
            else:
                logger.info(f"  {table:.<40} {row_count:>15}")

        logger.info("=" * 70)
