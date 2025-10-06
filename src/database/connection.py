"""DuckDB connection management"""

from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

from src.config import config


class DatabaseConnection:
    """Manage DuckDB database connection"""

    _instance: Optional[duckdb.DuckDBPyConnection] = None
    _db_path: Optional[Path] = None

    @classmethod
    def get_connection(
        cls, db_path: Path = None, read_only: bool = False
    ) -> duckdb.DuckDBPyConnection:
        """
        Get or create database connection (singleton pattern)

        Args:
            db_path: Path to database file (default from config)
            read_only: Open in read-only mode

        Returns:
            DuckDB connection
        """
        if db_path is None:
            db_path = config.database_path

        # Create new connection if needed
        if cls._instance is None or cls._db_path != db_path:
            if cls._instance:
                cls._instance.close()

            # Ensure database directory exists
            db_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"🔌 Connecting to database: {db_path}")
            cls._instance = duckdb.connect(str(db_path), read_only=read_only)
            cls._db_path = db_path

            # Configure DuckDB for performance
            cls._instance.execute("SET memory_limit='4GB'")
            cls._instance.execute("SET threads TO 4")
            cls._instance.execute("SET preserve_insertion_order=false")

        return cls._instance

    @classmethod
    def close(cls):
        """Close database connection"""
        if cls._instance:
            logger.info("🔌 Closing database connection")
            cls._instance.close()
            cls._instance = None
            cls._db_path = None

    @classmethod
    @contextmanager
    def transaction(cls):
        """Context manager for transactions"""
        conn = cls.get_connection()
        try:
            conn.execute("BEGIN TRANSACTION")
            yield conn
            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Transaction rolled back: {e}")
            raise

    @classmethod
    def execute_sql_file(cls, sql_file: Path):
        """
        Execute SQL from file

        Args:
            sql_file: Path to SQL file
        """
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        logger.info(f"📜 Executing SQL file: {sql_file.name}")

        with open(sql_file, "r") as f:
            sql_content = f.read()

        conn = cls.get_connection()

        # Remove block comments /* ... */
        import re

        sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)

        # Split by semicolon and execute each statement
        statements = [s.strip() for s in sql_content.split(";") if s.strip()]

        executed = 0
        for i, statement in enumerate(statements, 1):
            # Skip comments and empty lines
            lines = [
                line
                for line in statement.split("\n")
                if line.strip() and not line.strip().startswith("--")
            ]
            clean_statement = "\n".join(lines)

            if not clean_statement:
                continue

            try:
                conn.execute(clean_statement)
                executed += 1
            except Exception as e:
                logger.error(f"Error executing statement {i} from {sql_file.name}: {e}")
                logger.error(f"Statement: {clean_statement[:200]}...")
                raise

        logger.success(f" Executed {executed} statements from {sql_file.name}")

    @classmethod
    def table_exists(cls, table_name: str) -> bool:
        """
        Check if table exists

        Args:
            table_name: Name of table

        Returns:
            True if table exists
        """
        conn = cls.get_connection()
        result = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()

        return result[0] > 0

    @classmethod
    def get_table_row_count(cls, table_name: str) -> int:
        """
        Get row count for table

        Args:
            table_name: Name of table

        Returns:
            Number of rows
        """
        if not cls.table_exists(table_name):
            return 0

        conn = cls.get_connection()
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return result[0]

    @classmethod
    def get_database_stats(cls) -> dict:
        """
        Get database statistics

        Returns:
            Dict with database stats
        """
        conn = cls.get_connection()

        # Get all tables
        tables_result = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()

        tables = [t[0] for t in tables_result]

        stats = {
            "database_path": cls._db_path,
            "table_count": len(tables),
            "tables": {},
        }

        # Get row count for each table
        for table in tables:
            try:
                row_count = cls.get_table_row_count(table)
                stats["tables"][table] = {"row_count": row_count}
            except Exception:
                stats["tables"][table] = {"row_count": "N/A"}

        # Get database file size
        if cls._db_path and cls._db_path.exists():
            from src.utils import format_bytes

            stats["database_size"] = format_bytes(cls._db_path.stat().st_size)

        return stats
