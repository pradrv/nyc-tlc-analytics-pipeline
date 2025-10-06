"""
Tests for database connection module
"""

import duckdb


class TestDatabaseConnection:
    """Tests for DatabaseConnection class"""

    def test_connection_created(self, test_db_connection):
        """Test that connection is created successfully"""
        assert test_db_connection is not None
        assert isinstance(test_db_connection, duckdb.DuckDBPyConnection)

    def test_execute_simple_query(self, test_db_connection):
        """Test executing a simple query"""
        result = test_db_connection.execute("SELECT 1 as test").fetchone()
        assert result[0] == 1

    def test_create_table(self, test_db_connection):
        """Test creating a table"""
        test_db_connection.execute("""
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR
            )
        """)

        # Verify table exists
        tables = test_db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
        ).fetchall()

        assert (
            len(tables) > 0
            or test_db_connection.execute("SELECT COUNT(*) FROM test_table").fetchone()[
                0
            ]
            == 0
        )

    def test_insert_and_select(self, test_db_connection):
        """Test inserting and selecting data"""
        test_db_connection.execute("""
            CREATE TABLE test_insert (
                id INTEGER,
                value VARCHAR
            )
        """)

        test_db_connection.execute("""
            INSERT INTO test_insert VALUES (1, 'test1'), (2, 'test2')
        """)

        result = test_db_connection.execute(
            "SELECT COUNT(*) FROM test_insert"
        ).fetchone()

        assert result[0] == 2

    def test_transaction_rollback(self, test_db_connection):
        """Test transaction rollback"""
        test_db_connection.execute("""
            CREATE TABLE test_transaction (
                id INTEGER
            )
        """)

        test_db_connection.execute("BEGIN TRANSACTION")
        test_db_connection.execute("INSERT INTO test_transaction VALUES (1)")
        test_db_connection.execute("ROLLBACK")

        result = test_db_connection.execute(
            "SELECT COUNT(*) FROM test_transaction"
        ).fetchone()

        assert result[0] == 0

    def test_execute_with_parameters(self, test_db_connection):
        """Test executing query with parameters"""
        test_db_connection.execute("""
            CREATE TABLE test_params (
                id INTEGER,
                name VARCHAR
            )
        """)

        test_db_connection.execute("INSERT INTO test_params VALUES (?, ?)", [1, "test"])

        result = test_db_connection.execute(
            "SELECT name FROM test_params WHERE id = ?", [1]
        ).fetchone()

        assert result[0] == "test"

    def test_memory_limit_setting(self, test_db_connection):
        """Test setting memory limit"""
        test_db_connection.execute("SET memory_limit='1GB'")

        # Query should succeed (setting was applied)
        result = test_db_connection.execute("SELECT 1").fetchone()
        assert result[0] == 1

    def test_multiple_queries(self, test_db_connection):
        """Test executing multiple queries"""
        test_db_connection.execute("CREATE TABLE multi_test (id INTEGER)")
        test_db_connection.execute("INSERT INTO multi_test VALUES (1)")
        test_db_connection.execute("INSERT INTO multi_test VALUES (2)")

        result = test_db_connection.execute("SELECT SUM(id) FROM multi_test").fetchone()

        assert result[0] == 3
