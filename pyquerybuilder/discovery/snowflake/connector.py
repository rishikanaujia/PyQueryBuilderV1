# pyquerybuilder/discovery/snowflake/connector.py
"""Connector for Snowflake database interaction."""
import snowflake.connector
from typing import Dict, List, Any, Optional


class SnowflakeConnector:
    """Manages connection to Snowflake and executes queries."""

    def __init__(self, account, user, password, warehouse,
                 database, schema=None):
        """Initialize Snowflake connection parameters."""
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema or "PUBLIC"
        self._connection = None

    def connect(self):
        """Establish connection to Snowflake."""
        if not self._connection:
            self._connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
        return self._connection

    def execute_query(self, sql, params=None):
        """Execute a SQL query and return results.

        Args:
            sql: SQL query string
            params: Optional parameters dictionary

        Returns:
            List of dictionaries with query results
        """
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # Execute query
            cursor.execute(sql, params or {})

            # Fetch column names
            column_names = [desc[0] for desc in cursor.description]

            # Fetch and process results
            results = []
            for row in cursor.fetchall():
                result = dict(zip(column_names, row))
                results.append(result)

            return results
        finally:
            cursor.close()