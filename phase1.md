# Phase 1: Foundation and Core Components

Let's start building the PyQueryBuilder foundation and core components.

## Step 1: Project Setup

### Create Project Structure
```bash
mkdir -p pyquerybuilder/{core,schema,query,sql,utils,discovery}
touch pyquerybuilder/{core,schema,query,sql,utils,discovery}/__init__.py

mkdir -p pyquerybuilder/sql/{generators,functions,hints}
mkdir -p pyquerybuilder/discovery/snowflake
touch pyquerybuilder/__init__.py

```

### Create Setup Files
```python
# setup.py
from setuptools import setup, find_packages

setup(
    name="pyquerybuilder",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "snowflake-connector-python>=2.7.0",
    ],
    description="Fluent SQL query builder with schema discovery",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/pyquerybuilder",
)
```

### Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .  # Install the package in development mode
```

## Step 2: Core Query Builder

Let's implement the basic QueryBuilder class first:

```python
# pyquerybuilder/core/builder.py
"""Core query builder interface for PyQueryBuilder."""
from typing import Any, Dict, List, Optional, Tuple, Union


class QueryBuilder:
    """Main query builder with fluent interface for SQL queries."""

    def __init__(self, schema_registry=None, connector=None):
        """Initialize the QueryBuilder with schema information.

        Args:
            schema_registry: Registry containing schema metadata
            connector: Optional database connector for executing queries
        """
        self._schema_registry = schema_registry
        self._connector = connector

        # Query components
        self._select_fields = []
        self._from_table = None
        self._joins = []
        self._where_conditions = []
        self._order_by = []
        self._limit = None
        self._offset = None

    def select(self, *fields) -> 'QueryBuilder':
        """Add fields to the SELECT clause.

        Args:
            *fields: Field names or function objects to select

        Returns:
            Self for method chaining
        """
        for field in fields:
            self._select_fields.append(field)
        return self

    def from_table(self, table) -> 'QueryBuilder':
        """Set the table for the FROM clause.

        Args:
            table: Table name or subquery

        Returns:
            Self for method chaining
        """
        # Handle table with alias
        if isinstance(table, str) and " as " in table.lower():
            parts = table.lower().split(" as ")
            self._from_table = {
                "table": parts[0].strip(),
                "alias": parts[1].strip()
            }
        elif isinstance(table, str) and " AS " in table:
            parts = table.split(" AS ")
            self._from_table = {
                "table": parts[0].strip(),
                "alias": parts[1].strip()
            }
        else:
            self._from_table = {"table": table}
        return self

    def join(self, table, condition=None, join_type="INNER") -> 'QueryBuilder':
        """Add a JOIN clause to the query.

        Args:
            table: Table name or subquery
            condition: Join condition (optional if can be auto-resolved)
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)

        Returns:
            Self for method chaining
        """
        # Handle table with alias
        table_info = {}
        if isinstance(table, str) and " as " in table.lower():
            parts = table.lower().split(" as ")
            table_info["table"] = parts[0].strip()
            table_info["alias"] = parts[1].strip()
        elif isinstance(table, str) and " AS " in table:
            parts = table.split(" AS ")
            table_info["table"] = parts[0].strip()
            table_info["alias"] = parts[1].strip()
        else:
            table_info["table"] = table

        # Add join condition and type
        if condition:
            table_info["condition"] = condition
        table_info["type"] = join_type.upper()

        self._joins.append(table_info)
        return self

    def where(self, field, operator=None, value=None) -> 'QueryBuilder':
        """Add a WHERE condition to the query.

        This method can be called in two ways:
        1. where(field, operator, value)
        2. where(field, value) - with implied "=" operator

        Args:
            field: Field name or function
            operator: Comparison operator or value (if value is None)
            value: Value to compare against

        Returns:
            Self for method chaining
        """
        if value is None and operator is not None:
            # Shift parameters for convenience
            value = operator
            operator = "="

        self._where_conditions.append({
            "field": field,
            "operator": operator,
            "value": value
        })
        return self

    def order_by(self, field, direction: str = "asc") -> 'QueryBuilder':
        """Add a field to the ORDER BY clause.

        Args:
            field: Field or function to order by
            direction: Sort direction (asc or desc)

        Returns:
            Self for method chaining
        """
        self._order_by.append({
            "field": field,
            "direction": direction
        })
        return self

    def limit(self, limit: int) -> 'QueryBuilder':
        """Set the LIMIT clause.

        Args:
            limit: Maximum number of rows

        Returns:
            Self for method chaining
        """
        self._limit = limit
        return self

    def offset(self, offset: int) -> 'QueryBuilder':
        """Set the OFFSET clause.

        Args:
            offset: Number of rows to skip

        Returns:
            Self for method chaining
        """
        self._offset = offset
        return self

    def build(self) -> Tuple[str, Dict[str, Any]]:
        """Build the SQL query and parameter dictionary.

        Returns:
            Tuple of (sql_string, parameters)
        """
        # Simple implementation for now
        sql_parts = []
        params = {}
        param_idx = 0

        # SELECT clause
        if self._select_fields:
            select_clause = "SELECT " + ", ".join(str(field) for field in self._select_fields)
        else:
            select_clause = "SELECT *"
        sql_parts.append(select_clause)

        # FROM clause
        if self._from_table:
            table = self._from_table["table"]
            alias = self._from_table.get("alias")
            if alias:
                from_clause = f"FROM {table} AS {alias}"
            else:
                from_clause = f"FROM {table}"
            sql_parts.append(from_clause)

        # JOIN clauses
        for join in self._joins:
            table = join["table"]
            alias = join.get("alias")
            condition = join.get("condition")
            join_type = join.get("type", "INNER")

            if alias:
                join_clause = f"{join_type} JOIN {table} AS {alias}"
            else:
                join_clause = f"{join_type} JOIN {table}"

            if condition:
                join_clause += f" ON {condition}"

            sql_parts.append(join_clause)

        # WHERE clause
        if self._where_conditions:
            where_parts = []
            for condition in self._where_conditions:
                field = condition["field"]
                operator = condition["operator"]
                value = condition["value"]

                # Create parameter name
                param_name = f"p{param_idx}"
                param_idx += 1

                where_parts.append(f"{field} {operator} :{param_name}")
                params[param_name] = value

            where_clause = "WHERE " + " AND ".join(where_parts)
            sql_parts.append(where_clause)

        # ORDER BY clause
        if self._order_by:
            order_parts = []
            for order_spec in self._order_by:
                field = order_spec["field"]
                direction = order_spec["direction"].upper()
                order_parts.append(f"{field} {direction}")
            order_clause = "ORDER BY " + ", ".join(order_parts)
            sql_parts.append(order_clause)

        # LIMIT and OFFSET
        if self._limit is not None:
            sql_parts.append(f"LIMIT {self._limit}")
        if self._offset is not None:
            sql_parts.append(f"OFFSET {self._offset}")

        # Combine all parts
        sql = " ".join(sql_parts)
        return sql, params

    def execute(self):
        """Execute the query and return results.

        Returns:
            List of dictionaries with query results
        """
        if not self._connector:
            raise ValueError("No database connector provided for query execution")

        sql, params = self.build()
        return self._connector.execute_query(sql, params)
```

Now let's create a minimal SQL generator module:

```python
# pyquerybuilder/sql/generator.py
"""Generator for producing SQL from analyzed queries."""
from typing import Dict, List, Tuple, Any


class SQLGenerator:
    """Generates SQL queries from analyzed components."""

    def __init__(self, dialect="snowflake"):
        """Initialize with SQL dialect."""
        self.dialect = dialect

    def generate(self, analyzed_query):
        """Generate SQL from analyzed query components.

        Args:
            analyzed_query: Dictionary of analyzed components

        Returns:
            Tuple of (sql_string, parameters)
        """
        params = {}
        sql_parts = []

        # Generate SELECT clause
        select_fields = analyzed_query.get("select_fields", [])
        if select_fields:
            select_clause = "SELECT " + ", ".join(str(field) for field in select_fields)
        else:
            select_clause = "SELECT *"
        sql_parts.append(select_clause)

        # Generate FROM clause
        from_table = analyzed_query.get("from_table", {})
        if from_table:
            table = from_table.get("table")
            alias = from_table.get("alias")
            if alias:
                from_clause = f"FROM {table} AS {alias}"
            else:
                from_clause = f"FROM {table}"
            sql_parts.append(from_clause)

        # Generate JOIN clauses
        joins = analyzed_query.get("joins", [])
        for join in joins:
            table = join.get("table")
            alias = join.get("alias")
            condition = join.get("condition")
            join_type = join.get("type", "INNER")

            if alias:
                join_clause = f"{join_type} JOIN {table} AS {alias}"
            else:
                join_clause = f"{join_type} JOIN {table}"

            if condition:
                join_clause += f" ON {condition}"

            sql_parts.append(join_clause)

        # Generate WHERE clause
        where_conditions = analyzed_query.get("where_conditions", [])
        param_idx = 0
        if where_conditions:
            where_parts = []
            for condition in where_conditions:
                field = condition["field"]
                operator = condition["operator"]
                value = condition["value"]

                # Create parameter name
                param_name = f"p{param_idx}"
                param_idx += 1

                where_parts.append(f"{field} {operator} :{param_name}")
                params[param_name] = value

            where_clause = "WHERE " + " AND ".join(where_parts)
            sql_parts.append(where_clause)

        # Generate ORDER BY clause
        order_by = analyzed_query.get("order_by", [])
        if order_by:
            order_parts = []
            for order_spec in order_by:
                field = order_spec["field"]
                direction = order_spec["direction"].upper()
                order_parts.append(f"{field} {direction}")
            order_clause = "ORDER BY " + ", ".join(order_parts)
            sql_parts.append(order_clause)

        # Generate LIMIT and OFFSET
        limit = analyzed_query.get("limit")
        offset = analyzed_query.get("offset")
        if limit is not None:
            sql_parts.append(f"LIMIT {limit}")
        if offset is not None:
            sql_parts.append(f"OFFSET {offset}")

        # Combine all parts
        sql = " ".join(sql_parts)
        return sql, params
```

## Step 3: Basic Schema Registry

Let's create a simple SchemaRegistry class:

```python
# pyquerybuilder/schema/registry.py
"""Registry for managing discovered schema information."""
from typing import Dict, List, Any, Optional


class SchemaRegistry:
    """Central registry for schema metadata."""

    def __init__(self):
        """Initialize empty registry."""
        self.tables = {}
        self.columns = {}
        self.relationships = {}
        self.join_paths = {}
        self.alias_map = {}

    def register_schema(self, schema_metadata):
        """Register discovered schema metadata."""
        self.tables = schema_metadata.get("tables", {})
        self.columns = schema_metadata.get("columns", {})
        self.relationships = schema_metadata.get("relationships", {})

        # Build alias map
        for table_name, table_info in self.tables.items():
            if "alias" in table_info:
                self.alias_map[table_info["alias"]] = table_name

        # Build join paths if not provided
        if "join_paths" in schema_metadata:
            self.join_paths = schema_metadata["join_paths"]
        else:
            self._build_join_paths()

    def _build_join_paths(self):
        """Build join paths from relationships."""
        for rel_id, rel in self.relationships.items():
            source_table = rel.get("source_table")
            target_table = rel.get("target_table")
            source_column = rel.get("source_column")
            target_column = rel.get("target_column")

            if all([source_table, target_table, source_column, target_column]):
                self._add_join_path(
                    source_table, target_table, source_column, target_column
                )

    def _add_join_path(self, source_table, target_table, source_column, target_column):
        """Add a join path between two tables."""
        # Get alias for target table
        target_alias = self.tables.get(target_table, {}).get("alias")
        if not target_alias:
            # Generate simple alias if not found
            target_alias = target_table[0].lower()

        # Build the join condition
        condition = f"{source_table}.{source_column} = {target_alias}.{target_column}"

        # Store the join path
        if source_table not in self.join_paths:
            self.join_paths[source_table] = {}

        self.join_paths[source_table][target_table] = {
            "table": target_table,
            "alias": target_alias,
            "condition": condition
        }
```

Let's create a simple alias generator to support the schema registry:

```python
# pyquerybuilder/schema/alias_generator.py
"""Functions for generating table aliases."""


def generate_alias(table_name):
    """Generate an alias for a table name.

    Args:
        table_name: Name of the table

    Returns:
        Generated alias string
    """
    # Simple alias generation - first letter of each word
    words = table_name.split('_')

    if len(words) == 1:
        # For single word, use first letter or first two letters
        if len(table_name) > 3:
            return table_name[:2].lower()
        return table_name[0].lower()

    # For multi-word names, use first letter of each word
    alias = ''.join(word[0] for word in words if word)

    return alias.lower()
```

Let's create a basic connector class for database access:

```python
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
```

Finally, let's update the package's `__init__.py` to expose the main classes:

```python
# pyquerybuilder/__init__.py
"""PyQueryBuilder - Fluent SQL query builder with schema discovery."""

from .core.builder import QueryBuilder
from .schema.registry import SchemaRegistry
```

This completes Phase 1 of the implementation, providing the foundation and core components for PyQueryBuilder. The implementation includes:

- Basic project structure and setup
- Core QueryBuilder class with fluent interface
- Simple SQL generation
- Schema registry for metadata storage
- Snowflake connector for database access

In the next phase, we'll improve the query analysis and SQL generation components to handle more complex queries.