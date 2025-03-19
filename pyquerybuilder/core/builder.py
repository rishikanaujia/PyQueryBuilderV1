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