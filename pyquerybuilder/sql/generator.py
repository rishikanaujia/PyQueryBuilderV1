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