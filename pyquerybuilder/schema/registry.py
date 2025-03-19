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