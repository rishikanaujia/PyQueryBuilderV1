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