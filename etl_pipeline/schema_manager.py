from .errors import ConfigError
from .type_utils import normalize_type, normalize_type_mappings


def build_table_columns(
    mappings,
    raw_json_column,
    type_mappings,
    business_columns,
    business_column_types,
):
    normalized_type_mappings = normalize_type_mappings(type_mappings)
    columns = []
    column_names = set()

    for mapping in mappings.values():
        column_name = mapping["column"]
        normalized_type = normalize_type(mapping["type"])
        if normalized_type not in normalized_type_mappings:
            raise ConfigError(
                f"Missing SQL type mapping for '{mapping['type']}' in runtime.type_mappings."
            )
        sql_type = normalized_type_mappings[normalized_type]
        _add_column(columns, column_names, column_name, sql_type, not_null=False)

    _add_column(columns, column_names, raw_json_column, "JSONB", not_null=True)

    for logical_name, column_name in business_columns.items():
        if logical_name not in business_column_types:
            raise ConfigError(
                f"Missing audit.business_column_types for '{logical_name}'."
            )
        sql_type = business_column_types[logical_name]
        _add_column(columns, column_names, column_name, sql_type, not_null=True)

    return columns


def _add_column(columns, column_names, name, sql_type, not_null):
    if name in column_names:
        raise ConfigError(f"Duplicate column name detected: {name}")
    columns.append({"name": name, "type": sql_type, "not_null": not_null})
    column_names.add(name)
