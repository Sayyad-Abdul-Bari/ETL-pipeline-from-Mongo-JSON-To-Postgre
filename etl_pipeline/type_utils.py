def normalize_type(type_value: str) -> str:
    return type_value.strip().lower()


def normalize_type_mappings(type_mappings: dict) -> dict:
    return {normalize_type(key): value for key, value in type_mappings.items()}


SUPPORTED_TYPES = {
    "text",
    "string",
    "varchar",
    "integer",
    "int",
    "bigint",
    "smallint",
    "float",
    "double",
    "double precision",
    "numeric",
    "decimal",
    "boolean",
    "bool",
    "date",
    "datetime",
}
