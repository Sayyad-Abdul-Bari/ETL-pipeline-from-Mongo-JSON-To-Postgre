import re
from pathlib import Path

from .errors import ConfigError
from .postgres_loader import split_table_name


CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([^\s(]+)",
    re.IGNORECASE,
)


def load_schema_tables(schema_path: str) -> set:
    path = Path(schema_path)
    if not path.exists():
        raise ConfigError(f"Schema file not found: {schema_path}")

    content = path.read_text(encoding="utf-8")
    matches = CREATE_TABLE_PATTERN.findall(content)
    return {normalize_table_name(name) for name in matches}


def normalize_table_name(table_name: str) -> str:
    schema, table = split_table_name(table_name)
    return f"{schema.lower()}.{table.lower()}"
