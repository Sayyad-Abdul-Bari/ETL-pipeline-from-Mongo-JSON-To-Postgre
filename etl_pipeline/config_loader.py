import json
from pathlib import Path

import yaml

from .errors import ConfigError
from .type_utils import SUPPORTED_TYPES, normalize_type, normalize_type_mappings


SUPPORTED_CONFIG_EXTENSIONS = {".json", ".yaml", ".yml"}


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    if path.suffix.lower() not in SUPPORTED_CONFIG_EXTENSIONS:
        raise ConfigError(
            f"Unsupported config format: {path.suffix}. Use JSON or YAML."
        )

    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
    else:
        with path.open("r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)

    if not isinstance(config, dict):
        raise ConfigError(f"Config file must contain a JSON/YAML object: {config_path}")

    return config


def validate_app_config(app_config: dict) -> None:
    required_sections = ["database", "runtime", "audit", "logging"]
    for section in required_sections:
        if section not in app_config:
            raise ConfigError(f"Missing '{section}' section in app config.")

    db_config = app_config["database"]
    for key in ["host", "port", "name", "user", "password"]:
        if key not in db_config:
            raise ConfigError(f"Missing database config key: {key}")

    runtime_config = app_config["runtime"]
    if "date_formats" not in runtime_config or not runtime_config["date_formats"]:
        raise ConfigError("Missing runtime.date_formats in app config.")
    if "date_output_format" not in runtime_config:
        raise ConfigError("Missing runtime.date_output_format in app config.")
    if "datetime_output_format" not in runtime_config:
        raise ConfigError("Missing runtime.datetime_output_format in app config.")
    if "schema_path" not in runtime_config:
        raise ConfigError("Missing runtime.schema_path in app config.")
    if "type_mappings" not in runtime_config or not runtime_config["type_mappings"]:
        raise ConfigError("Missing runtime.type_mappings in app config.")

    audit_config = app_config["audit"]
    for key in [
        "business_columns",
        "business_column_types",
        "audit_schema",
        "audit_table",
        "audit_columns",
        "audit_column_types",
        "status_values",
        "object_status_values",
    ]:
        if key not in audit_config:
            raise ConfigError(f"Missing audit.{key} in app config.")

    for column in ["ingested_at", "source_collection", "status"]:
        if column not in audit_config["business_columns"]:
            raise ConfigError(
                f"Missing audit.business_columns.{column} in app config."
            )

    for column in ["ingested_at", "source_collection", "status"]:
        if column not in audit_config["business_column_types"]:
            raise ConfigError(
                f"Missing audit.business_column_types.{column} in app config."
            )

    for column in [
        "ingested_at",
        "object_id",
        "source_collection",
        "object_name",
        "object_status",
        "missing_columns",
        "processing_status",
    ]:
        if column not in audit_config["audit_columns"]:
            raise ConfigError(f"Missing audit.audit_columns.{column} in app config.")

    for column in [
        "ingested_at",
        "object_id",
        "source_collection",
        "object_name",
        "object_status",
        "missing_columns",
        "processing_status",
    ]:
        if column not in audit_config["audit_column_types"]:
            raise ConfigError(
                f"Missing audit.audit_column_types.{column} in app config."
            )

    for status in ["success", "error", "missing"]:
        if status not in audit_config["status_values"]:
            raise ConfigError(f"Missing audit.status_values.{status} in app config.")

    for status in ["new", "missing", "already_exists"]:
        if status not in audit_config["object_status_values"]:
            raise ConfigError(
                f"Missing audit.object_status_values.{status} in app config."
            )

    logging_config = app_config["logging"]
    if "level" not in logging_config:
        raise ConfigError("Missing logging.level in app config.")


def validate_mapping_config(mapping_config: dict) -> None:
    if "collections" not in mapping_config:
        raise ConfigError("Missing 'collections' section in mapping config.")

    collections = mapping_config["collections"]
    if not isinstance(collections, dict) or not collections:
        raise ConfigError("Mapping config 'collections' must be a non-empty object.")

    for collection_name, collection_config in collections.items():
        for key in ["target_table", "raw_json_column", "object_id_attribute", "mappings"]:
            if key not in collection_config:
                raise ConfigError(
                    f"Missing '{key}' for collection '{collection_name}'."
                )

        mappings = collection_config["mappings"]
        if not isinstance(mappings, dict) or not mappings:
            raise ConfigError(
                f"Mappings for collection '{collection_name}' must be a non-empty object."
            )

        for source_attr, mapping in mappings.items():
            if "column" not in mapping or "type" not in mapping:
                raise ConfigError(
                    "Mapping entry must include 'column' and 'type' for "
                    f"attribute '{source_attr}' in collection '{collection_name}'."
                )


def validate_mapping_types(mapping_config: dict, type_mappings: dict) -> None:
    normalized_type_mappings = normalize_type_mappings(type_mappings)
    for collection_name, collection_config in mapping_config["collections"].items():
        mappings = collection_config["mappings"]
        for source_attr, mapping in mappings.items():
            normalized_type = normalize_type(mapping["type"])
            if normalized_type not in SUPPORTED_TYPES:
                raise ConfigError(
                    "Unsupported mapping type "
                    f"'{mapping['type']}' for attribute '{source_attr}' "
                    f"in collection '{collection_name}'."
                )
            if normalized_type not in normalized_type_mappings:
                raise ConfigError(
                    "Missing runtime.type_mappings entry for "
                    f"'{mapping['type']}' in collection '{collection_name}'."
                )
