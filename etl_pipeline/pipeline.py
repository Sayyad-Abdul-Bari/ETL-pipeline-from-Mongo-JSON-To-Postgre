from datetime import datetime, timezone

from .audit import build_audit_row, build_business_audit_fields
from .config_loader import (
    load_config,
    validate_app_config,
    validate_mapping_config,
    validate_mapping_types,
)
from .errors import LoadError
from .ingestion import load_input_json
from .logging_utils import configure_logging
from .mapping_resolver import get_collection_mapping
from .postgres_loader import PostgresLoader, ensure_database, split_table_name
from .reporting import PipelineReport
from .schema_manager import build_table_columns
from .schema_utils import load_schema_tables, normalize_table_name
from .transformer import transform_document


def run(
    input_path: str,
    app_config: str,
    mapping_config: str,
    logger=None,
) -> PipelineReport:
    """Run the ETL pipeline using config files and return a report."""
    app_config_data = load_config(app_config)
    mapping_config_data = load_config(mapping_config)
    validate_app_config(app_config_data)
    validate_mapping_config(mapping_config_data)
    validate_mapping_types(mapping_config_data, app_config_data["runtime"]["type_mappings"])

    if logger is None:
        logger = configure_logging(app_config_data["logging"]["level"])

    input_data = load_input_json(input_path)

    report = PipelineReport()
    runtime_config = app_config_data["runtime"]
    audit_config = app_config_data["audit"]

    schema_tables = load_schema_tables(runtime_config["schema_path"])

    audit_table = audit_config["audit_table"]
    audit_schema = audit_config.get("audit_schema")
    normalized_audit_table = normalize_table_name(audit_table)
    schema_tables_no_audit = set()
    for table in schema_tables:
        schema, _ = split_table_name(table)
        if audit_schema and schema.lower() == audit_schema.lower():
            continue
        if table == normalized_audit_table:
            continue
        schema_tables_no_audit.add(table)

    expected_collections = set(mapping_config_data["collections"].keys())
    input_collections = set(input_data.keys())
    missing_collections = expected_collections - input_collections
    if missing_collections:
        report.missing_collections.update(missing_collections)
        logger.warning(
            "Missing collections in input: %s",
            ", ".join(sorted(missing_collections)),
        )

    table_lookup = {}
    table_to_collection = {}
    for collection_name, collection_config in mapping_config_data["collections"].items():
        target_table = collection_config["target_table"]
        normalized_table = normalize_table_name(target_table)
        table_lookup[normalized_table] = target_table
        table_to_collection[normalized_table] = collection_name

    input_tables = set()
    for collection_name in input_data.keys():
        collection_config = mapping_config_data["collections"].get(collection_name)
        if collection_config:
            input_tables.add(normalize_table_name(collection_config["target_table"]))

    missing_input_tables = schema_tables_no_audit - input_tables
    if missing_input_tables:
        report.missing_tables_input.update(
            table_lookup.get(table, table) for table in missing_input_tables
        )
        logger.warning(
            "Tables in schema.sql missing from input: %s",
            ", ".join(sorted(report.missing_tables_input)),
        )

    ingestion_date = datetime.now(timezone.utc).date().isoformat()
    report.ingestion_date = ingestion_date

    try:
        ensure_database(app_config_data["database"])
        with PostgresLoader(app_config_data["database"]) as loader:
            _ensure_audit_tables(loader, audit_config)
            if not loader.table_exists(audit_table):
                raise LoadError(
                    f"Audit table '{audit_table}' is missing in the database."
                )

            for table in sorted(missing_input_tables):
                object_name = table_lookup.get(table, table)
                report.record_object_status(
                    object_name, audit_config["object_status_values"]["missing"]
                )
                audit_row = build_audit_row(
                    audit_config=audit_config,
                    object_id=None,
                    source_collection=table_to_collection.get(table),
                    object_name=object_name,
                    object_status=audit_config["object_status_values"]["missing"],
                    missing_columns=[],
                    processing_status=audit_config["status_values"]["missing"],
                )
                try:
                    loader.insert_row(audit_table, audit_row)
                    loader.commit()
                except Exception as exc:
                    loader.rollback()
                    logger.error(
                        "Failed to insert missing table audit for '%s': %s",
                        object_name,
                        exc,
                    )

            for collection_name, documents in input_data.items():
                collection_config = get_collection_mapping(
                    mapping_config_data, collection_name
                )
                if not collection_config:
                    logger.error(
                        "No mapping found for collection '%s'.", collection_name
                    )
                    report.unmapped_collections.add(collection_name)
                    continue

                target_table = collection_config["target_table"]
                raw_json_column = collection_config["raw_json_column"]
                object_id_attribute = collection_config["object_id_attribute"]
                mappings = collection_config["mappings"]
                normalized_table = normalize_table_name(target_table)
                table_in_schema = normalized_table in schema_tables_no_audit
                table_exists = loader.table_exists(target_table)

                if table_exists:
                    object_status = audit_config["object_status_values"]["already_exists"]
                elif not table_in_schema:
                    object_status = audit_config["object_status_values"]["new"]
                else:
                    object_status = audit_config["object_status_values"]["missing"]

                report.record_object_status(target_table, object_status)

                if not table_exists and not table_in_schema:
                    _ensure_schema(loader, target_table)
                    table_columns = build_table_columns(
                        mappings=mappings,
                        raw_json_column=raw_json_column,
                        type_mappings=runtime_config["type_mappings"],
                        business_columns=audit_config["business_columns"],
                        business_column_types=audit_config["business_column_types"],
                    )
                    loader.create_table(target_table, table_columns)
                    loader.commit()
                    logger.info(
                        "Created destination table '%s' for collection '%s'.",
                        target_table,
                        collection_name,
                    )
                    table_exists = True

                if table_in_schema and not table_exists:
                    logger.error(
                        "Missing destination table '%s' for collection '%s'.",
                        target_table,
                        collection_name,
                    )
                    report.missing_tables_db.add(target_table)

                if not documents:
                    logger.info(
                        "No documents found for collection '%s'.", collection_name
                    )
                    continue

                for index, document in enumerate(documents):
                    transformed, missing_columns, errors = transform_document(
                        document,
                        mappings,
                        runtime_config["date_formats"],
                        runtime_config["date_output_format"],
                        runtime_config["datetime_output_format"],
                    )

                    if missing_columns:
                        logger.warning(
                            "Collection '%s' document %d missing columns: %s",
                            collection_name,
                            index,
                            ", ".join(sorted(missing_columns)),
                        )
                        report.record_missing_columns(target_table, missing_columns)

                    if errors:
                        logger.error(
                            "Collection '%s' document %d transformation errors: %s",
                            collection_name,
                            index,
                            "; ".join(errors),
                        )

                    business_status = (
                        audit_config["status_values"]["error"]
                        if errors
                        else audit_config["status_values"]["success"]
                    )
                    processing_status = business_status
                    object_id = document.get(object_id_attribute)
                    if object_id is None:
                        logger.warning(
                            "Collection '%s' document %d missing object_id attribute '%s'.",
                            collection_name,
                            index,
                            object_id_attribute,
                        )

                    insert_failed = False
                    if table_exists:
                        row = {
                            **transformed,
                            raw_json_column: document,
                            **build_business_audit_fields(
                                audit_config, collection_name, business_status
                            ),
                        }
                        try:
                            loader.insert_row(target_table, row)
                        except Exception as exc:
                            loader.rollback()
                            processing_status = audit_config["status_values"]["error"]
                            insert_failed = True
                            logger.error(
                                "Failed to insert document %d in '%s': %s",
                                index,
                                collection_name,
                                exc,
                            )
                    else:
                        processing_status = audit_config["status_values"]["missing"]
                        insert_failed = True

                    audit_row = build_audit_row(
                        audit_config=audit_config,
                        object_id=str(object_id) if object_id is not None else None,
                        source_collection=collection_name,
                        object_name=target_table,
                        object_status=object_status,
                        missing_columns=missing_columns,
                        processing_status=processing_status,
                    )

                    try:
                        loader.insert_row(audit_table, audit_row)
                        loader.commit()
                        report.record_document(
                            collection_name,
                            had_errors=bool(errors),
                            insert_failed=insert_failed,
                        )
                    except Exception as exc:
                        loader.rollback()
                        logger.error(
                            "Failed to insert audit record for document %d in '%s': %s",
                            index,
                            collection_name,
                            exc,
                        )
                        report.record_document(
                            collection_name,
                            had_errors=bool(errors),
                            insert_failed=True,
                        )
    except LoadError:
        raise
    except Exception as exc:
        raise LoadError(str(exc)) from exc

    report.log_summary(logger, ingestion_date)
    return report


def _ensure_schema(loader: PostgresLoader, table_name: str) -> None:
    schema, _ = split_table_name(table_name)
    loader.create_schema(schema)


def _build_audit_table_columns(audit_config: dict) -> list:
    columns = []
    audit_columns = audit_config["audit_columns"]
    audit_column_types = audit_config["audit_column_types"]
    for key, column_name in audit_columns.items():
        column_type = audit_column_types.get(key)
        if not column_type:
            continue
        columns.append({"name": column_name, "type": column_type})
    return columns


def _ensure_audit_tables(loader: PostgresLoader, audit_config: dict) -> None:
    audit_table = audit_config["audit_table"]
    audit_schema = audit_config.get("audit_schema")
    schema, _ = split_table_name(audit_table)
    target_schema = audit_schema or schema
    loader.create_schema(target_schema)

    if not loader.table_exists(audit_table):
        audit_columns = _build_audit_table_columns(audit_config)
        loader.create_table(audit_table, audit_columns)

    report_tables = {
        f"{target_schema}.missing_attributes_report": [
            {"name": "ingestion_date", "type": "DATE"},
            {"name": "object_name", "type": "TEXT"},
            {"name": "missing_columns", "type": "JSONB"},
        ],
        f"{target_schema}.missing_collections_report": [
            {"name": "ingestion_date", "type": "DATE"},
            {"name": "object_name", "type": "TEXT"},
            {"name": "object_status", "type": "TEXT"},
        ],
    }
    for table_name, columns in report_tables.items():
        if not loader.table_exists(table_name):
            loader.create_table(table_name, columns)

    loader.commit()
