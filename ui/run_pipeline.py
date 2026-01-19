import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
import streamlit as st
import yaml
from psycopg2 import sql
from psycopg2.extras import Json

import etl_pipeline
from ui.postgres_setup import PostgresConfig


DATE_ONLY_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%Y.%m.%d",
]

DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%d-%m-%Y %H:%M:%S",
    "%m/%d/%Y %I:%M %p",
]

DEFAULT_DATE_FORMATS = DATE_ONLY_FORMATS + DATETIME_FORMATS


def generate_runtime_configs(
    input_path: str,
    source_type: str,
    pg_config: PostgresConfig,
    mapping_config_override: Optional[Dict[str, dict]] = None,
    collection_filter: Optional[List[str]] = None,
) -> Dict[str, str]:
    """Generate runtime configs for Phase 0 and return file paths."""
    runtime_dir = Path("config/runtime")
    runtime_dir.mkdir(parents=True, exist_ok=True)

    schema_path = runtime_dir / "schema.sql"
    if not schema_path.exists():
        schema_path.write_text("-- Runtime schema placeholder\n", encoding="utf-8")

    app_config_path = runtime_dir / "app_config.yaml"
    mapping_config_path = runtime_dir / "mapping_config.yaml"

    input_data = _load_json(input_path)
    if collection_filter:
        input_data = {
            collection_name: input_data.get(collection_name, [])
            for collection_name in collection_filter
        }
        input_path = _write_filtered_input(runtime_dir, collection_filter, input_data)
    if mapping_config_override:
        mapping_config = mapping_config_override
    else:
        mapping_config = _build_mapping_config(input_data, pg_config.target_schema)

    app_config = _build_app_config(
        input_path=input_path,
        source_type=source_type,
        pg_config=pg_config,
        schema_path=str(schema_path),
    )

    app_config_path.write_text(yaml.safe_dump(app_config, sort_keys=False), encoding="utf-8")
    mapping_config_path.write_text(
        yaml.safe_dump(mapping_config, sort_keys=False), encoding="utf-8"
    )

    return {
        "app_config": str(app_config_path),
        "mapping_config": str(mapping_config_path),
        "schema_path": str(schema_path),
        "input_path": input_path,
    }


def run_etl(
    input_path: str,
    app_config_path: str,
    mapping_config_path: str,
    pg_config: PostgresConfig,
    scope: str = "overall",
    target_table: Optional[str] = None,
) -> None:
    """Run Phase 0 ETL and stream logs and summaries to the UI."""
    if scope == "collection":
        st.subheader("Run ETL (Collection)")
    else:
        st.header("Run ETL")
    logs: List[str] = st.session_state.setdefault("etl_logs", [])
    logs.clear()

    logger = _build_streamlit_logger(logs)

    try:
        report = etl_pipeline.run(
            input_path=input_path,
            app_config=app_config_path,
            mapping_config=mapping_config_path,
            logger=logger,
        )
        _persist_report_tables(report, pg_config)
        audit_count = _fetch_audit_count(pg_config, report.ingestion_date)
        st.success("ETL run completed.")
        st.write(f"Audit records written: {audit_count}")
        if scope == "collection":
            _render_collection_audit(
                pg_config, report.ingestion_date, target_table
            )
            _render_collection_missing_columns(
                pg_config, report.ingestion_date, target_table
            )
        else:
            _render_audit_pivot(pg_config, report.ingestion_date)
            _render_missing_reports(pg_config, report.ingestion_date)
            _render_missing_summary(report)
    except Exception as exc:
        st.error(f"ETL run failed: {exc}")

    error_logs = _filter_error_logs(logs)
    if error_logs:
        st.text_area("ETL Errors", value="\n".join(error_logs), height=300)


def _build_streamlit_logger(logs: List[str]) -> logging.Logger:
    """Create a Streamlit-friendly logger that writes to a list."""
    logger = logging.getLogger("etl_pipeline_streamlit")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = _StreamlitLogHandler(logs)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class _StreamlitLogHandler(logging.Handler):
    def __init__(self, logs: List[str]):
        super().__init__()
        self.logs = logs

    def emit(self, record):
        self.logs.append(self.format(record))


def _filter_error_logs(logs: List[str]) -> List[str]:
    return [
        line
        for line in logs
        if " [ERROR] " in line or " [CRITICAL] " in line
    ]


def _build_app_config(
    input_path: str,
    source_type: str,
    pg_config: PostgresConfig,
    schema_path: str,
) -> dict:
    """Build an app_config payload compatible with Phase 0."""
    return {
        "source": {
            "type": source_type,
            "input_path": input_path,
        },
        "database": {
            "host": pg_config.host,
            "port": pg_config.port,
            "name": pg_config.database,
            "user": pg_config.username,
            "password": pg_config.password,
            "sslmode": "disable",
        },
        "runtime": {
            "date_formats": DEFAULT_DATE_FORMATS,
            "date_output_format": "%Y-%m-%d",
            "datetime_output_format": "%Y-%m-%dT%H:%M:%S%z",
            "schema_path": schema_path,
            "type_mappings": _default_type_mappings(),
        },
        "audit": {
            "business_columns": {
                "ingested_at": "ingested_at",
                "source_collection": "source_collection",
                "status": "status",
            },
            "business_column_types": {
                "ingested_at": "TIMESTAMPTZ",
                "source_collection": "TEXT",
                "status": "TEXT",
            },
            "audit_table": f"{pg_config.audit_schema}.ingestion_audit",
            "audit_schema": pg_config.audit_schema,
            "audit_columns": {
                "ingested_at": "ingested_at",
                "object_id": "object_id",
                "source_collection": "source_collection",
                "object_name": "object_name",
                "object_status": "object_status",
                "missing_columns": "missing_columns",
                "processing_status": "processing_status",
            },
            "audit_column_types": {
                "ingested_at": "TIMESTAMPTZ",
                "object_id": "TEXT",
                "source_collection": "TEXT",
                "object_name": "TEXT",
                "object_status": "TEXT",
                "missing_columns": "JSONB",
                "processing_status": "TEXT",
            },
            "status_values": {
                "success": "success",
                "error": "error",
                "missing": "missing",
            },
            "object_status_values": {
                "new": "NEW",
                "missing": "MISSING",
                "already_exists": "ALREADY_EXISTS",
            },
        },
        "logging": {
            "level": "INFO",
        },
    }


def _build_mapping_config(input_data: dict, target_schema: str) -> dict:
    """Infer a placeholder mapping_config from staged input data."""
    collections_config = {}
    for collection_name, documents in input_data.items():
        attributes = _collect_attributes(documents)
        object_id_attribute = _select_object_id(attributes)
        mappings = {}
        for attribute in attributes:
            mappings[attribute] = {
                "column": attribute,
                "type": _infer_type(attribute, documents),
            }
        collections_config[collection_name] = {
            "target_table": f"{target_schema}.{collection_name}",
            "raw_json_column": "raw_json",
            "object_id_attribute": object_id_attribute,
            "mappings": mappings,
        }
    return {"collections": collections_config}


def _collect_attributes(documents: List[dict]) -> List[str]:
    """Collect unique attribute names from documents."""
    attributes = set()
    for document in documents:
        attributes.update(document.keys())
    return sorted(attributes)


def _select_object_id(attributes: List[str]) -> str:
    """Choose a default object_id attribute from available attributes."""
    if "_id" in attributes:
        return "_id"
    if "id" in attributes:
        return "id"
    return attributes[0] if attributes else "id"


def _infer_type(attribute: str, documents: List[dict]) -> str:
    """Infer a mapping type based on observed attribute values."""
    observed_types = set()
    for document in documents:
        value = document.get(attribute)
        if value is None:
            continue
        observed_types.add(_infer_value_type(value))

    if not observed_types:
        return "text"
    if observed_types == {"integer"}:
        return "integer"
    if observed_types.issubset({"integer", "numeric"}):
        return "numeric" if "numeric" in observed_types else "integer"
    if observed_types == {"boolean"}:
        return "boolean"
    if observed_types == {"date"}:
        return "date"
    if len(observed_types) > 1:
        return "text"
    return observed_types.pop()


def _infer_value_type(value: Any) -> str:
    """Infer a single value type suitable for mapping."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "numeric"
    if isinstance(value, str):
        if _parse_datetime(value):
            return "datetime"
        if _parse_date(value):
            return "date"
        numeric_type = _parse_numeric(value)
        if numeric_type:
            return numeric_type
        return "text"
    return "text"


def _parse_date(value: str) -> bool:
    """Return True if the value matches configured date-only formats."""
    for fmt in DATE_ONLY_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def _parse_datetime(value: str) -> bool:
    """Return True if the value matches configured datetime formats."""
    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def _parse_numeric(value: str) -> str:
    """Parse numeric strings into integer or numeric labels."""
    try:
        int(value)
        return "integer"
    except ValueError:
        pass
    try:
        float(value)
        return "numeric"
    except ValueError:
        return ""


def _default_type_mappings() -> dict:
    """Return the default mapping of logical types to SQL types."""
    return {
        "text": "TEXT",
        "string": "TEXT",
        "varchar": "TEXT",
        "integer": "INTEGER",
        "int": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "float": "DOUBLE PRECISION",
        "double": "DOUBLE PRECISION",
        "double precision": "DOUBLE PRECISION",
        "numeric": "NUMERIC",
        "decimal": "NUMERIC",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMPTZ",
    }


def _load_json(path: str) -> dict:
    """Load JSON from a file path."""
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_filtered_input(
    runtime_dir: Path, collection_names: List[str], data: dict
) -> str:
    suffix = "selected" if len(collection_names) > 1 else collection_names[0]
    file_path = runtime_dir / f"input_{suffix}.json"
    file_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return str(file_path)


def _persist_report_tables(report, pg_config: PostgresConfig) -> None:
    """Persist audit reports into doc_audit report tables."""
    if not report.ingestion_date:
        return
    with psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        dbname=pg_config.database,
        user=pg_config.username,
        password=pg_config.password,
    ) as connection:
        with connection.cursor() as cursor:
            _insert_object_statuses(cursor, report, pg_config.audit_schema)
            _insert_missing_columns(cursor, report, pg_config.audit_schema)
        connection.commit()


def _insert_object_statuses(cursor, report, audit_schema: str) -> None:
    """Insert object-level status rows into the report table."""
    table = sql.Identifier(audit_schema, "missing_collections_report")
    for object_name, status in report.object_statuses.items():
        cursor.execute(
            sql.SQL(
                "INSERT INTO {} (ingestion_date, object_name, object_status) VALUES (%s, %s, %s)"
            ).format(table),
            (report.ingestion_date, object_name, status),
        )


def _insert_missing_columns(cursor, report, audit_schema: str) -> None:
    """Insert missing column rows into the report table."""
    table = sql.Identifier(audit_schema, "missing_attributes_report")
    for object_name, missing_columns in report.missing_columns_by_object.items():
        cursor.execute(
            sql.SQL(
                "INSERT INTO {} (ingestion_date, object_name, missing_columns) VALUES (%s, %s, %s)"
            ).format(table),
            (report.ingestion_date, object_name, Json(list(missing_columns))),
        )


def _fetch_audit_count(pg_config: PostgresConfig, ingestion_date: str) -> int:
    """Fetch audit record count from ingestion_audit for the run date."""
    with psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        dbname=pg_config.database,
        user=pg_config.username,
        password=pg_config.password,
    ) as connection:
        with connection.cursor() as cursor:
            query = sql.SQL(
                "SELECT COUNT(*) FROM {}.ingestion_audit WHERE ingested_at::date = %s"
            ).format(sql.Identifier(pg_config.audit_schema))
            cursor.execute(query, (ingestion_date,))
            return int(cursor.fetchone()[0])


def _fetch_latest_ingestion_date(pg_config: PostgresConfig) -> Optional[str]:
    query = sql.SQL(
        "SELECT MAX(ingested_at::date) FROM {}.ingestion_audit"
    ).format(sql.Identifier(pg_config.audit_schema))
    with psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        dbname=pg_config.database,
        user=pg_config.username,
        password=pg_config.password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            if row and row[0]:
                return row[0].isoformat()
            return None


def _fetch_audit_rows(
    pg_config: PostgresConfig,
    ingestion_date: str,
    object_name: Optional[str] = None,
) -> List[dict]:
    where_clause = "WHERE ingested_at::date = %s"
    params: List[object] = [ingestion_date]
    if object_name:
        where_clause += " AND object_name = %s"
        params.append(object_name)
    query = sql.SQL(
        f"""
        SELECT object_id, object_name, processing_status, missing_columns
        FROM {{}}.ingestion_audit
        {where_clause}
        """
    ).format(sql.Identifier(pg_config.audit_schema))
    with psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        dbname=pg_config.database,
        user=pg_config.username,
        password=pg_config.password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {
                    "object_id": row[0],
                    "object_name": row[1],
                    "processing_status": row[2],
                    "missing_columns": row[3],
                }
                for row in rows
            ]


def _fetch_missing_report(pg_config: PostgresConfig, ingestion_date: str) -> List[dict]:
    query = sql.SQL(
        """
        SELECT ingestion_date, object_name, object_status
        FROM {}.missing_collections_report
        WHERE ingestion_date = %s
        ORDER BY object_name
        """
    ).format(sql.Identifier(pg_config.audit_schema))
    with psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        dbname=pg_config.database,
        user=pg_config.username,
        password=pg_config.password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (ingestion_date,))
            rows = cursor.fetchall()
            return [
                {
                    "ingestion_date": row[0],
                    "object_name": row[1],
                    "object_status": row[2],
                }
                for row in rows
            ]


def _fetch_missing_columns_report(
    pg_config: PostgresConfig,
    ingestion_date: str,
    object_name: Optional[str] = None,
) -> List[dict]:
    where_clause = "WHERE ingestion_date = %s"
    params: List[object] = [ingestion_date]
    if object_name:
        where_clause += " AND object_name = %s"
        params.append(object_name)
    query = sql.SQL(
        f"""
        SELECT ingestion_date, object_name, missing_columns
        FROM {{}}.missing_attributes_report
        {where_clause}
        ORDER BY object_name
        """
    ).format(sql.Identifier(pg_config.audit_schema))
    with psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        dbname=pg_config.database,
        user=pg_config.username,
        password=pg_config.password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                {
                    "ingestion_date": row[0],
                    "object_name": row[1],
                    "missing_columns": row[2],
                }
                for row in rows
            ]


def _render_missing_summary(report) -> None:
    """Render a concise missing summary in the UI."""
    rejected = {
        name: count for name, count in report.insert_failures.items() if count > 0
    }
    if (
        not report.missing_collections
        and not report.missing_tables_db
        and not report.missing_tables_input
        and not rejected
    ):
        return

    st.subheader("Missing Summary")
    if report.missing_collections:
        st.write(
            "Missing collections: " + ", ".join(sorted(report.missing_collections))
        )
    if report.missing_tables_db:
        st.write(
            "Missing tables (DB): " + ", ".join(sorted(report.missing_tables_db))
        )
    if report.missing_tables_input:
        st.write(
            "Missing tables (input): " + ", ".join(sorted(report.missing_tables_input))
        )
    if rejected:
        st.write(
            "Rejected inserts: "
            + ", ".join(f"{name}={count}" for name, count in rejected.items())
        )


def _render_audit_pivot(pg_config: PostgresConfig, ingestion_date: str) -> None:
    st.subheader("Audit Pivot (by Object and Status)")
    rows = _fetch_audit_rows(pg_config, ingestion_date)
    if not rows:
        return
    df = pd.DataFrame(rows)
    pivot = pd.pivot_table(
        df,
        index="object_name",
        columns="processing_status",
        values="object_id",
        aggfunc="count",
        fill_value=0,
    )
    st.dataframe(pivot, use_container_width=True)
    st.subheader("Ingestion Audit (by Document ID)")
    st.dataframe(df, use_container_width=True)


def _render_collection_audit(
    pg_config: PostgresConfig, ingestion_date: str, target_table: Optional[str]
) -> None:
    rows = _fetch_audit_rows(pg_config, ingestion_date, target_table)
    if not rows:
        return
    st.subheader("Ingestion Audit (Collection)")
    st.dataframe(pd.DataFrame(rows), use_container_width=True)


def _render_collection_missing_columns(
    pg_config: PostgresConfig, ingestion_date: str, target_table: Optional[str]
) -> None:
    rows = _fetch_missing_columns_report(pg_config, ingestion_date, target_table)
    if not rows:
        return
    st.subheader("Missing Columns Report (Collection)")
    st.dataframe(pd.DataFrame(rows), use_container_width=True)


def render_overall_report(pg_config: PostgresConfig) -> None:
    """Render overall audit and missing reports without running ETL."""
    latest_date = _fetch_latest_ingestion_date(pg_config)
    if not latest_date:
        st.info("No audit data available for overall report.")
        return
    st.header("Overall Report")
    _render_audit_pivot(pg_config, latest_date)
    _render_missing_reports(pg_config, latest_date)


def _render_missing_reports(pg_config: PostgresConfig, ingestion_date: str) -> None:
    missing_rows = _fetch_missing_report(pg_config, ingestion_date)
    missing_columns_rows = _fetch_missing_columns_report(pg_config, ingestion_date)
    if not missing_rows and not missing_columns_rows:
        return

    if missing_rows:
        st.subheader("Missing Collections Report")
        st.dataframe(pd.DataFrame(missing_rows), use_container_width=True)
    if missing_columns_rows:
        st.subheader("Missing Columns Report")
        st.dataframe(pd.DataFrame(missing_columns_rows), use_container_width=True)
