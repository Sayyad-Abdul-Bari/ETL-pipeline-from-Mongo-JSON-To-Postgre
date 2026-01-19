from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import psycopg2
import streamlit as st
import yaml
from psycopg2 import sql


@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    target_schema: str
    audit_schema: str


def render_postgres_config() -> Tuple[bool, PostgresConfig]:
    """Render PostgreSQL config UI and return (validated, config)."""
    st.header("PostgreSQL Configuration")
    default_target_schema = "public"
    default_audit_schema = "doc_audit"
    host = st.text_input("Host", value=st.session_state.get("pg_host", "localhost"))
    port = st.text_input("Port", value=st.session_state.get("pg_port", "5432"))
    username = st.text_input(
        "Username", value=st.session_state.get("pg_user", "etl_user")
    )
    password = st.text_input(
        "Password",
        type="password",
        value=st.session_state.get("pg_password", ""),
    )
    target_schema = default_target_schema
    audit_schema = default_audit_schema

    if host not in {"localhost", "127.0.0.1"}:
        st.error("PostgreSQL host must be localhost.")
        return False, None

    if not port.isdigit():
        st.error("Port must be a number.")
        return False, None

    server_validated = st.session_state.get("pg_server_validated", False)
    if st.button("Validate PostgreSQL Connection"):
        try:
            _validate_postgres_server(host, int(port), username, password)
            st.success("PostgreSQL connection validated.")
            server_validated = True
            st.session_state["pg_server_validated"] = True
        except Exception as exc:
            st.error(f"PostgreSQL validation failed: {exc}")
            server_validated = False
            st.session_state["pg_server_validated"] = False

    if not server_validated:
        return False, None

    databases = _list_databases(host, int(port), username, password)
    if not databases:
        st.error("No databases available. Check permissions.")
        return False, None

    st.subheader("Database Selection")
    db_action = st.radio(
        "Choose database action",
        ["Select existing database", "Create new database"],
        key="db_action",
    )
    selected_db = ""
    created = False
    if db_action == "Select existing database":
        selected_db = st.selectbox(
            "Existing databases",
            options=databases,
            index=_default_database_index(databases),
        )
    else:
        selected_db = st.text_input(
            "New database name", value=st.session_state.get("pg_database", "etl_db")
        )
        if st.button("Create Database"):
            try:
                _create_database(host, int(port), username, password, selected_db)
                st.success(f"Database '{selected_db}' created.")
                created = True
            except Exception as exc:
                st.error(f"Database creation failed: {exc}")
                return False, None

    if not selected_db:
        return False, None
    st.session_state["pg_database"] = selected_db

    config = PostgresConfig(
        host=host,
        port=int(port),
        database=selected_db,
        username=username,
        password=password,
        target_schema=target_schema,
        audit_schema=audit_schema,
    )

    if st.button("Save PostgreSQL Configuration"):
        try:
            _validate_database(config)
            _persist_runtime_pg_config(config)
            initialize_audit_schema(config)
            st.success("PostgreSQL configuration saved.")
            return True, config
        except Exception as exc:
            st.error(f"Database validation failed: {exc}")
            return False, config

    return False, config


def initialize_audit_schema(config: PostgresConfig) -> None:
    """Create audit schema and tables if they do not exist."""
    ddl_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {config.audit_schema};",
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_schema}.ingestion_audit (
            ingested_at TIMESTAMPTZ NOT NULL,
            object_id TEXT,
            source_collection TEXT,
            object_name TEXT NOT NULL,
            object_status TEXT NOT NULL,
            missing_columns JSONB,
            processing_status TEXT NOT NULL
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_schema}.missing_collections_report (
            ingestion_date DATE NOT NULL,
            object_name TEXT NOT NULL,
            object_status TEXT NOT NULL
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_schema}.missing_attributes_report (
            ingestion_date DATE NOT NULL,
            object_name TEXT NOT NULL,
            missing_columns JSONB
        );
        """,
    ]

    _execute_statements(config, ddl_statements)


def _persist_runtime_pg_config(config: PostgresConfig) -> None:
    runtime_config = {
        "host": config.host,
        "port": config.port,
        "database": config.database,
        "username": config.username,
        "password": config.password,
        "target_schema": config.target_schema,
        "audit_schema": config.audit_schema,
    }
    config_path = "config/runtime_pg_config.yaml"
    Path("config").mkdir(parents=True, exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(runtime_config, handle, sort_keys=False)


def _validate_postgres_server(host: str, port: int, username: str, password: str) -> None:
    with psycopg2.connect(
        host=host,
        port=port,
        dbname="postgres",
        user=username,
        password=password,
    ):
        return


def _list_databases(host: str, port: int, username: str, password: str) -> List[str]:
    with psycopg2.connect(
        host=host,
        port=port,
        dbname="postgres",
        user=username,
        password=password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
            )
            return [row[0] for row in cursor.fetchall()]


def _create_database(host: str, port: int, username: str, password: str, database: str) -> None:
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname="postgres",
        user=username,
        password=password,
    )
    try:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
            )
    finally:
        connection.close()


def _validate_database(config: PostgresConfig) -> None:
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.username,
        password=config.password,
    ):
        return


def _default_database_index(databases: List[str]) -> int:
    preferred = st.session_state.get("pg_database")
    if preferred in databases:
        return databases.index(preferred)
    return 0




def _execute_statements(config: PostgresConfig, statements) -> None:
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.username,
        password=config.password,
    ) as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        connection.commit()


