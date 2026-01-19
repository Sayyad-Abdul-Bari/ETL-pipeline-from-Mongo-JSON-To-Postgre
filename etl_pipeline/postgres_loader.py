from typing import Any, Dict, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

from .errors import LoadError


class PostgresLoader:
    def __init__(self, db_config: Dict[str, Any]):
        self._db_config = db_config
        self._connection = None
        self._cursor = None

    def __enter__(self):
        try:
            connection_params = {
                "host": self._db_config["host"],
                "port": self._db_config["port"],
                "dbname": self._db_config["name"],
                "user": self._db_config["user"],
                "password": self._db_config["password"],
            }
            sslmode = self._db_config.get("sslmode")
            if sslmode:
                connection_params["sslmode"] = sslmode
            self._connection = psycopg2.connect(**connection_params)
            self._connection.autocommit = False
            self._cursor = self._connection.cursor()
        except Exception as exc:
            raise LoadError(f"Failed to connect to PostgreSQL: {exc}") from exc
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            if exc:
                self._connection.rollback()
            else:
                self._connection.commit()
            self._connection.close()

    def commit(self):
        if self._connection:
            self._connection.commit()

    def rollback(self):
        if self._connection:
            self._connection.rollback()

    def table_exists(self, table_name: str) -> bool:
        schema, table = split_table_name(table_name)
        query = """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        self._cursor.execute(query, (schema, table))
        return self._cursor.fetchone() is not None

    def insert_row(self, table_name: str, row: Dict[str, Any]):
        if not row:
            raise LoadError("Cannot insert empty row.")

        schema, table = split_table_name(table_name)
        table_identifier = sql.Identifier(schema, table)
        columns = list(row.keys())
        values = [prepare_value(value) for value in row.values()]

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            table_identifier,
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )
        self._cursor.execute(query, values)

    def create_table(self, table_name: str, columns: list):
        if not columns:
            raise LoadError("Cannot create table without columns.")

        schema, table = split_table_name(table_name)
        table_identifier = sql.Identifier(schema, table)
        column_definitions = []
        for column in columns:
            column_sql = sql.SQL("{} {}").format(
                sql.Identifier(column["name"]),
                sql.SQL(column["type"]),
            )
            if column.get("not_null"):
                column_sql = sql.SQL("{} NOT NULL").format(column_sql)
            column_definitions.append(column_sql)

        query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            table_identifier,
            sql.SQL(", ").join(column_definitions),
        )
        self._cursor.execute(query)

    def create_schema(self, schema_name: str):
        if not schema_name:
            raise LoadError("Schema name is required.")
        query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema_name)
        )
        self._cursor.execute(query)


def ensure_database(db_config: Dict[str, Any]) -> None:
    if not db_config.get("create_if_missing", True):
        return

    connection_params = {
        "host": db_config["host"],
        "port": db_config["port"],
        "dbname": db_config["name"],
        "user": db_config["user"],
        "password": db_config["password"],
    }
    sslmode = db_config.get("sslmode")
    if sslmode:
        connection_params["sslmode"] = sslmode

    try:
        connection = psycopg2.connect(**connection_params)
        connection.close()
        return
    except psycopg2.OperationalError as exc:
        message = str(exc)
        if "does not exist" not in message:
            raise LoadError(f"Failed to connect to PostgreSQL: {exc}") from exc
        if not db_config.get("create_if_missing", True):
            raise LoadError(
                f"Database '{db_config['name']}' does not exist and auto-create is disabled."
            ) from exc

    admin_db = db_config.get("admin_db", "postgres")
    admin_params = {
        "host": db_config["host"],
        "port": db_config["port"],
        "dbname": admin_db,
        "user": db_config["user"],
        "password": db_config["password"],
    }
    if sslmode:
        admin_params["sslmode"] = sslmode

    try:
        connection = psycopg2.connect(**admin_params)
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (db_config["name"],),
            )
            exists = cursor.fetchone() is not None
            if not exists:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(db_config["name"])
                    )
                )
    except Exception as exc:
        raise LoadError(f"Failed to create database '{db_config['name']}': {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()


def prepare_value(value: Any):
    if isinstance(value, (dict, list)):
        return Json(value)
    return value


def split_table_name(table_name: str) -> Tuple[str, str]:
    if "." in table_name:
        schema, table = table_name.split(".", maxsplit=1)
        return schema, table
    return "public", table_name
