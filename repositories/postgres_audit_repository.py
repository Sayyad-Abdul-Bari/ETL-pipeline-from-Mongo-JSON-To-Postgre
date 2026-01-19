from pathlib import Path

import pandas as pd
import psycopg2
import yaml
from psycopg2 import sql

from repositories.audit_repository import AuditRepository
from ui.postgres_setup import PostgresConfig


class PostgresAuditRepository(AuditRepository):
    def __init__(self, pg_config: PostgresConfig):
        self._config = pg_config

    @property
    def cache_key(self) -> str:
        return (
            f"{self._config.host}:{self._config.port}:"
            f"{self._config.database}:{self._config.username}:"
            f"{self._config.audit_schema}"
        )

    def fetch_ingestion_audit(self) -> pd.DataFrame:
        query = sql.SQL(
            """
            SELECT ingested_at::date AS ingestion_date,
                   object_id,
                   object_name,
                   source_collection,
                   processing_status,
                   missing_columns
            FROM {}.ingestion_audit
            ORDER BY ingested_at DESC
            """
        ).format(sql.Identifier(self._config.audit_schema))
        return self._read_query(query)

    def fetch_missing_columns_report(self) -> pd.DataFrame:
        collections = self._load_mapping_collections()
        table_to_collection = {
            config.get("target_table"): name
            for name, config in collections.items()
            if isinstance(config, dict) and config.get("target_table")
        }

        missing_query = sql.SQL(
            """
            SELECT ingestion_date,
                   object_name,
                   missing_columns
            FROM {}.missing_attributes_report
            ORDER BY ingestion_date DESC
            """
        ).format(sql.Identifier(self._config.audit_schema))
        df_missing = self._read_query(missing_query)
        if df_missing.empty:
            df_missing = pd.DataFrame(
                columns=["ingestion_date", "collection_name", "missing_columns"]
            )
        else:
            df_missing["collection_name"] = df_missing["object_name"].map(
                table_to_collection
            )
            df_missing["collection_name"] = df_missing["collection_name"].fillna(
                df_missing["object_name"]
            )
            df_missing = df_missing.drop(columns=["object_name"]).drop_duplicates(
                subset=["ingestion_date", "collection_name"]
            )

        counts_query = sql.SQL(
            """
            SELECT ingested_at::date AS ingestion_date,
                   source_collection AS collection_name,
                   COUNT(*) FILTER (
                       WHERE missing_columns IS NOT NULL
                         AND jsonb_array_length(missing_columns) > 0
                   ) AS missing_docs_count
            FROM {}.ingestion_audit
            WHERE source_collection IS NOT NULL
            GROUP BY ingested_at::date, source_collection
            """
        ).format(sql.Identifier(self._config.audit_schema))
        df_counts = self._read_query(counts_query)
        if df_counts.empty:
            df_counts = pd.DataFrame(
                columns=["ingestion_date", "collection_name", "missing_docs_count"]
            )

        df_report = pd.merge(
            df_counts,
            df_missing,
            on=["ingestion_date", "collection_name"],
            how="outer",
        )
        if "missing_docs_count" not in df_report.columns:
            df_report["missing_docs_count"] = 0
        else:
            df_report["missing_docs_count"] = (
                df_report["missing_docs_count"].fillna(0).astype(int)
            )

        df_report = df_report.sort_values(
            ["ingestion_date", "collection_name"],
            ascending=[False, True],
            na_position="last",
        )
        return df_report[
            ["collection_name", "ingestion_date", "missing_columns", "missing_docs_count"]
        ]

    def fetch_missing_collections_report(self) -> pd.DataFrame:
        query = sql.SQL(
            """
            SELECT ingestion_date,
                   object_name,
                   object_status
            FROM {}.missing_collections_report
            ORDER BY ingestion_date DESC
            """
        ).format(sql.Identifier(self._config.audit_schema))
        return self._read_query(query)

    def _load_mapping_collections(self) -> dict:
        path = Path("config/mapping_config.yaml")
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
        collections = data.get("collections", {})
        if not isinstance(collections, dict):
            return {}
        return collections

    def _read_query(self, query) -> pd.DataFrame:
        with psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.database,
            user=self._config.username,
            password=self._config.password,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
