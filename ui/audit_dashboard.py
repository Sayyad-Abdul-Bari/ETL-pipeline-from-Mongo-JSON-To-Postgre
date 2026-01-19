from pathlib import Path
from typing import Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml

from repositories import AuditRepository, get_audit_repository
from ui.postgres_setup import PostgresConfig


def render_audit_dashboard(
    pg_config: PostgresConfig, force_refresh: bool = False
) -> None:
    """Render the audit observability dashboard."""
    st.header("ETL Audit & Observability Dashboard")
    refresh = st.button("Refresh Dashboard")
    if refresh or force_refresh:
        load_audit_data.clear()

    repository = get_audit_repository(pg_config)
    df_ingestion, df_missing_cols = load_audit_data(repository)
    expected_collections = _load_mapping_collections()
    if df_ingestion.empty and df_missing_cols.empty and not expected_collections:
        st.info("No audit data available.")
        return

    _render_kpis(df_ingestion)
    _render_audit_pivot(df_ingestion)
    _render_ingestion_drilldown(df_ingestion)
    _render_missing_columns(df_ingestion, df_missing_cols)
    _render_missing_collections(df_ingestion)


@st.cache_data(show_spinner=False)
def load_audit_data(
    _repository: AuditRepository,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load audit datasets via the repository."""
    try:
        return (
            _repository.fetch_ingestion_audit(),
            _repository.fetch_missing_columns_report(),
        )
    except Exception:
        return pd.DataFrame(), pd.DataFrame()


def _render_kpis(
    df_ingestion: pd.DataFrame,
) -> None:
    st.subheader("KPI Summary")
    total_docs = int(df_ingestion.shape[0])
    successful_docs = int(
        df_ingestion[df_ingestion["processing_status"] == "success"].shape[0]
    )
    if df_ingestion.empty or "missing_columns" not in df_ingestion.columns:
        missing_columns_docs = 0
    else:
        missing_columns_docs = int(
            df_ingestion["missing_columns"]
            .apply(lambda value: isinstance(value, (list, tuple)) and len(value) > 0)
            .sum()
        )

    expected_collections = set(_load_mapping_collections())
    if "source_collection" in df_ingestion.columns:
        processed_collections = set(
            df_ingestion["source_collection"].dropna().unique()
        )
    else:
        processed_collections = set()
    processed_expected = expected_collections & processed_collections
    missing_expected = expected_collections - processed_collections

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Documents", total_docs)
    col2.metric("Successful Documents", successful_docs)
    col3.metric("Docs w/ Missing Columns", missing_columns_docs)
    if expected_collections:
        coverage = pd.DataFrame(
            {
                "status": ["Processed", "Missing"],
                "count": [len(processed_expected), len(missing_expected)],
            }
        )
        pie = px.pie(
            coverage,
            names="status",
            values="count",
            title="Collection Coverage",
        )
        pie.update_traces(textinfo="percent+label")
        col4.plotly_chart(pie, use_container_width=True)
    else:
        col4.info("No collections found in mapping_config.yaml.")


def _render_audit_pivot(df_ingestion: pd.DataFrame) -> None:
    st.subheader("Audit Status Overview")
    if df_ingestion.empty:
        st.info("No ingestion audit data available.")
        return

    pivot = pd.pivot_table(
        df_ingestion,
        index="object_name",
        columns="processing_status",
        values="object_id",
        aggfunc="count",
        fill_value=0,
    )
    st.dataframe(pivot, use_container_width=True)

    heatmap = px.imshow(
        pivot,
        text_auto=True,
        aspect="auto",
        title="Audit Status Heatmap",
        color_continuous_scale="Blues",
    )
    st.plotly_chart(heatmap, use_container_width=True)


def _render_ingestion_drilldown(df_ingestion: pd.DataFrame) -> None:
    st.subheader("Ingestion Audit Drilldown")
    if df_ingestion.empty:
        st.info("No ingestion audit data available.")
        return

    statuses = ["All"] + sorted(df_ingestion["processing_status"].dropna().unique())
    selected_status = st.selectbox("Filter by status", options=statuses)
    filtered = df_ingestion
    if selected_status != "All":
        filtered = df_ingestion[df_ingestion["processing_status"] == selected_status]

    styled = filtered.style.apply(
        _status_style, axis=1
    ).format(na_rep="-")
    st.dataframe(
        styled,
        use_container_width=True,
        column_config={
            "object_id": "Object ID",
            "object_name": "Object Name",
            "processing_status": "Status",
            "missing_columns": "Missing Columns",
        },
    )


def _render_missing_columns(
    df_ingestion: pd.DataFrame, df_missing_cols: pd.DataFrame
) -> None:
    st.subheader("Missing Columns Report")
    if df_missing_cols.empty:
        st.info("No missing columns report data available.")
    else:
        if "ingestion_date" in df_missing_cols.columns:
            df_missing_cols = df_missing_cols.sort_values(
                "ingestion_date", ascending=False, na_position="last"
            )
        if "missing_docs_count" in df_missing_cols.columns:
            df_missing_cols = df_missing_cols[
                df_missing_cols["missing_docs_count"] > 0
            ]
        if df_missing_cols.empty:
            st.info("No missing columns report data available.")
        else:
            st.dataframe(df_missing_cols, use_container_width=True)

    st.subheader("Missing Columns Analysis")
    if df_ingestion.empty or "missing_columns" not in df_ingestion.columns:
        st.info("No missing columns data available.")
        return

    exploded = (
        df_ingestion.dropna(subset=["missing_columns"])
        .explode("missing_columns")
        .rename(columns={"missing_columns": "missing_column"})
    )
    if exploded.empty:
        st.info("No missing columns data available.")
        return

    counts = (
        exploded.groupby("missing_column")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    bar = px.bar(
        counts,
        x="missing_column",
        y="count",
        title="Missing Columns Frequency",
    )
    st.plotly_chart(bar, use_container_width=True)


def _render_missing_collections(df_ingestion: pd.DataFrame) -> None:
    st.subheader("Missing Collections")
    expected_collections = set(_load_mapping_collections())
    if not expected_collections:
        st.info("No collections found in mapping_config.yaml.")
        return

    if "source_collection" in df_ingestion.columns:
        processed_collections = set(
            df_ingestion["source_collection"].dropna().unique()
        )
    else:
        processed_collections = set()

    missing_collections = sorted(expected_collections - processed_collections)
    if not missing_collections:
        st.info("No missing collections.")
        return

    st.dataframe(
        pd.DataFrame({"collection_name": missing_collections}),
        use_container_width=True,
    )


def _load_mapping_collections() -> list:
    path = Path("config/mapping_config.yaml")
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    collections = data.get("collections", {})
    if not isinstance(collections, dict):
        return []
    return sorted(collections.keys())


def _status_style(row: pd.Series) -> list:
    status = str(row.get("processing_status", "")).lower()
    color = ""
    if status == "success":
        color = "background-color: #e6f4ea"
    elif status == "error":
        color = "background-color: #fdecea"
    elif status == "missing":
        color = "background-color: #fff4e5"
    return [color] * len(row)
