from pathlib import Path

import streamlit as st

from ui.audit_dashboard import render_audit_dashboard
from ui.json_ingest import render_json_ingest
from ui.mapping_editor import render_mapping_editor
from ui.mongo_ingest import render_mongo_ingest
from ui.postgres_setup import render_postgres_config
from ui.source_selection import render_source_selection


def main() -> None:
    """Run the Streamlit orchestration UI for the ETL pipeline."""
    _apply_header_style()
    with st.container():
        st.markdown(
            "<h1 class='app-title'>ETL pipeline from Mongo(JSON) To Postgre</h1>",
            unsafe_allow_html=True,
        )

    _init_session_state()
    nav_steps = ["Ingestion", "PostgreSQL", "Mapping", "Dashboard"]
    current_step = st.session_state.get("step", 1)
    st.sidebar.header("Navigation")
    _render_nav_progress(nav_steps, current_step, st.sidebar)

    if st.session_state["step"] == 1:
        render_source_selection()
        source_type = st.session_state.get("source_type")
        if source_type is None:
            source_type = "Upload JSON File"
            st.session_state["source_type"] = source_type
        ready = False
        if source_type == "Upload JSON File":
            ready, input_path, collections = render_json_ingest()
            normalized_source = "json"
        elif source_type == "Connect to MongoDB":
            ready, input_path, collections = render_mongo_ingest()
            normalized_source = "mongodb"
        else:
            st.warning("Please select a source type.")
            input_path, collections = "", []
            normalized_source = ""

        if ready:
            st.session_state["input_path"] = input_path
            st.session_state["collections"] = collections
            st.session_state["source_type_normalized"] = normalized_source
            if normalized_source == "mongodb":
                st.session_state["mongo_config"] = {
                    "host": st.session_state.get("mongo_host", "localhost"),
                    "port": st.session_state.get("mongo_port", "27017"),
                    "username": st.session_state.get("mongo_user", ""),
                    "password": st.session_state.get("mongo_password", ""),
                    "auth_db": st.session_state.get("mongo_auth_db", ""),
                    "database": st.session_state.get("mongo_db_name", ""),
                }
            st.success("Source data staged successfully.")

        _render_back_next(can_back=False, can_next=ready)

    elif st.session_state["step"] == 2:
        validated, pg_config = render_postgres_config()
        if pg_config is not None:
            st.session_state["pg_config"] = pg_config
        if validated:
            st.session_state["pg_validated"] = True
        _render_back_next(can_back=True, can_next=st.session_state.get("pg_validated", False))

    elif st.session_state["step"] == 3:
        st.header("Mapping & ETL")
        input_path = st.session_state.get("input_path", "")
        pg_config = st.session_state.get("pg_config")

        source_type = st.session_state.get("source_type_normalized", "")
        if source_type != "mongodb":
            if not input_path or not Path(input_path).exists():
                st.error("Staged input file is missing.")
                _render_back_next(can_back=True, can_next=False)
                return
        elif not st.session_state.get("mongo_config"):
            st.error("MongoDB configuration is missing.")
            _render_back_next(can_back=True, can_next=False)
            return

        if pg_config is None:
            st.error("PostgreSQL configuration is missing.")
            _render_back_next(can_back=True, can_next=False)
            return

        can_next = render_mapping_editor(
            input_path=input_path,
            source_type=source_type,
            pg_config=pg_config,
            mongo_config=st.session_state.get("mongo_config"),
        )
        _render_back_next(
            can_back=True,
            can_next=can_next,
            on_next=lambda: st.session_state.__setitem__("dashboard_refresh", True),
        )
    elif st.session_state["step"] == 4:
        pg_config = st.session_state.get("pg_config")
        if pg_config is None:
            st.error("PostgreSQL configuration is missing.")
            _render_back_next(can_back=True, can_next=False)
            return
        render_audit_dashboard(
            pg_config,
            force_refresh=st.session_state.pop("dashboard_refresh", False),
        )
        _render_back_next(can_back=True, can_next=False)


def _init_session_state() -> None:
    """Initialize Streamlit session state defaults."""
    defaults = {
        "step": 1,
        "source_type": "Upload JSON File",
        "source_type_normalized": "",
        "input_path": "",
        "collections": [],
        "pg_config": None,
        "pg_validated": False,
        "etl_logs": [],
        "dashboard_refresh": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _apply_header_style() -> None:
    theme_base = st.get_option("theme.base") or "light"
    primary_color = st.get_option("theme.primaryColor")
    title_color = primary_color or ("#2ecc71" if theme_base == "light" else "#5ad17a")
    css = """
        <style>
        div[data-testid="stVerticalBlock"] > div:nth-child(1) {{
            position: sticky;
            top: 0;
            z-index: 999;
            background: var(--background-color);
            padding: 0.75rem 0 0.5rem 0;
        }}
        .app-title {{
            text-align: center;
            color: {title_color};
            text-decoration: underline;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }}
        .nav-progress {{
            display: flex;
            gap: 0.5rem;
            justify-content: center;
            margin: 0.5rem 0 0.75rem 0;
            flex-wrap: wrap;
        }}
        .nav-step {{
            color: var(--text-color);
            padding: 0.25rem 0.75rem;
            border-radius: 999px;
            font-weight: 600;
            font-size: 0.85rem;
        }}
        </style>
        """.format(
        title_color=title_color
    )
    st.markdown(css, unsafe_allow_html=True)

def _render_nav_progress(nav_steps: list, current_step: int, container) -> None:
    theme_base = st.get_option("theme.base") or "light"
    palette = {
        "current": "#e74c3c" if theme_base == "light" else "#ff6b61",
        "done": "#2ecc71" if theme_base == "light" else "#5ad17a",
        "remaining": "#f1c40f" if theme_base == "light" else "#f4d35e",
    }
    container.markdown("<div class='nav-progress'>", unsafe_allow_html=True)
    for index, label in enumerate(nav_steps, start=1):
        if index == current_step:
            color = palette["current"]
        elif index < current_step:
            color = palette["done"]
        else:
            color = palette["remaining"]
        container.markdown(
            f"<span class='nav-step' style='background:{color}'>{label}</span>",
            unsafe_allow_html=True,
        )
    container.markdown("</div>", unsafe_allow_html=True)


def _render_back_next(can_back: bool, can_next: bool, on_next=None) -> None:
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Back", disabled=not can_back):
            st.session_state["step"] = max(1, st.session_state["step"] - 1)
            st.rerun()
    with col2:
        if st.button("Next", disabled=not can_next):
            if on_next:
                on_next()
            st.session_state["step"] = min(4, st.session_state["step"] + 1)
            st.rerun()


if __name__ == "__main__":
    main()
