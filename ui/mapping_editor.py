from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
import yaml

from ui.mongo_ingest import connect_mongo, export_collection_to_json
from ui.run_pipeline import (
    DATE_ONLY_FORMATS,
    DATETIME_FORMATS,
    generate_runtime_configs,
    run_etl,
)


TYPE_OPTIONS = [
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
]


def render_mapping_editor(
    input_path: str,
    source_type: str,
    pg_config,
    mongo_config: Optional[dict] = None,
) -> bool:
    """Render the mapping editor and per-collection ETL triggers."""
    input_data = _load_input_data(input_path, source_type, mongo_config)
    mapping_state = st.session_state.setdefault("mapping_state", {})
    object_id_state = st.session_state.setdefault("object_id_state", {})
    mapping_config_data = _load_mapping_config()
    existing_mappings = mapping_config_data.get("collections", {})

    st.subheader("Collection Selection")
    available_collections = sorted(input_data.keys())
    selected_collections = st.multiselect(
        "Select collections to map",
        options=available_collections,
        default=st.session_state.get("selected_collections", available_collections),
    )
    st.session_state["selected_collections"] = selected_collections

    if not selected_collections:
        st.warning("Select at least one collection to continue.")
        return False

    st.subheader("Mapping Configuration")
    for collection_name in selected_collections:
        documents = input_data.get(collection_name, [])
        is_existing = collection_name in existing_mappings
        with st.expander(f"Collection: {collection_name}", expanded=True):
            if is_existing:
                st.info("Mapping already exists in `mapping_config.yaml`.")
                mapping_entry = existing_mappings.get(collection_name, {})
                mapping_rows = _rows_from_entry(mapping_entry)
                duplicates = _detect_duplicate_targets(
                    mapping_rows,
                    reserved_columns=_reserved_columns(),
                )
                if duplicates:
                    st.warning(
                        "Duplicate target columns detected: "
                        + ", ".join(sorted(set(duplicates)))
                    )
                auto_resolve = st.checkbox(
                    "Auto-resolve duplicate target columns",
                    value=True,
                    key=f"auto_resolve_existing_{collection_name}",
                )
                if st.button(
                    f"Run ETL for {collection_name}",
                    key=f"run_existing_{collection_name}",
                ):
                    if duplicates and not auto_resolve:
                        st.error(
                            "Rename duplicate target columns in the mapping file "
                            "or enable auto-resolve to continue."
                        )
                        return
                    if duplicates and auto_resolve:
                        resolved_rows, rename_map = _auto_resolve_duplicates(
                            mapping_rows,
                            reserved_columns=_reserved_columns(),
                        )
                        mapping_entry = _apply_rows_to_entry(
                            mapping_entry, resolved_rows
                        )
                        existing_mappings[collection_name] = mapping_entry
                        mapping_config_data["collections"] = existing_mappings
                        _persist_mapping_config(mapping_config_data)
                        st.warning(
                            "Duplicate target columns were auto-renamed: "
                            + ", ".join(
                                f"{old}->{new}" for old, new in rename_map.items()
                            )
                        )
                    mapping_config = _merge_mapping_config(
                        base_mappings=existing_mappings,
                        collection_names=[collection_name],
                    )
                    source_path = _ensure_input_path(
                        input_path,
                        source_type,
                        mongo_config,
                        collection_name,
                    )
                    runtime_paths = generate_runtime_configs(
                        input_path=source_path,
                        source_type=source_type,
                        pg_config=pg_config,
                        mapping_config_override=mapping_config,
                        collection_filter=[collection_name],
                    )
                    run_etl(
                        input_path=runtime_paths["input_path"],
                        app_config_path=runtime_paths["app_config"],
                        mapping_config_path=runtime_paths["mapping_config"],
                        pg_config=pg_config,
                        scope="collection",
                        target_table=mapping_entry.get("target_table"),
                    )
                continue

            if collection_name not in mapping_state:
                mapping_state[collection_name] = {
                    "rows": _default_mapping_rows(documents),
                    "target_table": f"public.{collection_name}",
                }

            collection_state = mapping_state[collection_name]
            target_table = st.text_input(
                "Target table",
                value=collection_state.get(
                    "target_table", f"public.{collection_name}"
                ),
                key=f"target_table_{collection_name}",
            )

            mapping_rows = collection_state.get("rows", _default_mapping_rows(documents))
            df = pd.DataFrame(mapping_rows)
            if df.empty:
                df = pd.DataFrame(
                    [
                        {
                            "source_attribute": "",
                            "source_type": "text",
                            "target_column": "",
                            "target_type": "text",
                        }
                    ]
                )

            edited_df = st.data_editor(
                df,
                num_rows="dynamic",
                use_container_width=True,
                key=f"mapping_editor_{collection_name}",
                column_config={
                    "source_attribute": st.column_config.TextColumn(
                        "Source Column", disabled=True
                    ),
                    "source_type": st.column_config.TextColumn(
                        "Source Type", disabled=True
                    ),
                    "target_column": st.column_config.TextColumn("Target Column"),
                    "target_type": st.column_config.SelectboxColumn(
                        "Target Type",
                        options=TYPE_OPTIONS,
                        required=True,
                    ),
                },
            )
            updated_rows = edited_df.to_dict("records")
            collection_state["rows"] = updated_rows
            collection_state["target_table"] = target_table

            available_attributes = [
                row.get("source_attribute")
                for row in updated_rows
                if row.get("source_attribute")
            ]
            if not available_attributes:
                available_attributes = ["id"]

            default_object_id = object_id_state.get(collection_name)
            if default_object_id not in available_attributes:
                default_object_id = available_attributes[0]

            object_id_state[collection_name] = st.selectbox(
                "Object ID attribute",
                options=available_attributes,
                index=available_attributes.index(default_object_id),
                key=f"object_id_{collection_name}",
            )

            auto_resolve = st.checkbox(
                "Auto-resolve duplicate target columns",
                value=True,
                key=f"auto_resolve_{collection_name}",
            )

            if st.button(
                f"Confirm mapping for {collection_name}",
                key=f"confirm_{collection_name}",
            ):
                duplicates = _detect_duplicate_targets(
                    updated_rows,
                    reserved_columns=_reserved_columns(),
                )
                if duplicates and not auto_resolve:
                    st.error(
                        "Duplicate target columns detected. Rename target columns "
                        "or enable auto-resolve to continue."
                    )
                    return
                if duplicates and auto_resolve:
                    updated_rows, rename_map = _auto_resolve_duplicates(
                        updated_rows,
                        reserved_columns=_reserved_columns(),
                    )
                    collection_state["rows"] = updated_rows
                    st.warning(
                        "Duplicate target columns were auto-renamed: "
                        + ", ".join(
                            f"{old}->{new}" for old, new in rename_map.items()
                        )
                    )

                mapping_entry = _build_mapping_entry(
                    target_table=target_table,
                    object_id_attribute=object_id_state[collection_name],
                    mapping_rows=updated_rows,
                )
                existing_mappings[collection_name] = mapping_entry
                mapping_config_data["collections"] = existing_mappings
                _persist_mapping_config(mapping_config_data)
                st.success("Mapping saved to `config/mapping_config.yaml`.")

            if collection_name in existing_mappings and st.button(
                f"Run ETL for {collection_name}",
                key=f"run_new_{collection_name}",
            ):
                mapping_config = _merge_mapping_config(
                    base_mappings=existing_mappings,
                    collection_names=[collection_name],
                )
                source_path = _ensure_input_path(
                    input_path,
                    source_type,
                    mongo_config,
                    collection_name,
                )
                runtime_paths = generate_runtime_configs(
                    input_path=source_path,
                    source_type=source_type,
                    pg_config=pg_config,
                    mapping_config_override=mapping_config,
                    collection_filter=[collection_name],
                )
                run_etl(
                    input_path=runtime_paths["input_path"],
                    app_config_path=runtime_paths["app_config"],
                    mapping_config_path=runtime_paths["mapping_config"],
                    pg_config=pg_config,
                    scope="collection",
                    target_table=target_table,
                )

    missing = [
        name for name in selected_collections if name not in existing_mappings
    ]
    can_next = not missing
    return can_next


def _default_mapping_rows(documents: List[dict]) -> List[dict]:
    attributes = sorted(_collect_attributes(documents))
    rows = []
    for attribute in attributes:
        detected = _detect_attribute_type(attribute, documents)
        rows.append(
            {
                "source_attribute": attribute,
                "source_type": detected,
                "target_column": attribute,
                "target_type": detected,
            }
        )
    return rows


def _collect_attributes(documents: List[dict]) -> List[str]:
    attributes = set()
    for document in documents:
        attributes.update(document.keys())
    return list(attributes)


def _detect_attribute_type(attribute: str, documents: List[dict]) -> str:
    observed = set()
    for document in documents:
        value = document.get(attribute)
        if value is None:
            continue
        observed.add(_detect_value_type(value))
    if not observed:
        return "text"
    if observed == {"integer"}:
        return "integer"
    if observed == {"datetime"}:
        return "datetime"
    if observed == {"date"}:
        return "date"
    if observed == {"boolean"}:
        return "boolean"
    if observed.issubset({"integer", "numeric"}):
        return "numeric"
    return "text"


def _detect_value_type(value) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "numeric"
    if isinstance(value, str):
        if _matches_formats(value, DATETIME_FORMATS):
            return "datetime"
        if _matches_formats(value, DATE_ONLY_FORMATS):
            return "date"
        return "text"
    return "text"


def _build_mapping_entry(
    target_table: str,
    object_id_attribute: str,
    mapping_rows: List[dict],
) -> Dict[str, dict]:
    mappings = {}
    for row in mapping_rows:
        source_attr = (row.get("source_attribute") or "").strip()
        target_column = (row.get("target_column") or "").strip()
        if not source_attr or not target_column:
            continue
        mappings[source_attr] = {
            "column": target_column,
            "type": row.get("target_type") or "text",
        }
    return {
        "target_table": target_table,
        "raw_json_column": "raw_json",
        "object_id_attribute": object_id_attribute,
        "mappings": mappings,
    }


def _load_json(path: str) -> dict:
    import json

    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_input_data(
    input_path: str, source_type: str, mongo_config: Optional[dict]
) -> dict:
    if source_type == "mongodb":
        return _load_mongo_preview(mongo_config)
    return _load_json(input_path)


def _load_mongo_preview(mongo_config: Optional[dict]) -> dict:
    if not mongo_config:
        return {}
    client = connect_mongo(
        mongo_config["host"],
        int(mongo_config["port"]),
        mongo_config.get("username", ""),
        mongo_config.get("password", ""),
        mongo_config.get("auth_db", ""),
    )
    database = mongo_config.get("database")
    if not database:
        return {}
    collections = client[database].list_collection_names()
    preview = {name: [] for name in collections}
    for name in collections:
        doc = client[database][name].find_one()
        preview[name] = [doc] if doc else []
    return preview


def _ensure_input_path(
    input_path: str,
    source_type: str,
    mongo_config: Optional[dict],
    collection_name: str,
) -> str:
    if source_type != "mongodb":
        return input_path
    if not mongo_config:
        raise ValueError("MongoDB configuration is missing.")
    client = connect_mongo(
        mongo_config["host"],
        int(mongo_config["port"]),
        mongo_config.get("username", ""),
        mongo_config.get("password", ""),
        mongo_config.get("auth_db", ""),
    )
    database = mongo_config.get("database")
    if not database:
        raise ValueError("MongoDB database is missing.")
    return export_collection_to_json(client, database, collection_name)


def _matches_formats(value: str, formats: List[str]) -> bool:
    from datetime import datetime

    for fmt in formats:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def _load_mapping_config() -> Dict[str, dict]:
    path = Path("config/mapping_config.yaml")
    if not path.exists():
        return {"collections": {}}
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if "collections" not in data:
        data["collections"] = {}
    return data


def _persist_mapping_config(mapping_config: Dict[str, dict]) -> None:
    path = Path("config/mapping_config.yaml")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(mapping_config, handle, sort_keys=False)


def _merge_mapping_config(
    base_mappings: Dict[str, dict],
    collection_names: List[str],
) -> Dict[str, dict]:
    collections = {}
    for name in collection_names:
        if name in base_mappings:
            collections[name] = base_mappings[name]
    return {"collections": collections}


def _rows_from_entry(mapping_entry: Dict[str, dict]) -> List[dict]:
    rows = []
    for source_attr, details in mapping_entry.get("mappings", {}).items():
        rows.append(
            {
                "source_attribute": source_attr,
                "target_column": details.get("column", ""),
                "target_type": details.get("type", "text"),
            }
        )
    return rows


def _apply_rows_to_entry(
    mapping_entry: Dict[str, dict], rows: List[dict]
) -> Dict[str, dict]:
    mappings = {}
    for row in rows:
        source_attr = (row.get("source_attribute") or "").strip()
        target_column = (row.get("target_column") or "").strip()
        if not source_attr or not target_column:
            continue
        mappings[source_attr] = {
            "column": target_column,
            "type": row.get("target_type") or "text",
        }
    mapping_entry["mappings"] = mappings
    return mapping_entry


def _reserved_columns() -> List[str]:
    return ["raw_json", "ingested_at", "source_collection", "status"]


def _detect_duplicate_targets(
    rows: List[dict],
    reserved_columns: List[str],
) -> List[str]:
    seen = {name.lower() for name in reserved_columns}
    duplicates = []
    for row in rows:
        target = (row.get("target_column") or "").strip()
        if not target:
            continue
        normalized = target.lower()
        if normalized in seen:
            duplicates.append(target)
        else:
            seen.add(normalized)
    return duplicates


def _auto_resolve_duplicates(
    rows: List[dict],
    reserved_columns: List[str],
) -> tuple[List[dict], Dict[str, str]]:
    seen = {name.lower() for name in reserved_columns}
    rename_map: Dict[str, str] = {}
    updated_rows = []
    for row in rows:
        target = (row.get("target_column") or "").strip()
        if not target:
            updated_rows.append(row)
            continue
        base = target
        normalized = base.lower()
        if normalized in seen:
            counter = 1
            new_name = f"{base}{counter}"
            while new_name.lower() in seen:
                counter += 1
                new_name = f"{base}{counter}"
            row["target_column"] = new_name
            rename_map[base] = new_name
            seen.add(new_name.lower())
        else:
            seen.add(normalized)
        updated_rows.append(row)
    return updated_rows, rename_map
