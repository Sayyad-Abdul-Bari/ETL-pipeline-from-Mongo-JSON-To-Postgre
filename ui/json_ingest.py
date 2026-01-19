import json
from pathlib import Path
from typing import List, Tuple

import streamlit as st


def render_json_ingest() -> Tuple[bool, str, List[str]]:
    """Render JSON upload UI, return (ready, file_path, collections)."""
    st.header("Source Configuration (JSON)")
    uploaded_file = st.file_uploader(
        "Upload JSON file",
        type=["json"],
        accept_multiple_files=False,
    )

    if uploaded_file is None:
        return False, "", []

    try:
        content = uploaded_file.getvalue().decode("utf-8")
        data = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        st.error(f"Invalid JSON file: {exc}")
        return False, "", []

    valid, collections = _validate_collections(data)
    if not valid:
        st.error(
            "JSON must be an object where each key maps to a list of documents."
        )
        return False, "", []

    data_dir = Path("Data")
    data_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(uploaded_file.name).name
    if not filename.lower().endswith(".json"):
        filename = f"{filename}.json"
    file_path = data_dir / filename
    file_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    st.success(f"Saved file to {file_path}")
    return True, str(file_path), collections


def _validate_collections(data) -> Tuple[bool, List[str]]:
    """Validate JSON structure and return collection names."""
    if not isinstance(data, dict):
        return False, []
    collections = []
    for name, documents in data.items():
        if not isinstance(documents, list):
            return False, []
        for document in documents:
            if not isinstance(document, dict):
                return False, []
        collections.append(name)
    return True, collections
