from typing import Optional

import streamlit as st


def render_source_selection() -> Optional[str]:
    """Render the source selection UI and return the selected option."""
    st.header("Source Selection")
    options = ["Upload JSON File", "Connect to MongoDB"]
    default_index = 0
    if st.session_state.get("source_type") in options:
        default_index = options.index(st.session_state["source_type"])
    selected = st.radio(
        "Choose a source",
        options,
        index=default_index,
        key="source_type",
    )
    return selected
