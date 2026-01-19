import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import streamlit as st
from bson import ObjectId
from pymongo import MongoClient


def render_mongo_ingest() -> Tuple[bool, str, List[str]]:
    """Render MongoDB ingestion UI, return (ready, file_path, collections)."""
    st.header("Mongo Configuration")
    host = st.text_input("Host", value=st.session_state.get("mongo_host", "localhost"))
    port = st.text_input("Port", value=st.session_state.get("mongo_port", "27017"))
    username = st.text_input("Username (optional)", value=st.session_state.get("mongo_user", ""))
    password = st.text_input(
        "Password (optional)",
        type="password",
        value=st.session_state.get("mongo_password", ""),
    )
    auth_db = st.text_input(
        "Authentication DB (optional)",
        value=st.session_state.get("mongo_auth_db", ""),
    )

    if host not in {"localhost", "127.0.0.1"}:
        st.error("MongoDB host must be localhost.")
        return False, "", []

    if not port.isdigit():
        st.error("Port must be a number.")
        return False, "", []

    if st.button("Connect to MongoDB"):
        try:
            client = connect_mongo(host, int(port), username, password, auth_db)
            db_names = client.list_database_names()
            st.session_state["mongo_client"] = client
            st.session_state["mongo_db_names"] = db_names
            st.success("Connected to MongoDB.")
        except Exception as exc:
            st.error(f"MongoDB connection failed: {exc}")
            return False, "", []

    client = st.session_state.get("mongo_client")
    db_names = st.session_state.get("mongo_db_names", [])
    if not client or not db_names:
        return False, "", []

    db_name = st.selectbox("Database", options=db_names, key="mongo_db_name")
    if st.button("Use Database"):
        st.success(f"Selected database: {db_name}")
        return True, "", []

    return False, "", []


def connect_mongo(
    host: str,
    port: int,
    username: str,
    password: str,
    auth_db: str,
) -> MongoClient:
    """Connect to MongoDB using optional credentials."""
    client_args = {"host": host, "port": port}
    if username:
        client_args["username"] = username
        client_args["password"] = password
        if auth_db:
            client_args["authSource"] = auth_db
    return MongoClient(**client_args)


def _serialize_document(document: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a MongoDB document into JSON-friendly types."""
    return {key: _serialize_value(value) for key, value in document.items()}


def _serialize_value(value: Any) -> Any:
    """Serialize individual values to JSON-friendly types."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    return value


def export_collection_to_json(
    client: MongoClient, database_name: str, collection_name: str
) -> str:
    documents = list(client[database_name][collection_name].find())
    serialized_docs = [_serialize_document(doc) for doc in documents]
    data = {collection_name: serialized_docs}

    data_dir = Path("Data")
    data_dir.mkdir(parents=True, exist_ok=True)
    file_path = data_dir / f"{collection_name}.json"
    file_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return str(file_path)
