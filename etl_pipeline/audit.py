from datetime import datetime, timezone


def build_business_audit_fields(audit_config, collection_name, status):
    columns = audit_config["business_columns"]
    return {
        columns["ingested_at"]: datetime.now(timezone.utc),
        columns["source_collection"]: collection_name,
        columns["status"]: status,
    }


def build_audit_row(
    audit_config,
    object_id,
    source_collection,
    object_name,
    object_status,
    missing_columns,
    processing_status,
):
    columns = audit_config["audit_columns"]
    return {
        columns["ingested_at"]: datetime.now(timezone.utc),
        columns["object_id"]: object_id,
        columns["source_collection"]: source_collection,
        columns["object_name"]: object_name,
        columns["object_status"]: object_status,
        columns["missing_columns"]: missing_columns or [],
        columns["processing_status"]: processing_status,
    }
