CREATE TABLE IF NOT EXISTS public.customers (
    customer_id INTEGER,
    name TEXT,
    signup_date DATE,
    email TEXT,
    raw_json JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    source_collection TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    amount NUMERIC,
    raw_json JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    source_collection TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE SCHEMA IF NOT EXISTS doc_audit;

CREATE TABLE IF NOT EXISTS doc_audit.ingestion_audit (
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    object_id TEXT,
    source_collection TEXT,
    object_name TEXT NOT NULL,
    object_status TEXT NOT NULL,
    missing_columns JSONB,
    processing_status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS doc_audit.missing_attributes_report (
    ingestion_date DATE NOT NULL,
    object_name TEXT NOT NULL,
    missing_columns JSONB
);

CREATE TABLE IF NOT EXISTS doc_audit.missing_collections_report (
    ingestion_date DATE NOT NULL,
    object_name TEXT NOT NULL,
    object_status TEXT NOT NULL
);
