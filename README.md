# Configurable MongoDB to PostgreSQL ETL Pipeline

This project implements a configuration-driven ETL pipeline that ingests JSON
documents representing multiple logical collections and loads them into
PostgreSQL in a structured, auditable, and extensible manner.

Authoritative requirements and design documents are in `Documents/`:
- `Documents/Product Requirements Document (PRD).md`
- `Documents/High-Level Design (HLD).md`
- `Documents/Basic Requirement.md`

## Setup

1. Install dependencies:
   - `pip install -r requirements.txt`
2. (Optional) Install the package for CLI usage from any directory:
   - `pip install -e .`
3. Update database credentials in `config/app_config.yaml`.
4. Create tables using the provided DDL:
   - `psql -f sql/schema.sql`

## Setup (Windows CMD)

1. Install dependencies:
   - `pip install -r requirements.txt`
2. (Optional) Install the package for CLI usage from any directory:
   - `pip install -e .`
3. Update database credentials in `config/app_config.yaml`.
4. Create tables using the provided DDL:
   - `psql -f sql/schema.sql`

## Run

Example CLI command:
- `python -m etl_pipeline.cli --input data/sample_input.json --app-config config/app_config.yaml --mapping-config config/mapping_config.yaml`

## Streamlit UI

Run the local UI:
- `streamlit run streamlit_app.py`

See `DOCUMENTATION.md` for a concise architecture and usage guide.

What the UI does:
- Source selection with in-place JSON upload or MongoDB export to `Data/`.
- PostgreSQL connection + database selection/creation.
- Collection selection, mapping editor for new collections, per-collection ETL runs, and an Overall Report view (no ETL).
- Writes runtime configs under `config/runtime/`.
- Initializes audit tables under schema `doc_audit`.
- Triggers the ETL pipeline via Python (no CLI required).
- Enforces localhost-only connections for MongoDB and PostgreSQL.
- Shows audit pivot summaries and missing collection reports after ETL runs.
- Includes a read-only audit dashboard with KPIs and charts.

## Expected Behavior

- Reads a JSON file containing multiple collections.
- Applies attribute-to-column mappings from configuration.
- Normalizes date fields to the configured standard format.
- Inserts NULL for missing attributes.
- Stores the full raw JSON document in the configured JSON column.
- Adds audit metadata per document (ingestion time, source collection, status).
- Auto-creates mapped tables that are not defined in `sql/schema.sql`.
- Records high-level (object status) and low-level (missing columns) audit reports.
- Stores audit rows in `doc_audit.ingestion_audit` and report rows in
  `doc_audit.missing_attributes_report` / `doc_audit.missing_collections_report`.
- Reports missing collections or destination tables in logs.
