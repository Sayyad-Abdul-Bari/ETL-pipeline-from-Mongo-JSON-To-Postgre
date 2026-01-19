# 1. Task Overview:
Create an ETL Pipeline to migrate data from Mongo to Postgres with following

# 2. Requirements:

- User can select attributes of a source collection to be mapped to destination table
columns
- Auditing information should be stored against each document
- Entire json object should also be stored in destination tables
- Should be dynamic, user should be able to add new mappings
- Apply transformations according to the column type, especially the DATE attribute
- MUST cater for multiple date formats in source documents and convert them to a single format before saving to destination Table
- Database configurations and credentials should be passed through configuration file not hard coded.
- If a mapped destination table is not defined in `schema.sql`, create it before insert
- Record object-level audit status (NEW / MISSING / ALREADY_EXISTS)
- Record missing columns per object in audit data
- Provide a Streamlit-based UI to stage data, generate configs, and trigger the ETL
- Allow local MongoDB export to JSON for staging
- Initialize `doc_audit` schema with audit/report tables
- Provide a mapping editor to add/update columns for new collections before ETL
- Allow per-collection ETL runs from the UI
- Provide an overall report view without running ETL
- Provide a read-only audit dashboard with KPIs and charts
- Allow collection selection after PostgreSQL configuration
- Support datetime mappings in addition to date

# 3. Supported Tools:

**Tools for ETL:**

| 1. Apache Airflow | 2. Python | 3. SQL |
| --- | --- | --- |

# 4. Deliverables:

- A file (containing JSON documents from multiple source collections) will be passed to pipeline as cli arguments, to simulate incoming documents from multiple mongo collections.
- The github repo for the pipeline (Python project or airflow dag)
- if the pipeline won’t create the destination table based on the mapping and will be manually created its DDL should be created
- Efficient documentation for the Assignment.
- Documentation for the assignment.

# 5. Good to have

- missing attribute/column mapping and use NULL/NONE as value for those columns
- missing collection/tables shall be reported
