from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class PipelineReport:
    missing_collections: set = field(default_factory=set)
    unmapped_collections: set = field(default_factory=set)
    missing_tables_db: set = field(default_factory=set)
    missing_tables_input: set = field(default_factory=set)
    processed_counts: dict = field(default_factory=lambda: defaultdict(int))
    error_counts: dict = field(default_factory=lambda: defaultdict(int))
    insert_failures: dict = field(default_factory=lambda: defaultdict(int))
    object_statuses: dict = field(default_factory=dict)
    missing_columns_by_object: dict = field(default_factory=lambda: defaultdict(set))
    missing_columns_count: int = 0
    ingestion_date: str = ""

    def record_document(self, collection_name, had_errors=False, insert_failed=False):
        if insert_failed:
            self.insert_failures[collection_name] += 1
            return
        self.processed_counts[collection_name] += 1
        if had_errors:
            self.error_counts[collection_name] += 1

    def record_object_status(self, object_name, status):
        self.object_statuses[object_name] = status

    def record_missing_columns(self, object_name, missing_columns):
        if not missing_columns:
            return
        self.missing_columns_by_object[object_name].update(missing_columns)
        self.missing_columns_count += 1

    def log_summary(self, logger, ingestion_date):
        total_processed = sum(self.processed_counts.values())
        total_failures = sum(self.insert_failures.values())
        total_errors = sum(self.error_counts.values())
        total_docs = total_processed + total_failures
        successful_docs = max(total_processed - total_errors, 0)

        summary_lines = [
            "ETL Summary",
            f"Ingestion date: {ingestion_date}",
            "",
            "KPI Summary:",
            f"  Total documents: {total_docs}",
            f"  Successful documents: {successful_docs}",
            f"  Documents with errors: {total_errors}",
            f"  Documents with missing columns: {self.missing_columns_count}",
            f"  Insert failures: {total_failures}",
            "",
            "Input coverage:",
            f"  Missing collections: {_format_list(self.missing_collections)}",
            f"  Unmapped collections: {_format_list(self.unmapped_collections)}",
            f"  Missing tables in schema.sql: {_format_list(self.missing_tables_input)}",
            f"  Missing tables in database: {_format_list(self.missing_tables_db)}",
        ]

        if self.processed_counts:
            summary_lines.append("")
            summary_lines.append("Per-collection metrics:")
            header = (
                f"  {'Collection':<20} {'Processed':>9} "
                f"{'Errors':>7} {'InsertFail':>11}"
            )
            summary_lines.append(header)
            summary_lines.append(f"  {'-' * (len(header) - 2)}")
            for collection_name in sorted(self.processed_counts.keys()):
                processed = self.processed_counts[collection_name]
                errors = self.error_counts.get(collection_name, 0)
                failures = self.insert_failures.get(collection_name, 0)
                summary_lines.append(
                    f"  {collection_name:<20} {processed:>9} {errors:>7} {failures:>11}"
                )

        if self.object_statuses:
            summary_lines.append("")
            summary_lines.append("Object statuses:")
            for object_name in sorted(self.object_statuses.keys()):
                summary_lines.append(
                    f"  - {object_name}: {self.object_statuses[object_name]}"
                )

        if self.missing_columns_by_object:
            summary_lines.append("")
            summary_lines.append("Missing columns:")
            for object_name in sorted(self.missing_columns_by_object.keys()):
                missing_columns = sorted(self.missing_columns_by_object[object_name])
                summary_lines.append(
                    f"  - {object_name}: {', '.join(missing_columns)}"
                )

        logger.info("\n".join(summary_lines))


def _format_list(items: set) -> str:
    if not items:
        return "None"
    return ", ".join(sorted(items))
