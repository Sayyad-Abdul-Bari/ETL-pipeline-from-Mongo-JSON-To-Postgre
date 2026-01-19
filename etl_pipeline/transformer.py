from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation

from .type_utils import normalize_type


def transform_document(
    document,
    mappings,
    date_formats,
    date_output_format,
    datetime_output_format,
):
    transformed = {}
    missing_columns = []
    errors = []

    for source_attr, mapping in mappings.items():
        target_column = mapping["column"]
        target_type = mapping["type"]

        if source_attr not in document:
            transformed[target_column] = None
            missing_columns.append(target_column)
            continue

        value = document.get(source_attr)
        converted, error = transform_value(
            value,
            target_type,
            date_formats,
            date_output_format,
            datetime_output_format,
        )
        if error:
            errors.append(f"{source_attr}: {error}")
            converted = None
        transformed[target_column] = converted

    return transformed, missing_columns, errors


def transform_value(
    value,
    target_type,
    date_formats,
    date_output_format,
    datetime_output_format,
):
    if value is None:
        return None, None

    normalized_type = normalize_type(target_type)

    try:
        if normalized_type in {"text", "string", "varchar"}:
            return str(value), None
        if normalized_type in {"integer", "int", "bigint", "smallint"}:
            return int(value), None
        if normalized_type in {"float", "double", "double precision"}:
            return float(value), None
        if normalized_type in {"numeric", "decimal"}:
            return Decimal(str(value)), None
        if normalized_type in {"boolean", "bool"}:
            return normalize_boolean(value), None
        if normalized_type == "date":
            parsed = parse_date(value, date_formats)
            if parsed is None:
                return None, "invalid date format"
            return parsed.strftime(date_output_format), None
        if normalized_type == "datetime":
            parsed = parse_datetime(value, date_formats)
            if parsed is None:
                return None, "invalid datetime format"
            return parsed.strftime(datetime_output_format), None
    except (ValueError, TypeError, InvalidOperation):
        return None, f"invalid value for type '{target_type}'"

    return None, f"unsupported target type '{target_type}'"


def parse_date(value, date_formats):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None

    for date_format in date_formats:
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            continue
    return None


def parse_datetime(value, date_formats):
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if not isinstance(value, str):
        return None

    for date_format in date_formats:
        try:
            return datetime.strptime(value, date_format)
        except ValueError:
            continue
    return None


def normalize_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False
    raise ValueError("invalid boolean value")
