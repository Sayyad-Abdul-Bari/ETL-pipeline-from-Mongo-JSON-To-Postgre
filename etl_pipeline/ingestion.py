import json
from pathlib import Path

from .errors import InputError


def load_input_json(input_path: str) -> dict:
    path = Path(input_path)
    if not path.exists():
        raise InputError(f"Input file not found: {input_path}")

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise InputError("Input JSON must be an object keyed by collection name.")

    for collection_name, documents in data.items():
        if not isinstance(documents, list):
            raise InputError(
                f"Collection '{collection_name}' must map to a list of documents."
            )
        for index, document in enumerate(documents):
            if not isinstance(document, dict):
                raise InputError(
                    f"Document at index {index} in collection '{collection_name}' "
                    "must be an object."
                )

    return data
